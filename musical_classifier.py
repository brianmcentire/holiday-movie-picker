"""Labels the musicals and music shows for the Musical pill, using Claude.

Each film gets one kind:
  musical     several full songs performed on screen that carry the story or are the point of it
  music_show  a concert, a variety special built on songs, a filmed ballet or opera, or a documentary about musicians
  some_songs  a song or two, in a story told mostly in dialogue
  none        no singing on screen beyond a moment

The pill holds musicals and music shows. Claude Opus 5.5 and Claude Sonnet 5.5 each answer every film, with their
confidence and whether they know the film. Claude Fable 5.1 decides the films where they disagree about the pill, or
where either is unsure or doesn't know the film. Answers are saved in musicals.json, so each film is asked once;
Fable's calls are listed in musicals_review.md for a human to skim. To overrule a call, add it to OVERRIDES.

  python musical_classifier.py ask [--batch]      Opus and Sonnet answer the films that have no answer yet, then
                                                  it reports how many films Fable would decide and the cost
  python musical_classifier.py decide [--skip-quiet]
                                                  Fable decides those films, then the pill in holiday_engine.json
                                                  is updated. --skip-quiet leaves out films both models call "none"
                                                  that TMDB gives no musical keyword or Music genre
  python musical_classifier.py apply              Updates the pill in holiday_engine.json from musicals.json

--batch uses the Batch API: half the price, but it can take up to 24 hours (usually under one). An interrupted batch
run picks up its batches again when started the same way. The builder reads musicals.json for the pill; it doesn't
ask Claude itself.
"""
import argparse
import asyncio
import json
import os
import random
import sys
import time
from pathlib import Path

import aiohttp
import anthropic

BASE_DIR = Path(__file__).resolve().parent
ANSWERS_FILE = BASE_DIR / "musicals.json"
REVIEW_FILE = BASE_DIR / "musicals_review.md"
DATA_FILE = BASE_DIR / "holiday_engine.json"
BATCHES_FILE = BASE_DIR / ".musical-batches.json"  # Batches still running, so an interrupted run can pick them up

READERS = {"opus": "claude-opus-5-5", "sonnet": "claude-sonnet-5-5"}
DECIDER = "claude-fable-5-1"
PROMPT_VERSION = 1  # Bump after changing the instructions below to ask about every film again
PRICES = {"claude-opus-5-5": (4.00, 20.00), "claude-sonnet-5-5": (2.00, 10.00), DECIDER: (10.00, 50.00)}  # $/M in, out
CONCURRENT = 8
GENRE = "Musical"  # The pill's name; it replaces TMDB's "Music" genre
KINDS = ["musical", "music_show", "some_songs", "none"]
ENGLISH_MARKETS = {"US", "GB", "CA", "AU", "IE", "NZ"}  # Where a film's other titles are shown to the models
PILL_KINDS = {"musical", "music_show"}

# Your calls, which beat Claude's: TMDB id -> kind
OVERRIDES = {}

INSTRUCTIONS = """You label movies and TV specials for a holiday movie picker's "Musical" filter. People use it to find, or to skip, the titles where songs are the main event.

Pick one kind:
- "musical": several full songs or song-and-dance numbers are performed on screen, and they carry the story or are the point of it. Characters sing what they feel, or the story is built around the performances. Animated specials count when they're built around their songs. A story about a singer or band counts only when it's full of songs, not when the lead performs once or twice.
- "music_show": a concert, a variety special built on songs, a filmed ballet or opera, or a documentary about musicians or a song.
- "some_songs": a song or two, a carol scene, a pageant number, a karaoke scene or a single performance, but the story is told mostly in dialogue.
- "none": no singing on screen beyond a moment, or songs only on the soundtrack.

Use what you know about the title itself, not just the description provided, and judge it as a whole."""

ASK = ("Give the kind, the rough number of full songs performed on screen, how sure you are, whether you know the "
       "title, and a one-sentence reason.")

DECIDER_INSTRUCTIONS = INSTRUCTIONS + """

Two reviewers have answered for this title. They disagreed about whether it belongs in the filter, or at least one of them wasn't sure or didn't know the title. Weigh what you know about the title against their answers, then give the final kind."""

VERDICT_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["kind", "songs_on_screen", "confidence", "knows_film", "reason"],
    "properties": {
        "kind": {"type": "string", "enum": KINDS},
        "songs_on_screen": {"type": "integer"},
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "knows_film": {"type": "boolean"},
        "reason": {"type": "string"},
    },
}
RULING_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["kind", "knows_film", "reason"],
    "properties": {
        "kind": {"type": "string", "enum": KINDS},
        "knows_film": {"type": "boolean"},
        "reason": {"type": "string"},
    },
}


def plural(n, word):
    return f"{n} {word}" + ("" if n == 1 else "s")


def read_key(name):
    """A key from the environment or the local .env file, or None."""
    key = os.environ.get(name)
    env_file = BASE_DIR / ".env"
    if not key and env_file.exists():
        for line in env_file.read_text().splitlines():
            k, _, value = line.partition("=")
            if k.strip() == name:
                key = value.strip().strip("\"'")
    return key or None


def load_answers():
    return json.loads(ANSWERS_FILE.read_text()) if ANSWERS_FILE.exists() else {}


def save_answers(answers):
    ANSWERS_FILE.write_text(json.dumps(answers, indent=1, sort_keys=True, ensure_ascii=False) + "\n")


# --- What decides a film ---

def in_pill_kind(kind):
    return kind in PILL_KINDS


def answered(e, who):
    return bool(e) and e.get(who, {}).get("version") == PROMPT_VERSION


def complete(e):
    return all(answered(e, who) for who in READERS)


def needs_decider(e):
    """Why Fable should decide this film, or None: the two disagree about the pill, or either is unsure or doesn't
    know the film. Disagreeing only on musical vs. music show isn't a disagreement."""
    if not complete(e): return None
    a, b = e["opus"], e["sonnet"]
    if in_pill_kind(a["kind"]) != in_pill_kind(b["kind"]): return "disagree"
    if "low" in (a["confidence"], b["confidence"]): return "unsure"
    if not (a["knows_film"] and b["knows_film"]): return "unknown"
    return None


def quiet(e):
    """Both say "none" and TMDB gives no musical keyword or Music genre: not worth Fable's time."""
    return e["opus"]["kind"] == "none" and e["sonnet"]["kind"] == "none" and not e.get("music_hint")


def final_kind(mid, e):
    """The film's kind: your override, Fable's call, or Opus's. None if it has no answer yet."""
    if mid in OVERRIDES: return OVERRIDES[mid]
    if answered(e, "fable"): return e["fable"]["kind"]
    return e["opus"]["kind"] if complete(e) else None


def pill_films(movie_ids):
    """For the builder: (the films in the pill, how many films have no answer yet)."""
    answers = load_answers()
    kinds = {mid: final_kind(mid, answers.get(mid)) for mid in movie_ids}
    return [mid for mid in movie_ids if in_pill_kind(kinds[mid])], sum(k is None for k in kinds.values())


# --- What Claude sees about a film ---

async def fetch_films(ids, log):
    """Title, year, genres, keywords and description from TMDB, the same details the builder uses."""
    token = read_key("TMDB_API_KEY")
    if not token: sys.exit("TMDB_API_KEY not found in the environment or .env.")
    semaphore, films = asyncio.Semaphore(CONCURRENT), {}

    async def one(session, mid):
        url = f"https://api.themoviedb.org/3/movie/{mid}"
        for attempt in range(4):
            async with semaphore:
                try:
                    async with session.get(url, params={"append_to_response": "keywords,alternative_titles"}) as r:
                        if r.status == 200:
                            d = await r.json()
                            films[mid] = {
                                "title": d["title"], "year": (d.get("release_date") or "")[:4] or "unknown",
                                "aka": other_titles(d),
                                "genres": [g["name"] for g in d.get("genres", [])],
                                "keywords": [" ".join(k["name"].split()) for k in d.get("keywords", {}).get("keywords", [])],
                                "overview": d.get("overview") or "",
                            }
                            return
                        if r.status == 404: return
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    pass
            await asyncio.sleep(2 ** attempt + random.random())

    async with aiohttp.ClientSession(headers={"Authorization": f"Bearer {token}"},
                                     timeout=aiohttp.ClientTimeout(total=20)) as session:
        await asyncio.gather(*(one(session, mid) for mid in ids))
    if len(films) < len(ids):
        log(f"  {len(ids) - len(films)} films couldn't be read from TMDB; they wait for the next run.")
    return films


def other_titles(tmdb_movie):
    """The film's other English-market titles on TMDB. TV movies are often renamed for streaming, and a model may
    know one under its other name (Christmas Crush is also Holiday High School Reunion)."""
    seen = {tmdb_movie["title"].lower(), (tmdb_movie.get("original_title") or "").lower()}
    titles = []
    for t in tmdb_movie.get("alternative_titles", {}).get("titles", []):
        title = " ".join((t.get("title") or "").split())
        if t.get("iso_3166_1") in ENGLISH_MARKETS and title and title.lower() not in seen:
            seen.add(title.lower())
            titles.append(title)
    return titles[:5]


def film_prompt(film):
    aka = f"Also known as: {' / '.join(film['aka'])}\n" if film.get("aka") else ""
    return (f"Title: {film['title']} ({film['year']})\n{aka}Genres: {', '.join(film['genres']) or 'unknown'}\n"
            f"TMDB keywords: {', '.join(film['keywords']) or 'none'}\nDescription: {film['overview'] or 'none'}")


def music_hint(film):
    return "Music" in film["genres"] or any("musical" in k.lower() for k in film["keywords"])


def reader_params(model, film):
    return {"model": model, "max_tokens": 4000, "system": INSTRUCTIONS,
            "output_config": {"effort": "low", "format": {"type": "json_schema", "schema": VERDICT_SCHEMA}},
            "messages": [{"role": "user", "content": film_prompt(film) + "\n\n" + ASK}]}


def read_json_reply(msg):
    if msg.stop_reason == "refusal": raise RuntimeError("declined")
    text = next((b.text for b in msg.content if b.type == "text"), None)
    if text is None: raise RuntimeError(f"no answer (stop reason: {msg.stop_reason})")
    return json.loads(text)


class Cost:
    def __init__(self):
        self.dollars = 0.0

    def add(self, model, usage, batch=False):
        price_in, price_out = PRICES[model]
        self.dollars += (usage.input_tokens * price_in + usage.output_tokens * price_out) / 1e6 * (0.5 if batch else 1)


def record(answers, mid, film, who, verdict):
    e = answers.setdefault(mid, {})
    e.update(title=film["title"], year=film["year"], music_hint=music_hint(film))
    e[who] = {"kind": verdict["kind"], "songs": verdict["songs_on_screen"], "confidence": verdict["confidence"],
              "knows_film": verdict["knows_film"], "reason": verdict["reason"], "version": PROMPT_VERSION}


# --- Asking Opus and Sonnet ---

def catalog_ids():
    return list(json.loads(DATA_FILE.read_text())["movies"])


async def ask_now(todo, films, answers, cost, log):
    """Normal requests, for a handful of films."""
    client = anthropic.AsyncAnthropic(api_key=read_key("ANTHROPIC_API_KEY"), max_retries=5)
    semaphore = asyncio.Semaphore(CONCURRENT)

    async def one(mid, who):
        async with semaphore:
            model = READERS[who]
            msg = await client.beta.messages.create(**reader_params(model, films[mid]),
                                                    betas=["server-side-fallback-2026-07-01"], fallbacks="default")
            cost.add(model, msg.usage)
            return read_json_reply(msg)

    jobs = [(mid, who) for mid, whos in todo.items() for who in whos]
    results = await asyncio.gather(*(one(mid, who) for mid, who in jobs), return_exceptions=True)
    failed = 0
    for (mid, who), result in zip(jobs, results):
        if isinstance(result, BaseException):
            failed += 1
            log(f"  {who} couldn't answer for {films[mid]['title']}: {type(result).__name__}: {str(result)[:150]}")
        else:
            record(answers, mid, films[mid], who, result)
    return failed


def ask_in_batches(todo, films, answers, cost, log):
    """The Batch API: one batch per model. Waits for both, checking every minute."""
    client = anthropic.Anthropic(api_key=read_key("ANTHROPIC_API_KEY"), max_retries=5)
    state = json.loads(BATCHES_FILE.read_text()) if BATCHES_FILE.exists() else {}
    if state:
        log(f"Picking up the batches already running: {', '.join(state.values())}")
    else:
        for who, model in READERS.items():
            requests = [{"custom_id": f"{who}-{mid}", "params": reader_params(model, films[mid])}
                        for mid, whos in todo.items() if who in whos]
            if requests:
                state[who] = client.messages.batches.create(requests=requests).id
                log(f"Sent {len(requests)} films to {who} as batch {state[who]}")
        BATCHES_FILE.write_text(json.dumps(state, indent=1) + "\n")

    started = time.monotonic()
    while True:
        batches = {who: client.messages.batches.retrieve(bid) for who, bid in state.items()}
        def progress(c):
            finished = c.succeeded + c.errored + c.canceled + c.expired
            return f"{finished} of {finished + c.processing}"
        counts = ", ".join(f"{who} {progress(b.request_counts)}" for who, b in batches.items())
        log(f"  {time.strftime('%H:%M')} after {int((time.monotonic() - started) / 60)} min: {counts} done")
        if all(b.processing_status == "ended" for b in batches.values()): break
        time.sleep(60)

    failed = 0
    for who, bid in state.items():
        model = READERS[who]
        for result in client.messages.batches.results(bid):
            mid = result.custom_id.split("-", 1)[1]
            try:
                if result.result.type != "succeeded": raise RuntimeError(result.result.type)
                cost.add(model, result.result.message.usage, batch=True)
                if mid in films: record(answers, mid, films[mid], who, read_json_reply(result.result.message))
            except Exception as err:  # A film that failed is asked again on the next run
                failed += 1
                log(f"  {who} couldn't answer for {mid}: {type(err).__name__}: {str(err)[:150]}")
    BATCHES_FILE.unlink()
    return failed


def ask(batch, log):
    answers = load_answers()
    if BATCHES_FILE.exists() and not batch:
        sys.exit(f"Batches are still running ({BATCHES_FILE.name}); run `ask --batch` to pick them up.")
    todo = {mid: [who for who in READERS if not answered(answers.get(mid), who)]
            for mid in catalog_ids() if mid not in OVERRIDES}
    todo = {mid: whos for mid, whos in todo.items() if whos}
    if not todo and not BATCHES_FILE.exists():
        log("Every film already has both answers.")
    else:
        if not read_key("ANTHROPIC_API_KEY"): sys.exit("ANTHROPIC_API_KEY not found in the environment or .env.")
        log(f"Reading {len(todo)} films from TMDB...")
        films = asyncio.run(fetch_films(list(todo), log))
        todo = {mid: whos for mid, whos in todo.items() if mid in films}
        cost = Cost()
        failed = ask_in_batches(todo, films, answers, cost, log) if batch else asyncio.run(ask_now(todo, films, answers, cost, log))
        save_answers(answers)
        log(f"Saved the answers, about ${cost.dollars:.2f}." + (f" {failed} answers failed; run again to retry them." if failed else ""))
    report(answers, log)


def report(answers, log):
    ids = catalog_ids()
    entries = {mid: answers.get(mid) for mid in ids}
    done = {mid: e for mid, e in entries.items() if complete(e)}
    flagged = {mid: needs_decider(e) for mid, e in done.items() if needs_decider(e) and not answered(e, "fable")}
    skippable = [mid for mid in flagged if quiet(done[mid])]
    agreed_in = sum(1 for mid, e in done.items() if not needs_decider(e) and in_pill_kind(e["opus"]["kind"]))
    log(f"\n{len(done)} of {len(ids)} films have both answers. {agreed_in} are in the pill by agreement.")
    if not flagged:
        log("No films are waiting for Fable.")
        return
    why = {r: sum(1 for v in flagged.values() if v == r) for r in ("disagree", "unsure", "unknown")}
    low, high = cost_range(len(flagged)), cost_range(len(flagged) - len(skippable))
    log(f"{len(flagged)} films are waiting for Fable: {why['disagree']} disagree about the pill, {why['unsure']} "
        f"have a low-confidence answer, {why['unknown']} weren't known to a model.")
    log(f"  Fable for all of them: about ${low[0]:.0f}–{low[1]:.0f}.")
    log(f"  With --skip-quiet ({len(skippable)} films both call \"none\" with no musical hint from TMDB): "
        f"{len(flagged) - len(skippable)} films, about ${high[0]:.0f}–{high[1]:.0f}.")
    log("Run `python musical_classifier.py decide` (or with --skip-quiet) to have Fable decide them.")


def cost_range(n):
    """Fable's cost for n films: about 1,500 tokens in, and 600 to 2,000 out depending on how much it thinks."""
    price_in, price_out = PRICES[DECIDER]
    return tuple(n * (1500 * price_in + out * price_out) / 1e6 for out in (600, 2000))


# --- Fable decides ---

def answer_line(name, a):
    unknown = ", didn't know the title" if not a["knows_film"] else ""
    return f'Reviewer {name} said "{a["kind"]}" ({a["songs"]} songs, {a["confidence"]} confidence{unknown}): {a["reason"]}'


async def decide_now(todo, films, answers, cost, log):
    client = anthropic.AsyncAnthropic(api_key=read_key("ANTHROPIC_API_KEY"), max_retries=5)
    semaphore = asyncio.Semaphore(CONCURRENT)

    async def one(mid):
        e = answers[mid]
        content = (film_prompt(films[mid]) + "\n\n" + answer_line("A", e["opus"]) + "\n" + answer_line("B", e["sonnet"])
                   + "\n\nGive the final kind, whether you know the title, and a one-sentence reason.")
        async with semaphore:
            msg = await client.beta.messages.create(
                model=DECIDER, max_tokens=16000, system=DECIDER_INSTRUCTIONS,
                output_config={"effort": "high", "format": {"type": "json_schema", "schema": RULING_SCHEMA}},
                betas=["server-side-fallback-2026-07-01"], fallbacks="default",
                messages=[{"role": "user", "content": content}])
            cost.add(DECIDER, msg.usage)
            return read_json_reply(msg)

    results = await asyncio.gather(*(one(mid) for mid in todo), return_exceptions=True)
    failed = 0
    for mid, result in zip(todo, results):
        if isinstance(result, BaseException):
            failed += 1
            log(f"  Fable couldn't decide {films[mid]['title']}: {type(result).__name__}: {str(result)[:150]}")
        else:
            answers[mid]["fable"] = {**result, "why_asked": needs_decider(answers[mid]), "version": PROMPT_VERSION}
    return failed


def decide(skip_quiet, log):
    answers = load_answers()
    ids = set(catalog_ids())
    todo = [mid for mid, e in answers.items() if mid in ids and mid not in OVERRIDES
            and needs_decider(e) and not answered(e, "fable") and not (skip_quiet and quiet(e))]
    if todo:
        if not read_key("ANTHROPIC_API_KEY"): sys.exit("ANTHROPIC_API_KEY not found in the environment or .env.")
        log(f"Fable is deciding {len(todo)} films...")
        films = asyncio.run(fetch_films(todo, log))
        todo = [mid for mid in todo if mid in films]
        cost = Cost()
        failed = asyncio.run(decide_now(todo, films, answers, cost, log))
        save_answers(answers)
        log(f"Fable decided {len(todo) - failed}, about ${cost.dollars:.2f}." + (f" {failed} failed; run again to retry." if failed else ""))
    else:
        log("No films are waiting for Fable.")
    write_review(answers, ids)
    apply(log)


def write_review(answers, ids):
    """Fable's calls that differ from Opus or Sonnet about the pill, for a human to skim."""
    decided = [(mid, e) for mid, e in answers.items() if mid in ids and answered(e, "fable")]
    differs = lambda e: len({in_pill_kind(e[who]["kind"]) for who in ("fable", "opus", "sonnet")}) > 1
    rows = sorted(((mid, e) for mid, e in decided if differs(e)),
                  key=lambda row: (KINDS.index(row[1]["fable"]["kind"]), row[1]["title"]))
    lines = [
        "# Musical pill: calls to review",
        "",
        "Opus and Sonnet each said whether a film is a musical. Fable made the final call when the two disagreed about",
        "the pill, or one of them wasn't sure or didn't know the film. Listed here are the calls where Fable, Opus and",
        "Sonnet didn't all agree about the pill. To overrule one, add its TMDB id to `OVERRIDES` in",
        "`musical_classifier.py`, then run `python musical_classifier.py apply`.",
        "",
        "Kinds: **musical** and **music_show** are in the pill · **some_songs** and **none** aren't",
        "",
        "| Film | Final | Opus | Sonnet | Why asked | Fable's reason |",
        "|---|---|---|---|---|---|",
    ]
    for mid, e in rows:
        f = e["fable"]
        nobody = " *(no model knew the film)*" if not (f["knows_film"] or e["opus"]["knows_film"] or e["sonnet"]["knows_film"]) else ""
        reason = f["reason"].replace("|", "/").replace("\n", " ")
        lines.append(f"| {e['title']} ({e['year']}) `{mid}` | **{f['kind']}** | {e['opus']['kind']} | {e['sonnet']['kind']}"
                     f" | {f['why_asked']} | {reason}{nobody} |")
    lines += ["", f"{plural(len(rows), 'call')} to review. Fable decided {plural(len(decided), 'film')} in all; "
                  "on the rest it agreed with both models."]
    REVIEW_FILE.write_text("\n".join(lines) + "\n")


def apply(log):
    """Puts the pill into holiday_engine.json the way the builder does, without a full rebuild."""
    data = json.loads(DATA_FILE.read_text())
    films, unanswered = pill_films(list(data["movies"]))
    data["genres"].pop("Music", None)  # TMDB's genre, which the pill replaces
    data["index"].pop("musical", None)  # TMDB's patchy keyword, so a search for "musical" finds the pill's films
    if films: data["genres"][GENRE] = films
    else: data["genres"].pop(GENRE, None)
    DATA_FILE.write_text(json.dumps(data, indent=2))
    log(f"The Musical pill holds {plural(len(films), 'film')}." + (f" {plural(unanswered, 'film')} with no answer yet." if unanswered else ""))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("ask").add_argument("--batch", action="store_true", help="use the Batch API (half price, slower)")
    sub.add_parser("decide").add_argument("--skip-quiet", action="store_true",
                                          help='leave out films both models call "none" with no musical hint from TMDB')
    sub.add_parser("apply")
    args = parser.parse_args()
    log = lambda msg: print(msg, flush=True)
    if args.command == "ask": ask(args.batch, log)
    elif args.command == "decide": decide(args.skip_quiet, log)
    else: apply(log)
