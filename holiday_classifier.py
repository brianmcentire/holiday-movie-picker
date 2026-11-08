"""Sorts movies by how much the holiday matters to them, using Claude.

Each film gets one of four levels:
  about     the holiday is the story (Elf, a Hallmark Christmas romance)
  set_at    set at the holidays, and the season shapes it (The Apartment)
  adjacent  the holidays are backdrop (Die Hard)
  none      no real holiday connection; the builder leaves these out

Claude Opus 5.5 classifies every film and says how sure it is. Claude Haiku 5.5 gives an
independent second opinion. When they disagree, or Opus isn't sure, Opus referees at higher
effort, weighing both answers. Results are saved in holiday_levels.json, so each film is
classified once; the films that needed a referee, or that the model didn't know, are listed in
holiday_levels_review.md for a human to skim. To overrule a call, add it to OVERRIDES.
"""
import asyncio
import json
import os
from collections import Counter
from pathlib import Path
from typing import Literal

import anthropic
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent
LEVELS_FILE = BASE_DIR / "holiday_levels.json"
REVIEW_FILE = BASE_DIR / "holiday_levels_review.md"

PRIMARY_MODEL = "claude-opus-5-5"
SECOND_MODEL = "claude-haiku-5-5"
PROMPT_VERSION = 2  # Bump after changing the instructions below to classify every film again
CONCURRENT_FILMS = 12
PRICES = {PRIMARY_MODEL: (4.00, 20.00), SECOND_MODEL: (0.10, 0.50)}  # $ per million tokens, in / out

LEVELS = ("about", "set_at", "adjacent", "none")
Level = Literal["about", "set_at", "adjacent", "none"]

# Your calls, which beat the classifier's: TMDB id -> level
OVERRIDES = {
    "81188": "about",  # Rise of the Guardians (2012)
    "9714": "about",   # Home Alone 3 (1997)
    "909": "adjacent", # Meet Me in St. Louis (1944)
}

INSTRUCTIONS = """You sort movies for a holiday movie picker covering Christmas, Hanukkah, Kwanzaa, Thanksgiving, New Year's and Lunar New Year. For each movie, decide how much the holiday matters to it:

- "about": the holiday is the subject. The story is about the holiday, its traditions or its spirit. Examples: The Polar Express, The Santa Clause, a Hallmark Christmas romance, an animated Hanukkah special. This also covers films people think of as holiday movies through and through, where the holiday runs through the whole story and its emotional payoff even though the plot is about something else, like It's a Wonderful Life or Home Alone.
- "set_at": the story is about something else, but the season shapes it: its mood, themes or key scenes depend on the holidays. These films use the holidays without being holiday movies at heart. Examples: The Apartment, whose office party, Christmas Eve and New Year's Eve carry its loneliness; Planes, Trains and Automobiles, where getting home for Thanksgiving drives the plot.
- "adjacent": the story happens at the holidays, but the holiday is backdrop; you could move it to another time of year and lose little. Example: an action thriller that happens to take place at an office Christmas party.
- "none": no meaningful holiday connection, or only a passing scene.

Use what you know about the film itself, not just the description provided. Judge the movie as a whole, not one scene."""

REFEREE_INSTRUCTIONS = INSTRUCTIONS + """

Two reviewers disagreed about this movie, or weren't sure. Make the case for each plausible level in turn, using what you know about the film, then give the final call."""


class Verdict(BaseModel):
    level: Level
    confidence: Literal["high", "medium", "low"]
    knows_film: bool  # Whether you know this film itself, rather than going by the description
    reason: str  # One short sentence


class SecondOpinion(BaseModel):
    level: Level
    reason: str  # One short sentence


class Ruling(BaseModel):
    arguments: str  # The case for each plausible level
    level: Level
    reason: str  # One sentence explaining the final call


class Usage:
    """Running token totals per model, for a cost estimate."""
    def __init__(self):
        self.tokens = {model: [0, 0] for model in PRICES}

    def add(self, model, usage):
        self.tokens[model][0] += usage.input_tokens
        self.tokens[model][1] += usage.output_tokens

    def cost(self):
        return sum(i / 1e6 * PRICES[m][0] + o / 1e6 * PRICES[m][1] for m, (i, o) in self.tokens.items())


def load_api_key():
    """Read ANTHROPIC_API_KEY from the environment or the local .env file. Returns None if missing."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    env_file = BASE_DIR / ".env"
    if not key and env_file.exists():
        for line in env_file.read_text().splitlines():
            name, _, value = line.partition("=")
            if name.strip() == "ANTHROPIC_API_KEY":
                key = value.strip().strip("\"'")
    return key or None


def film_prompt(film):
    return (f"Title: {film['title']} ({film['year']})\n"
            f"Genres: {', '.join(film['genres']) or 'unknown'}\n"
            f"TMDB keywords: {', '.join(film['keywords']) or 'none'}\n"
            f"Description: {film['overview'] or 'none'}")


async def ask_opus(client, usage, system, content, output_format, effort):
    resp = await client.beta.messages.parse(
        model=PRIMARY_MODEL,
        max_tokens=4000,
        system=system,
        output_config={"effort": effort},
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",  # If Opus declines, the API retries on Anthropic's recommended fallback model
        messages=[{"role": "user", "content": content}],
        output_format=output_format,
    )
    usage.add(PRIMARY_MODEL, resp.usage)
    if resp.stop_reason == "refusal" or resp.parsed_output is None:
        raise RuntimeError(f"no answer (stop reason: {resp.stop_reason})")
    return resp.parsed_output


async def ask_haiku(client, usage, content):
    resp = await client.messages.parse(
        model=SECOND_MODEL,
        max_tokens=4000,
        system=INSTRUCTIONS,
        output_config={"effort": "low"},
        messages=[{"role": "user", "content": content}],
        output_format=SecondOpinion,
    )
    usage.add(SECOND_MODEL, resp.usage)
    if resp.stop_reason == "refusal" or resp.parsed_output is None:
        raise RuntimeError(f"no answer (stop reason: {resp.stop_reason})")
    return resp.parsed_output


async def classify_film(client, semaphore, usage, film):
    content = film_prompt(film)
    async with semaphore:
        primary, second = await asyncio.gather(
            ask_opus(client, usage, INSTRUCTIONS, content + "\n\nGive the level, how sure you are, whether you know the film, and a one-sentence reason.", Verdict, "low"),
            ask_haiku(client, usage, content + "\n\nGive the level and a one-sentence reason."),
        )
        entry = {
            "title": film["title"], "year": film["year"], "level": primary.level,
            "opus": primary.level, "haiku": second.level, "confidence": primary.confidence,
            "knows_film": primary.knows_film, "reason": primary.reason, "referee": None,
            "version": PROMPT_VERSION,
        }
        if primary.level != second.level or primary.confidence != "high":
            ruling = await ask_opus(client, usage, REFEREE_INSTRUCTIONS, content + (
                f"\n\nReviewer A said \"{primary.level}\": {primary.reason}"
                f"\nReviewer B said \"{second.level}\": {second.reason}"
                "\n\nMake the case for each plausible level, then give the final level and a one-sentence reason."), Ruling, "high")
            entry["level"] = ruling.level
            entry["referee"] = ruling.reason
    return entry


def load_levels():
    return json.loads(LEVELS_FILE.read_text()) if LEVELS_FILE.exists() else {}


async def classify_all(films, log):
    """Classify films not yet classified. films: TMDB id -> {title, year, genres, keywords, overview}.
    Returns (levels for every film that has one, number of films still without one)."""
    saved = load_levels()
    todo = [mid for mid in films
            if mid not in OVERRIDES and saved.get(mid, {}).get("version") != PROMPT_VERSION]

    if todo:
        key = load_api_key()
        if not key:
            log(f"Holiday levels: ANTHROPIC_API_KEY not found, so {len(todo)} films stay unsorted.")
        else:
            log(f"Holiday levels: classifying {len(todo)} films...")
            client = anthropic.AsyncAnthropic(api_key=key, max_retries=5)
            semaphore = asyncio.Semaphore(CONCURRENT_FILMS)
            usage = Usage()
            results = await asyncio.gather(*[classify_film(client, semaphore, usage, films[mid]) for mid in todo],
                                           return_exceptions=True)
            failures = Counter()
            for mid, result in zip(todo, results):
                if isinstance(result, BaseException):
                    failures[f"{type(result).__name__}: {str(result)[:200]}"] += 1
                else:
                    saved[mid] = result
            LEVELS_FILE.write_text(json.dumps(saved, indent=1, sort_keys=True, ensure_ascii=False) + "\n")
            log(f"Holiday levels: {len(todo) - sum(failures.values())} classified, about ${usage.cost():.2f}.")
            for message, count in failures.items():
                log(f"  {count} failed: {message}")

    levels = {}
    for mid in films:
        if mid in OVERRIDES:
            levels[mid] = OVERRIDES[mid]
        elif saved.get(mid, {}).get("version") == PROMPT_VERSION:
            levels[mid] = saved[mid]["level"]
    write_review(films, saved)
    return levels, len(films) - len(levels)


def write_review(films, saved):
    """List the calls worth a human look: films the two models disagreed on, and films left out."""
    def worth_a_look(e):
        return e["opus"] != e["haiku"] or e["level"] != e["opus"] or e["level"] == "none"

    rows = sorted(((mid, e) for mid, e in saved.items() if mid in films and mid not in OVERRIDES and worth_a_look(e)),
                  key=lambda row: (LEVELS.index(row[1]["level"]), row[1]["title"]))
    lines = [
        "# Holiday levels to review",
        "",
        "Calls worth a second look: Opus and Haiku disagreed, so a referee made the final call, or the film is left",
        "out of the site as having no real holiday connection. To overrule one, add its TMDB id to `OVERRIDES` in",
        "`holiday_classifier.py`.",
        "",
        "Levels: **about** the holiday is the story · **set_at** set at the holidays, and it matters ·",
        "**adjacent** the holidays are backdrop · **none** left out",
        "",
        "| Film | Final | Opus | Haiku | Why |",
        "|---|---|---|---|---|",
    ]
    for mid, e in rows:
        why = (e["referee"] or e["reason"]).replace("|", "/").replace("\n", " ")
        note = "" if e["knows_film"] else " *(didn't know the film)*"
        lines.append(f"| {e['title']} ({e['year']}) `{mid}` | **{e['level']}** | {e['opus']} | {e['haiku']} | {why}{note} |")
    lines.append("")
    lines.append(f"{len(rows)} films to review.")
    REVIEW_FILE.write_text("\n".join(lines) + "\n")
