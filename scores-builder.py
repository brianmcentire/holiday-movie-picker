#!/usr/bin/env python3
"""Fetch IMDb, Tomatometer and Popcornmeter scores for every film in holiday_engine.json from MDBList, into scores.json.

scores.json is for the owner's Tampermonkey script (userscripts/scores.user.js). The site itself never loads it.

    {"v": 1, "source": "MDBList", "updated": "2026-11-11", "movies": {
    "10719": {"imdb": 7.1, "tomatoes": 85, "popcorn": 79, "imdb_id": "tt0319343", "rt": "m/elf", "at": "2026-11-11"},
    ...}}

A missing field means unknown; there are never nulls, zeros or empty strings. `rt` is the path after
https://www.rottentomatoes.com/. `at` is when the film's scores were last fetched; `updated` is the last run that
fetched any.

Old values are kept rather than lost: a field MDBList leaves out this week keeps last week's value, and a film this
run couldn't fetch keeps its whole entry. Films no longer in holiday_engine.json are dropped.

Polite to MDBList (a free hobby service): one request every 1.5 seconds, 50 films a request (about 23 requests for
the whole catalog), at most 300 requests a run, and it stops early rather than eat the last 100 of the day's 1,000
free requests, which the owner's browser script also uses. No key, a refused key or MDBList trouble leave
scores.json as it was and exit 0, so the weekly data build still publishes.

The key is MDBLIST_API_KEY, from the environment or .env. It's never printed.
"""
import datetime
import json
import math
import os
import re
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
# The paths, hosts and waits can be changed for testing
CATALOG = Path(os.environ.get("SCORES_CATALOG", BASE_DIR / "holiday_engine.json"))
OUT = Path(os.environ.get("SCORES_OUT", BASE_DIR / "scores.json"))
API_ROOT = os.environ.get("MDBLIST_API_ROOT", "https://api.mdblist.com")
WIKIDATA_URL = os.environ.get("WIKIDATA_SPARQL_URL", "https://query.wikidata.org/sparql")
WAIT_SCALE = float(os.environ.get("SCORES_WAIT_SCALE", "1"))  # tests shrink every wait

USER_AGENT = "holiday-movie-picker (https://github.com/brianmcentire/holiday-movie-picker)"
BATCH = 50                # films per request; a refused size drops to 10, the documented limit elsewhere in the API
SMALL_BATCH = 10
PAUSE = 1.5               # seconds between requests: well inside MDBList's 300 writes per 5 minutes
TIMEOUT = 20
MAX_REQUESTS = 300
DAILY_RESERVE = 100       # stop before the day's free allowance drops below this
RETRY_AFTER_DEFAULT = 60
RETRY_AFTER_CAP = 120
RETRIES_429 = 2
FAILED_BATCHES_STOP = 3

IMDB_ID = re.compile(r"^tt\d+$")
RT_PATH = re.compile(r"^m/[a-z0-9_\-]+$")
FIELDS = ("imdb", "tomatoes", "popcorn", "imdb_id", "rt", "at")
SCORE_FIELDS = ("imdb", "tomatoes", "popcorn")


class TooBig(Exception):
    """MDBList refused this many films in one request."""


class Stop(Exception):
    """Ends the run early. keep_file: write nothing at all (a refused key)."""
    def __init__(self, message, keep_file=False):
        super().__init__(message)
        self.keep_file = keep_file


def load_api_key():
    key = os.environ.get("MDBLIST_API_KEY")
    env_file = Path(os.environ.get("SCORES_ENV_FILE", BASE_DIR / ".env"))  # tests point this elsewhere
    if not key and env_file.exists():
        for line in env_file.read_text().splitlines():
            name, _, value = line.partition("=")
            if name.strip() == "MDBLIST_API_KEY":
                key = value.strip().strip("\"'")
    return key or None


_secret = None


def scrub(text):
    text = str(text)
    if _secret:
        text = text.replace(_secret, "***").replace(urllib.parse.quote(_secret), "***")
    return re.sub(r"apikey=[^&\s\"']+", "apikey=***", text)


def log(message):
    print(scrub(message), flush=True)


def wait(seconds):
    time.sleep(seconds * WAIT_SCALE)


# ---------- Values: anything that isn't a sensible score or id counts as unknown ----------

def _number(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def valid_imdb(value):
    number = _number(value)
    return round(number, 1) if number is not None and 1.0 <= number <= 10.0 else None


def valid_percent(value):
    # 0 is how MDBList says "no score", as the Jellyfin plugin also reads it
    number = _number(value)
    return int(round(number)) if number is not None and 1 <= number <= 100 else None


def valid_imdb_id(value):
    return value if isinstance(value, str) and IMDB_ID.match(value) else None


def valid_rt(value):
    if not isinstance(value, str):
        return None
    path = value.strip()
    for prefix in ("https://www.rottentomatoes.com", "http://www.rottentomatoes.com", "https://rottentomatoes.com"):
        if path.startswith(prefix):
            path = path[len(prefix):]
    path = path.strip("/")
    return path if RT_PATH.match(path) else None


def fields_from_item(item):
    """The valid fields in one MDBList batch item, and its TMDB id."""
    ids = item.get("ids") or {}
    tmdb = ids.get("tmdb")
    if tmdb is None:
        return None, {}
    found = {}
    ratings = {r.get("source"): r for r in item.get("ratings") or [] if isinstance(r, dict)}
    imdb = valid_imdb((ratings.get("imdb") or {}).get("value"))
    tomatoes = valid_percent((ratings.get("tomatoes") or {}).get("value"))
    # The audience score was called tomatoesaudience on MDBList's older host
    audience = ratings.get("popcorn") or ratings.get("tomatoesaudience") or {}
    popcorn = valid_percent(audience.get("value"))
    imdb_id = valid_imdb_id(ids.get("imdb")) or valid_imdb_id(item.get("imdb_id"))
    rt = valid_rt((ratings.get("tomatoes") or {}).get("url")) or valid_rt(audience.get("url"))
    for name, value in (("imdb", imdb), ("tomatoes", tomatoes), ("popcorn", popcorn), ("imdb_id", imdb_id), ("rt", rt)):
        if value is not None:
            found[name] = value
    return str(tmdb), found


def clean_entry(entry):
    """An old entry with anything invalid left out, so a hand-edited or older file can't carry bad values forward."""
    if not isinstance(entry, dict):
        return {}
    checks = {"imdb": valid_imdb, "tomatoes": valid_percent, "popcorn": valid_percent,
              "imdb_id": valid_imdb_id, "rt": valid_rt}
    kept = {}
    for name, check in checks.items():
        value = check(entry.get(name))
        if value is not None:
            kept[name] = value
    at = entry.get("at")
    if isinstance(at, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", at) and any(f in kept for f in SCORE_FIELDS):
        kept["at"] = at
    return kept


# ---------- MDBList ----------

class Client:
    def __init__(self, key):
        self.key = key
        self.requests = 0
        self.last = None
        self.remaining = None  # the day's free requests left, from MDBList's headers

    def _send(self, ids):
        if self.requests >= MAX_REQUESTS:
            raise Stop(f"Stopped at the {MAX_REQUESTS}-request cap for one run.")
        if self.remaining is not None and self.remaining < DAILY_RESERVE:
            raise Stop(f"Stopped with {self.remaining} of today's MDBList requests left, to leave room for the browser script.")
        if self.last is not None:
            gap = PAUSE - (time.monotonic() - self.last) / WAIT_SCALE
            if gap > 0:
                wait(gap)
        self.requests += 1
        self.last = time.monotonic()
        url = f"{API_ROOT}/tmdb/movie/?" + urllib.parse.urlencode({"apikey": self.key})
        request = urllib.request.Request(url, data=json.dumps({"ids": [int(i) for i in ids]}).encode(), method="POST",
                                         headers={"Content-Type": "application/json", "Accept": "application/json",
                                                  "User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
                self._note_remaining(response.headers)
                return response.status, json.loads(response.read().decode("utf-8")), response.headers
        except urllib.error.HTTPError as error:
            self._note_remaining(error.headers)
            try:
                body = error.read()[:300].decode("utf-8", "replace")
            except Exception:
                body = ""
            return error.code, body, error.headers

    def _note_remaining(self, headers):
        try:
            value = headers.get("X-RateLimit-Remaining") if headers else None
            if value is not None:
                self.remaining = int(value)
        except (TypeError, ValueError):
            pass

    def batch(self, ids):
        """The batch's items, or None when it failed after a retry. Raises Stop when the run should end,
        and TooBig when MDBList refuses the batch size."""
        tries_429 = 0
        tried_again = False
        while True:
            try:
                status, body, headers = self._send(ids)
            except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError, UnicodeDecodeError) as error:
                status, body, headers = None, scrub(error), None
            if status == 200:
                if isinstance(body, list):
                    return body
                status, body = None, "the reply wasn't a list"
            if status in (401, 403):
                raise Stop(f"MDBList refused the key (HTTP {status}). Check MDBLIST_API_KEY. scores.json is unchanged.",
                           keep_file=True)
            if status == 429:
                if "Daily" in str(body):
                    raise Stop("MDBList's daily limit is used up for today.")
                if tries_429 >= RETRIES_429:
                    raise Stop("MDBList kept saying too many requests; stopping for this run.")
                tries_429 += 1
                try:
                    pause = int((headers or {}).get("Retry-After") or RETRY_AFTER_DEFAULT)
                except (TypeError, ValueError):
                    pause = RETRY_AFTER_DEFAULT
                pause = max(1, min(pause, RETRY_AFTER_CAP))
                log(f"  MDBList asked us to slow down; waiting {pause}s.")
                wait(pause)
                continue
            if status == 400 and len(ids) > SMALL_BATCH:
                raise TooBig()
            # A server error, a network error or something unexpected: one more try, then skip this batch
            if not tried_again:
                tried_again = True
                log(f"  A batch failed ({'HTTP ' + str(status) if status else scrub(body)}); trying it once more in 5s.")
                wait(5)
                continue
            log(f"  Skipped a batch of {len(ids)} films ({'HTTP ' + str(status) if status else scrub(body)}); "
                "their old scores stay.")
            return None


def fetch_scores(client, ids):
    """{tmdb id: fields} for every film MDBList returned, the ids in batches it answered, and why the run stopped
    early (or None)."""
    found, answered = {}, set()
    queue = [ids[i:i + BATCH] for i in range(0, len(ids), BATCH)]
    failed_in_a_row = 0
    while queue:
        chunk = queue.pop(0)
        try:
            items = client.batch(chunk)
        except TooBig:
            log(f"  MDBList refused {len(chunk)} films in one request; using batches of {SMALL_BATCH}.")
            small = [c[i:i + SMALL_BATCH] for c in [chunk] + queue for i in range(0, len(c), SMALL_BATCH)]
            queue = small
            continue
        except Stop as stop:
            if stop.keep_file:
                raise
            return found, answered, str(stop)
        if items is None:
            failed_in_a_row += 1
            if failed_in_a_row >= FAILED_BATCHES_STOP:
                return found, answered, f"Stopped after {FAILED_BATCHES_STOP} failed batches in a row."
            continue
        failed_in_a_row = 0
        wanted = set(chunk)
        answered |= wanted
        for item in items:
            if not isinstance(item, dict):
                continue
            tmdb, fields = fields_from_item(item)
            if tmdb in wanted:
                found[tmdb] = fields
    return found, answered, None


# ---------- Wikidata, for IMDb ids and RT paths MDBList doesn't have ----------

def fetch_wikidata(ids):
    query = ('SELECT ?tmdb ?imdb ?rt WHERE { VALUES ?tmdb { %s } ?film wdt:P4947 ?tmdb . '
             'OPTIONAL { ?film wdt:P345 ?imdb } OPTIONAL { ?film wdt:P1258 ?rt } }') % " ".join(f'"{i}"' for i in ids)
    request = urllib.request.Request(WIKIDATA_URL, method="POST",
                                     data=urllib.parse.urlencode({"query": query, "format": "json"}).encode(),
                                     headers={"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        rows = json.loads(response.read().decode("utf-8"))["results"]["bindings"]
    found = {}
    for row in rows:
        tmdb = row.get("tmdb", {}).get("value")
        if not tmdb:
            continue
        entry = found.setdefault(tmdb, {})
        imdb_id = valid_imdb_id(row.get("imdb", {}).get("value"))
        rt = valid_rt(row.get("rt", {}).get("value"))
        if imdb_id and "imdb_id" not in entry:
            entry["imdb_id"] = imdb_id
        if rt and "rt" not in entry:
            entry["rt"] = rt
    return found


# ---------- The file ----------

def read_old():
    if not OUT.exists():
        return {}
    try:
        data = json.loads(OUT.read_text())
        movies = data["movies"]
        if not isinstance(movies, dict):
            raise ValueError("movies isn't an object")
        return {str(k): clean_entry(v) for k, v in movies.items()}
    except Exception as error:
        log(f"WARNING: scores.json couldn't be read ({scrub(error)}). Starting from empty; its old scores are lost.")
        return {}


def render(updated, movies):
    lines = []
    for tmdb in sorted(movies, key=lambda i: (len(i), i)):
        entry = {f: movies[tmdb][f] for f in FIELDS if f in movies[tmdb]}
        lines.append(f"{json.dumps(tmdb)}: {json.dumps(entry, ensure_ascii=False, separators=(', ', ': '))}")
    head = f'{{"v": 1, "source": "MDBList", "updated": {json.dumps(updated)}, "movies": {{'
    return head + ("\n" + ",\n".join(lines) + "\n" if lines else "") + "}}\n"


def write_atomically(text):
    handle, temp = tempfile.mkstemp(dir=OUT.parent, prefix=".scores-", suffix=".json")
    try:
        with os.fdopen(handle, "w") as f:
            f.write(text)
        os.chmod(temp, 0o644)  # mkstemp makes it owner-only; the dev server and Pages serve it like the other files
        os.replace(temp, OUT)
    except BaseException:
        if os.path.exists(temp):
            os.unlink(temp)
        raise


def main():
    global _secret
    started = time.monotonic()
    key = load_api_key()
    if not key:
        log("Scores: no MDBLIST_API_KEY, so scores.json is unchanged.")
        return 0
    _secret = key

    try:
        catalog_ids = [str(i) for i in json.loads(CATALOG.read_text())["movies"]]
    except Exception as error:
        log(f"Scores: couldn't read {CATALOG.name} ({scrub(error)}), so scores.json is unchanged.")
        return 1
    if not catalog_ids:
        log(f"Scores: {CATALOG.name} has no films, so scores.json is unchanged.")
        return 0

    old_text = OUT.read_text() if OUT.exists() else None
    old = read_old()
    try:
        old_updated = json.loads(old_text).get("updated") if old_text else None
    except Exception:
        old_updated = None

    client = Client(key)
    try:
        fetched, answered, stopped = fetch_scores(client, catalog_ids)
    except Stop as stop:
        log(f"Scores: {stop}")
        return 0
    if stopped:
        log(f"Scores: {stopped}")
    if not fetched:
        log(f"Scores: nothing fetched from MDBList in {client.requests} requests, so scores.json is unchanged.")
        return 0

    today = datetime.date.today().isoformat()
    movies, kept, not_found, scored_now = {}, 0, 0, 0
    for tmdb in catalog_ids:
        entry = dict(old.get(tmdb, {}))
        if tmdb in fetched:
            new = fetched[tmdb]
            entry.update(new)  # new valid values win; anything missing from the reply keeps its old value
            if any(f in new for f in SCORE_FIELDS):
                entry["at"] = today
                scored_now += 1
        elif tmdb in old:
            kept += 1
        if tmdb in answered and tmdb not in fetched:
            not_found += 1
        if entry:
            movies[tmdb] = entry

    wiki_filled = {"imdb_id": 0, "rt": 0}
    missing = [i for i in catalog_ids if "imdb_id" not in movies.get(i, {}) or "rt" not in movies.get(i, {})]
    if missing:
        try:
            wiki = fetch_wikidata(missing)
            for tmdb, ids in wiki.items():
                entry = movies.setdefault(tmdb, {})
                for name in ("imdb_id", "rt"):
                    if name in ids and name not in entry:
                        entry[name] = ids[name]
                        wiki_filled[name] += 1
        except Exception as error:
            log(f"  Wikidata didn't answer ({scrub(error)}); kept the IMDb ids and RT paths we had.")

    movies = {k: v for k, v in movies.items() if v}
    text = render(today, movies)
    if old_text is not None and render(old_updated or today, old) == render(old_updated or today, movies):
        log("Scores: no changes.")
    else:
        write_atomically(text)

    seconds = round(time.monotonic() - started)
    log(f"Scores: {len(fetched):,} films fetched in {client.requests} requests ({seconds // 60}m {seconds % 60}s); "
        f"{scored_now:,} with a score; {kept:,} kept from before; {not_found:,} not on MDBList.")
    coverage = {f: sum(1 for e in movies.values() if f in e) for f in ("imdb", "tomatoes", "popcorn", "imdb_id", "rt")}
    log("  Coverage of " + f"{len(catalog_ids):,} films: " + ", ".join(f"{name} {count:,}" for name, count in coverage.items())
        + f" (Wikidata filled {wiki_filled['imdb_id']} IMDb ids and {wiki_filled['rt']} RT paths).")
    if client.remaining is not None:
        log(f"  MDBList requests left today: {client.remaining}.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except BaseException:
        # A real bug: say what happened without the key, and fail so it gets noticed
        print(scrub(traceback.format_exc()), file=sys.stderr)
        sys.exit(1)
