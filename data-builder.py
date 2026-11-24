import asyncio
import aiohttp
import json
import os
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

import holiday_classifier
import musical_classifier

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent
TOTAL_MAX_LIMIT = 1500       # Safety cap, not a target
CONCURRENT_REQUESTS = 10 
MIN_VOTES = 10               # Waived for this year's releases, which haven't had time to collect votes...
THIS_YEAR = str(date.today().year)
NEW_RELEASE_DAYS = 183       # ...but only in their first 6 months: a film out longer with no votes has had the time
SHORT_FILM_MINUTES = 40      # ...and only at this length or more (the Academy's line between short and feature), so a
                             # short uploaded with a rating typed in doesn't get in (Dick Dangerous: Christmas EVIL, 6 min)
MIN_VOTES_R = 100            # R films need a real audience, with no new-release waiver
MIN_VOTES_UNRATED = 100      # Unrated films get in only if popular, like many Netflix originals (this year's: see STREAMER_NOTE)
UNRATED_CLASSIC_BEFORE = 1990  # ...except older ones, like TV specials that were never rated, which need only MIN_VOTES
MAX_RETRIES = 5
MAX_PAGES_PER_HOLIDAY = 100  # Safety cap on polling depth

HOLIDAY_QUERIES = {
    "Hanukkah": "7328|210405|335345|272884",
    "Kwanzaa": "260454|335346",
    "Lunar New Year": "356063|250496|373453",  # Includes 'Chinese New Year' and 'Spring Festival'
    "Thanksgiving": "4543",
    "New Year's": "613|209287",  # Combines 'New Year's Eve' and 'New Year'
    "Christmas": "207317|1441"   # 'christmas' plus 'christmas party' (Die Hard); 65 'holiday' is too tangential
}
# TMDB sometimes leaves the holiday keyword off, as with A Dog's Perfect Christmas,
# so also search titles for these words
TITLE_SEARCHES = {
    "Hanukkah": ["hanukkah", "chanukah"],
    "Christmas": ["christmas", "xmas"],
}

# Added regardless of tags or quality checks
ALWAYS_INCLUDE = {
    "1186957": "Christmas",  # The Velveteen Rabbit (2023)
    "927": "Christmas",      # Gremlins (1984), kept despite the horror rule
    "287903": "Christmas",   # Krampus (2015), kept despite the horror rule
    "28436": "Christmas",    # The Curse of the Cat People (1944), kept despite the horror rule
    "1567605": "Christmas",  # Ebenezer (2026), a Christmas Carol; kept despite the horror rule
}

# TODO(2027): reconsider Anna and the Apocalypse (2018, R, TMDB 461928), a zombie
# Christmas musical and cult favorite. The horror rule leaves it out; to include it,
# add it to ALWAYS_INCLUDE.

# Left out even though they match: not really holiday movies
SKIP_IDS = {
    "29381",   # Caged (1950)
    "25952",   # Conquest of Space (1955)
    "17696",   # Robin and the 7 Hoods (1964)
    "118716",  # The Day Mars Invaded Earth (1963)
    "1392796", # Diary of a Wimpy Kid: The Last Straw (2025), tagged christmas but isn't really
}

EARLIEST_YEAR = 1930  # Skips silent-era shorts

ALLOWED_CERTS = {'G', 'PG', 'PG-13', 'R', 'TV-G', 'TV-PG', 'TV-Y7', 'TV Movie'}

# Studios behind made-for-TV holiday movies, which usually have no US rating.
# Their unrated films are let in and labeled "TV Movie".
TV_STUDIOS = {
    4056: "Hallmark Entertainment", 6435: "Hallmark Productions", 53015: "Hallmark Media",
    9027: "Hallmark Hall of Fame Productions", 3431: "Lifetime",
}
# TMDB often lists only the company that made a TV movie for Hallmark or Lifetime, not the network. This year's
# unrated ones are let in anyway when a US release note or US streaming channel names the network (What If
# Christmas: Hallmark+, Nov 2026). From next year they need MIN_VOTES_UNRATED, unless TMDB has added the studio.
TV_NETWORK_NOTE = re.compile(r"hallmark|lifetime", re.IGNORECASE)

# Left out: these networks' films rate below Hallmark's and Lifetime's (IMDb 5.6 against 6.3 in Nov 2026), by
# studio or a US release note naming the network, unless also tied to Hallmark, Lifetime or a STREAMER_NOTE service.
LEFT_OUT_NETWORKS = {180934: "Great American Media", 210724: "UPtv", 111898: "UP Entertainment", 84068: "ION Television"}
LEFT_OUT_NETWORK_NOTE = re.compile(r"great american|gac family|\bup ?tv\b|^ion\b|^pax\b", re.IGNORECASE)  # Pax: ION's old name

# This year's unrated films from these streaming services skip MIN_VOTES_UNRATED, so they're in before they've
# had time to collect votes (Clashing Through the Snow, Prime Video, Nov 2026). From next year they need the
# votes like any unrated film. TMDB credits a streamer's film to the company that made it (Netflix's are often
# Motion Picture Corporation of America's), so the note on its US release, like "Netflix", names the service.
# Not a bare "Amazon": small films list it among the stores that rent them.
STREAMER_NOTE = re.compile(r"netflix|prime video|freevee|disney|apple tv", re.IGNORECASE)
API_ROOT = "https://api.themoviedb.org/3"

CORE_GENRES = {
    12: "Adventure", 16: "Animation", 35: "Comedy", 80: "Crime",
    18: "Drama", 10751: "Family", 14: "Fantasy", 27: "Horror",
    9648: "Mystery", 10749: "Romance", 53: "Thriller", 10402: "Music"
}

VIBE_TRIGGERS = {
    # --- LOCATIONS & SETTINGS ---
    "small town": "small town", "hometown": "small town", "village": "small town",
    "london": "london", "england": "london", "british": "london",
    "new york": "new york city", "nyc": "new york city", "manhattan": "new york city",
    "chicago": "chicago", "north pole": "north pole", "lapland": "north pole",
    "countryside": "countryside", "farm": "countryside", "vermont": "vermont",
    "wilderness": "forest", "woods": "forest", "forest": "forest",
    "hotel": "hotel", "resort": "hotel", "beach": "beach", "island": "beach",
    "castle": "royalty", "palace": "royalty", "prince": "royalty", "princess": "royalty",
    
    # --- MOODS & VIBES ---
    "cheerful": "cheerful", "heartwarming": "cheerful", "uplifting": "cheerful",
    "miserable": "miser", "miserly": "miser", "scrooge": "miser", "grumpy": "miser",
    "slapstick": "slapstick comedy", "funny": "slapstick comedy", "goofy": "slapstick comedy",
    "romance": "romance", "romcom": "romance", "fall in love": "romance", "dating": "romance",
    "supernatural": "supernatural", "ghost": "supernatural", "spirits": "supernatural",  # Not "spirit": it matches "Christmas spirit"
    "magic": "magic", "magical": "magic",
    
    # --- THE DARK SIDE ---
    "killer": "holiday horror", "serial killer": "holiday horror", "slasher": "holiday horror",
    "murder": "holiday horror", "blood": "holiday horror", "dark": "holiday horror",
    "sinister": "holiday horror", "terror": "holiday horror", "scary": "holiday horror",
    
    # --- TROPES & THEMES ---
    "saving christmas": "saving christmas", "save christmas": "saving christmas",
    "spirit of christmas": "christmas spirit", "party": "christmas party",
    "celebration": "christmas party", "office party": "christmas party",
    "vacation": "vacation", "trip": "vacation", "travel": "vacation",
    "remake": "remake", "novel": "literary", "book": "literary",
    "sequel": "sequel", "reindeer": "reindeer", "snowman": "snowman", "frosty": "snowman",
    "elves": "elves", "elf": "elves",
    
    # --- TIME & EVENTS ---
    "new year": "new year's eve", "thanksgiving": "thanksgiving", "halloween": "holiday overlap",
    "valentine": "holiday overlap", "winter": "winter", "snow": "winter", "icy": "winter",
    "19th century": "vintage", "victorian": "vintage", "black and white": "vintage",
    "retro": "vintage", "old-fashioned": "vintage"
}

# Whole words only (plus a plural), so "elf" doesn't match "himself"
VIBE_PATTERNS = [(re.compile(rf"\b{re.escape(trigger)}s?\b"), vibe) for trigger, vibe in VIBE_TRIGGERS.items()]

# Only counts on actual horror: "a cold, dark town" alone shouldn't make a PG cartoon a horror film
HORROR_GENRES = {"Horror", "Thriller"}

# Keeps R-rated holiday movies but not erotica
EXPLICIT_KEYWORDS = re.compile(r"softcore|erotic|sexploitation|sexsploitation|porn")

# Sidebar tags for the classifier's holiday levels; films it calls "none" are left out
LEVEL_TAGS = {"about": "holiday story", "set_at": "holiday setting", "adjacent": "holiday adjacent"}

# TMDB keywords that say nothing about the movie's content
JUNK_TAGS = {"aftercreditsstinger", "duringcreditsstinger", "woman director", "holiday"}

# The Musical pill (musical_classifier.py) replaces TMDB's patchy "musical" keyword and its "Music" genre. The
# classifiers still see both; they just don't reach the page, so a search for "musical" finds the pill's films.
REPLACED_TAGS = {"musical"}
HINT_ONLY_GENRES = {"Music"}

# The people search can find a film by (the page matches a surname, or a full name). For the 1,136 films of
# Nov 2026, these came to about 8,500 names (7,000 of them actors), adding 78KB to the compressed download.
# Actors: the first few billed, the first 10 for films most people have heard of, and anyone billed lower whom many
# people know (John Candy is 11th in Home Alone). Small films keep just their leads: past them it's bit parts.
CAST_TOP = 5
CAST_TOP_WELL_KNOWN = 10
CAST_WELL_KNOWN_VOTES = 500
PEOPLE_MIN_POPULARITY = 1.5   # TMDB's popularity for a person: bit parts are under 1; about the best-known 8% pass
# Crew: every director, the author of the book it's based on (Charles Dickens scores only 0.9), and screenwriters
# many people know (John Hughes). Producers are left out: finding a film through someone who isn't in it confuses.
SOURCE_AUTHOR_JOBS = {"Novel", "Book", "Short Story"}
WRITER_JOBS = {"Screenplay", "Writer", "Story"}

# The preview's "Trailer | More on IMDb" line: each film's IMDb id and one YouTube trailer, both from TMDB
IMDB_ID = re.compile(r"^tt\d+$")
YOUTUBE_KEY = re.compile(r"^[A-Za-z0-9_-]{11}$")
TRAILER_NAME = re.compile(r"trailer|preview", re.IGNORECASE)  # Hallmark calls its trailers previews
YOUTUBE_OEMBED = "https://www.youtube.com/oembed"  # Says whether a video still plays (see youtube_plays)
YOUTUBE_CONCURRENT = 4       # About 760 checks a build, a few at a time
MAX_TRAILERS_GONE = 0.1      # More than this share of trailers "gone" means YouTube's answers changed, not the videos

def load_api_key():
    """Read TMDB_API_KEY (the API Read Access Token) from the environment or a local .env file."""
    key = os.environ.get("TMDB_API_KEY")
    env_file = BASE_DIR / ".env"
    if not key and env_file.exists():
        for line in env_file.read_text().splitlines():
            name, _, value = line.partition("=")
            if name.strip() == "TMDB_API_KEY":
                key = value.strip().strip("\"'")
    if not key:
        sys.exit("TMDB_API_KEY not found. Add it to .env or set it in your environment.")
    return key

async def fetch_with_retry(session, url, params, semaphore):
    for attempt in range(MAX_RETRIES):
        async with semaphore:
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status == 200: return await resp.json()
                    if resp.status == 401: sys.exit("TMDB rejected TMDB_API_KEY. Use the API Read Access Token, not the short API Key.")
                    if resp.status == 404: return {}  # Not on TMDB, as when it merges away a duplicate film; None means no answer
                    if resp.status == 429: await asyncio.sleep(2 + (2 ** attempt))
                    # A passing server error would otherwise end a search's paging early and drop its later pages
                    elif resp.status >= 500: await asyncio.sleep(2 ** attempt)
                    else: return None
            except (aiohttp.ClientError, asyncio.TimeoutError): await asyncio.sleep(2 ** attempt)
    return None

DETAIL_PARAMS = {"append_to_response": "keywords,release_dates,credits,watch/providers,videos",
                 "include_video_language": "en,null"}  # English videos, and those with no language set

async def fetch_movie_details(session, semaphore, movie_id):
    data = await fetch_with_retry(session, f"{API_ROOT}/movie/{movie_id}", DETAIL_PARAMS, semaphore)
    if not data: return None
    return (*parse_details(data), data)

def parse_details(data):
    """(keywords, US rating, genres) from a film's TMDB details."""
    # Spacing tidied ("family business " → "family business"); the page's links match tags ignoring spacing
    keywords = [' '.join(k['name'].split()) for k in data.get('keywords', {}).get('keywords', [])]
    
    # Explicit Genre Mapping
    raw_genres = data.get('genres', [])
    genres = []
    for g in raw_genres:
        if g['id'] in CORE_GENRES:
            genres.append(CORE_GENRES[g['id']])
            
    cert = "NR"
    for country in data.get('release_dates', {}).get('results', []):
        if country['iso_3166_1'] == 'US':
            # Prefer a real rating when TMDB lists both, e.g. NR and G
            certs = [r['certification'] for r in country['release_dates'] if r.get('certification')]
            rated = [c for c in certs if c != "NR"]
            cert = (rated or certs or ["NR"])[0]
            break
    return keywords, cert, genres

def load_last_build():
    """The holiday_engine.json this build replaces (last week's), or {} if there isn't one."""
    try:
        return json.loads((BASE_DIR / "holiday_engine.json").read_text())
    except (OSError, ValueError):
        return {}

def us_notes(data):
    """The notes on the film's US releases, like "Netflix" or "Hallmark Channel"."""
    return [(r.get('note') or '').strip() for country in data.get('release_dates', {}).get('results', [])
            if country['iso_3166_1'] == 'US' for r in country['release_dates']]

def us_channels(data):
    """The US subscription services streaming the film, like "Hallmark+ Amazon Channel" (JustWatch's data, via TMDB)."""
    return [p['provider_name'] for p in data.get('watch/providers', {}).get('results', {}).get('US', {}).get('flatrate', [])]

def from_streamer(data):
    """True when a note on the film's US release names a STREAMER_NOTE service."""
    return any(STREAMER_NOTE.search(n) for n in us_notes(data))

def tied_to(data, studios, note):
    """True when one of the studios made the film, or a note on its US release matches."""
    return (any(c['id'] in studios for c in data.get('production_companies', []))
            or any(note.search(n) for n in us_notes(data)))

def from_left_out_network(data):
    return (tied_to(data, LEFT_OUT_NETWORKS, LEFT_OUT_NETWORK_NOTE)
            and not (tied_to(data, TV_STUDIOS, TV_NETWORK_NOTE) or from_streamer(data)))

def pick_people(credits, votes):
    """The people search finds this film by, as {"cast": [...], "directors": [...], "writers": [...]} with
    names in billing order; empty lists are left out to keep the file small."""
    credits = credits or {}
    top = CAST_TOP_WELL_KNOWN if votes >= CAST_WELL_KNOWN_VOTES else CAST_TOP
    known = lambda c: (c.get('popularity') or 0) >= PEOPLE_MIN_POPULARITY
    cast = sorted((c for c in credits.get('cast', []) if c.get('order', 999) < top or known(c)),
                  key=lambda c: c.get('order', 999))
    crew = credits.get('crew', [])
    directors = [c for c in crew if c.get('job') == 'Director']
    writers = [c for c in crew if c.get('job') in SOURCE_AUTHOR_JOBS or (c.get('job') in WRITER_JOBS and known(c))]
    people = {}
    for field, group in (("cast", cast), ("directors", directors), ("writers", writers)):
        names = list(dict.fromkeys(' '.join(c['name'].split()) for c in group if (c.get('name') or '').strip()))
        if names: people[field] = names
    return people

def rank_trailers(videos):
    """The film's YouTube trailers as video ids, best first; the preview links to the first that still plays (see
    check_trailers). Most films have one or none; for the rest (98 of 1,038 in Nov 2026), official ones first (from
    the studio's own channel), then ones named as a trailer or preview (Die Hard's "The Greatest Christmas Story" is
    filed as a trailer), then the sharpest, then the newest. Newest picks an anniversary or 4K re-release trailer
    over a blurry original. TMDB has no view counts."""
    trailers = [v for v in (videos or {}).get('results', [])
                if v.get('site') == 'YouTube' and v.get('type') == 'Trailer' and YOUTUBE_KEY.match(v.get('key') or '')]
    return [v['key'] for v in sorted(trailers, reverse=True, key=lambda v: (
        bool(v.get('official')), bool(TRAILER_NAME.search(v.get('name') or '')), v.get('size') or 0, v.get('published_at') or ''))]

def links(details):
    """{"imdb_id": ..., "trailer": ...} for the preview, each left out when TMDB has none."""
    found = {"imdb_id": details.get('imdb_id') if IMDB_ID.match(details.get('imdb_id') or '') else None,
             "trailer": next(iter(rank_trailers(details.get('videos'))), None)}
    return {k: v for k, v in found.items() if v}

async def youtube_plays(session, semaphore, key):
    """Whether the YouTube video still plays, from YouTube's oEmbed address (public, no key): False for a private
    (403) or removed (404) video; True when it plays (200), or plays on YouTube but not in other sites' players
    (401, which an age check also gives). None when YouTube doesn't answer or answers something else."""
    params = {"url": f"https://www.youtube.com/watch?v={key}", "format": "json"}
    for attempt in range(3):
        async with semaphore:
            try:
                async with session.get(YOUTUBE_OEMBED, params=params) as resp:
                    if resp.status in (403, 404): return False
                    if resp.status in (200, 401): return True
                    if resp.status != 429 and resp.status < 500: return None
            except (aiohttp.ClientError, asyncio.TimeoutError):
                pass
        await asyncio.sleep(2 ** attempt)
    return None

async def check_trailers(movies, options, log):
    """Swaps each film's trailer that YouTube calls private or removed for its next one that plays, or drops it, so
    the preview shows only More on IMDb. TMDB keeps listing videos their owners have since hidden (Fox's own
    Miracle on 34th Street trailer, 8 of 764 in Nov 2026). options: {film id: its trailers, best first}; a film
    kept from last week's build has only its pick. A trailer YouTube didn't answer about stays."""
    picks = {mid: m['trailer'] for mid, m in movies.items() if m.get('trailer')}
    semaphore = asyncio.Semaphore(YOUTUBE_CONCURRENT)
    # Its own session: the TMDB token stays with TMDB
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        answers = await asyncio.gather(*[youtube_plays(session, semaphore, key) for key in picks.values()])
        gone = [mid for mid, plays in zip(picks, answers) if plays is False]
        if len(gone) > MAX_TRAILERS_GONE * len(picks):
            log(f"Warning: YouTube called {len(gone)} of {len(picks)} trailers private or removed, too many to trust; all stay.")
            return
        for mid in gone:
            later = [key for key in options.get(mid, []) if key != picks[mid]]
            replacement = None
            for key in later:
                if await youtube_plays(session, semaphore, key) is not False:
                    replacement = key
                    break
            if replacement:
                movies[mid]['trailer'] = replacement
            else:
                del movies[mid]['trailer']
            log(f"Trailer {'replaced' if replacement else 'dropped'} (private or removed on YouTube): {movies[mid]['title']}")
    unanswered = sum(plays is None for plays in answers)
    if unanswered:
        log(f"Trailers: YouTube didn't answer about {unanswered}; they stay.")

def word_pattern(word):
    """A title-search word as a whole word, so "xmas" doesn't match "Xmasters"."""
    return re.compile(rf"\b{word}\b", re.IGNORECASE)

def passes_quick_checks(m, title_pattern):
    """Checks that need no extra request, run before fetching a movie's details."""
    if title_pattern and not title_pattern.search(m.get('title', '')): return False
    is_new = (m.get('release_date') or '').startswith(THIS_YEAR)
    return (m.get('original_language') == 'en' and bool(m.get('poster_path'))
            and (m.get('vote_count', 0) >= MIN_VOTES or is_new))

def new_feature(data):
    """True for a film released in the last NEW_RELEASE_DAYS (or not out yet) that runs at least SHORT_FILM_MINUTES
    (or has no runtime listed yet, as before release): what this year's MIN_VOTES waiver is for."""
    try:
        age = (date.today() - date.fromisoformat(data.get('release_date') or '')).days
    except ValueError:
        return False
    runtime = data.get('runtime') or 0
    return age <= NEW_RELEASE_DAYS and (runtime == 0 or runtime >= SHORT_FILM_MINUTES)

def passes_gate(m, keywords, cert, genres, data):
    """The quality gate, once a film's details are in: its rating as the page shows it (unrated films from TV
    studios become "TV Movie"), or None to leave it out."""
    is_new = (m.get('release_date') or '').startswith(THIS_YEAR)
    if m.get('vote_count', 0) < MIN_VOTES and not new_feature(data): return None
    if cert == "NR" and any(c['id'] in TV_STUDIOS for c in data.get('production_companies', [])):
        cert = "TV Movie"
    elif cert == "NR" and is_new and any(TV_NETWORK_NOTE.search(x) for x in us_notes(data) + us_channels(data)):
        cert = "TV Movie"
    if "Horror" in genres or str(m['id']) in SKIP_IDS or from_left_out_network(data): return None
    year = (m.get('release_date') or '9999')[:4]
    if year < str(EARLIEST_YEAR): return None
    votes = m.get('vote_count', 0)
    unrated_min = MIN_VOTES if year < str(UNRATED_CLASSIC_BEFORE) else MIN_VOTES_UNRATED
    if is_new and from_streamer(data): unrated_min = 0
    if cert not in ALLOWED_CERTS and not (cert == 'NR' and votes >= unrated_min): return None
    if cert == 'R' and votes < MIN_VOTES_R: return None
    if cert in ('R', 'NR') and any(EXPLICIT_KEYWORDS.search(k.lower()) for k in keywords): return None
    return cert

# New Year's and Thanksgiving films also need a holiday or Christmas keyword, so they're likely about the season
SEASON_ANCHORED = {"New Year's", "Thanksgiving"}
ANCHOR_TAGS = {"holiday", "holidays", "christmas"}

def anchored(holiday_name, keywords):
    return holiday_name not in SEASON_ANCHORED or any(k.lower() in ANCHOR_TAGS for k in keywords)

def holidays_of(data, keywords):
    """The holidays the searches would find a film under now, by a holiday keyword or a word in its title."""
    ids = {str(k['id']) for k in data.get('keywords', {}).get('keywords', [])}
    found = [name for name, query in HOLIDAY_QUERIES.items() if ids & set(query.split('|')) and anchored(name, keywords)]
    found += [name for name, words in TITLE_SEARCHES.items() if any(word_pattern(w).search(data.get('title', '')) for w in words)]
    return list(dict.fromkeys(found))

async def main():
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    movie_metadata = {}
    inverted_index = defaultdict(list)
    genre_index = defaultdict(list)
    film_info = {}  # What the holiday classifier sees about each film
    trailer_options = {}  # Each film's trailers, best first, for check_trailers
    failed_lookups = 0

    def add_holiday_tag(mid, holiday_name):
        # Prevent 'christmas' tag flooding: only other holidays get a tag
        tag = holiday_name.lower()
        if holiday_name != "Christmas" and mid not in inverted_index[tag]:
            inverted_index[tag].append(mid)

    def add_movie(m, keywords, cert, genres, holiday_name, details):
        mid = str(m['id'])
        movie_metadata[mid] = {
            "title": m['title'], "overview": m['overview'],
            "rating": m['vote_average'],
            "votes": m.get('vote_count', 0),
            "cert": cert, "date": m.get('release_date') or '0000-00-00',
            "poster": f"https://image.tmdb.org/t/p/w500{m['poster_path']}",
            **links(details),
            **pick_people(details.get('credits'), m.get('vote_count', 0)),
        }
        trailer_options[mid] = rank_trailers(details.get('videos'))

        # Vibes -> Lowercase
        movie_vibes = {v.lower() for v in keywords} - JUNK_TAGS - REPLACED_TAGS
        movie_vibes.add(cert.lower())

        overview_lower = m['overview'].lower()
        for pattern, vibe in VIBE_PATTERNS:
            if vibe == "holiday horror" and not HORROR_GENRES.intersection(genres): continue
            if pattern.search(overview_lower): movie_vibes.add(vibe.lower())

        for v in movie_vibes: inverted_index[v].append(mid)
        for g in genres:
            if g not in HINT_ONLY_GENRES: genre_index[g].append(mid)
        add_holiday_tag(mid, holiday_name)
        film_info[mid] = {"title": m['title'], "year": (m.get('release_date') or '')[:4] or "unknown",
                          "genres": genres, "keywords": keywords, "overview": m['overview']}

    def keep_last_weeks_copy(mid, last):
        """For a film TMDB didn't answer about this time: its entry, tags and genres as last week's build had them.
        Its holiday level and Musical pill answer come from their saved files, as for every film."""
        m = last["movies"][mid]
        movie_metadata[mid] = m
        level_tags = set(LEVEL_TAGS.values())
        tags = [t for t, ids in last.get("index", {}).items() if mid in ids and t not in level_tags]
        genres = [g for g, ids in last.get("genres", {}).items() if mid in ids and g != musical_classifier.GENRE]
        for t in tags: inverted_index[t].append(mid)
        for g in genres: genre_index[g].append(mid)
        film_info[mid] = {"title": m["title"], "year": m["date"][:4], "genres": genres, "keywords": tags,
                          "overview": m["overview"]}

    headers = {"Authorization": f"Bearer {load_api_key()}"}
    async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as session:
        print("Season's Screenings: Starting Movie Data Source Build...", file=sys.stderr)

        discover_url, search_url = f"{API_ROOT}/discover/movie", f"{API_ROOT}/search/movie"
        discover_base = {
            "sort_by": "popularity.desc",
            "with_original_language": "en",
            "certification_country": "US",
            "with_release_type": "2|3|4|6",  # Includes 4 (Netflix/Digital) and 6 (Hallmark/TV)
            "region": "US"
        }
        # Each search: (holiday, url, params, title pattern to require)
        searches = [(name, discover_url, {**discover_base, "with_keywords": ids, "vote_count.gte": MIN_VOTES}, None)
                    for name, ids in HOLIDAY_QUERIES.items()]
        # This year's releases haven't had time to collect votes
        searches += [(name, discover_url, {**discover_base, "with_keywords": ids, "primary_release_date.gte": f"{THIS_YEAR}-01-01"}, None)
                     for name, ids in HOLIDAY_QUERIES.items()]
        # TV studios' films often lack the US release details the searches above require
        studio_ids = "|".join(str(i) for i in TV_STUDIOS)
        searches += [(name, discover_url, {"sort_by": "popularity.desc", "with_original_language": "en", "with_keywords": ids, "with_companies": studio_ids}, None)
                     for name, ids in HOLIDAY_QUERIES.items()]
        # Title searches go one release year at a time. TMDB's search stops at 2,000 results, which "christmas" passes
        # (5,002 in Nov 2026), and its pages repeat some films in place of others, a different set each run (2,000 rows
        # held 1,586 films), so films came and went from week to week. A year's search gives each film once. Through
        # next year, as the single search did; a film with no release date isn't found, and none in the catalog lacked one.
        years = range(EARLIEST_YEAR, int(THIS_YEAR) + 2)
        searches += [(name, search_url, {"query": word, "include_adult": "false", "primary_release_year": year}, word_pattern(word))
                     for name, words in TITLE_SEARCHES.items() for word in words for year in years]

        for holiday_name, url, base_params, title_pattern in searches:
            label = f'{holiday_name} titles: "{base_params["query"]}"' if "query" in base_params else holiday_name
            year = base_params.get("primary_release_year")
            if year in (None, years[0]):
                print(f"\n--- Fetching: {label}{', one release year at a time' if year else ''} ---", file=sys.stderr)
            if year: label += f" {year}"

            for page in range(1, MAX_PAGES_PER_HOLIDAY + 1):
                print(f"[{label}] Page {page}...", file=sys.stderr, end='\r')
                resp = await fetch_with_retry(session, url, {**base_params, "page": page}, semaphore)
                if not resp or not resp.get('results'): break

                candidates = []
                for c in resp['results']:
                    if str(c['id']) in movie_metadata:
                        add_holiday_tag(str(c['id']), holiday_name)  # Already added under an earlier search
                    elif passes_quick_checks(c, title_pattern):
                        candidates.append(c)
                details = await asyncio.gather(*[fetch_movie_details(session, semaphore, c['id']) for c in candidates])

                valid_in_batch = 0
                for m, detail in zip(candidates, details):
                    if detail is None:
                        failed_lookups += 1
                        continue
                    keywords, cert, genres, data = detail
                    if str(m['id']) in movie_metadata: continue  # Listed twice on one page
                    cert = passes_gate(m, keywords, cert, genres, data)
                    if not cert or not anchored(holiday_name, keywords): continue
                    if len(movie_metadata) >= TOTAL_MAX_LIMIT: break
                    add_movie(m, keywords, cert, genres, holiday_name, data)
                    valid_in_batch += 1

                if valid_in_batch or not year:  # The year searches log only the years that add films
                    print(f"[{label}] Added {valid_in_batch}. Total: {len(movie_metadata)}", file=sys.stderr)
                if len(movie_metadata) >= TOTAL_MAX_LIMIT or page >= resp.get('total_pages', 1): break

        # Last week's films the searches missed this time stay if they still pass: still found under a holiday by
        # keyword or title, and through the same checks. Searches can still miss a film (a lookup that fails, TMDB
        # shuffling results), and before the year searches, 8 films came and 7 went between two runs 7 hours apart.
        # If TMDB doesn't answer about one, last week's copy stays; if TMDB has deleted it, it goes.
        last = load_last_build()
        missed = [mid for mid in last.get("movies", {}) if mid not in movie_metadata and mid not in ALWAYS_INCLUDE]
        print(f"\n--- Checking {len(missed)} films from last week's build that the searches didn't add ---", file=sys.stderr)
        answers = await asyncio.gather(*[fetch_with_retry(session, f"{API_ROOT}/movie/{mid}", DETAIL_PARAMS, semaphore)
                                         for mid in missed])
        for mid, data in zip(missed, answers):
            title = last["movies"][mid]["title"]
            if data is None:
                keep_last_weeks_copy(mid, last)
                print(f"Kept last week's copy (no answer from TMDB): {title}", file=sys.stderr)
                continue
            keywords, cert, genres = parse_details(data) if data else ([], None, [])
            holidays = holidays_of(data, keywords) if data else []
            if holidays and passes_quick_checks(data, None): cert = passes_gate(data, keywords, cert, genres, data)
            else: cert = None
            if not cert:
                print(f"Dropped, no longer qualifies{'' if data else ' (gone from TMDB)'}: {title}", file=sys.stderr)
                continue
            add_movie(data, keywords, cert, genres, holidays[0], data)
            for h in holidays[1:]: add_holiday_tag(mid, h)
            print(f"Kept: {title}", file=sys.stderr)

        missing = [mid for mid in ALWAYS_INCLUDE if mid not in movie_metadata]
        details = await asyncio.gather(*[fetch_movie_details(session, semaphore, mid) for mid in missing])
        for mid, detail in zip(missing, details):
            if detail is None:
                print(f"\nWarning: always-include movie {mid} wasn't found on TMDB.", file=sys.stderr)
                continue
            keywords, cert, genres, data = detail
            if not data.get('poster_path'):
                print(f"\nWarning: always-include movie {mid} has no poster on TMDB yet, skipping.", file=sys.stderr)
                continue
            add_movie(data, keywords, cert, genres, ALWAYS_INCLUDE[mid], data)

        if not movie_metadata:
            sys.exit("\nBuild failed: no titles found. Keeping the existing holiday_engine.json.")

        print("", file=sys.stderr)
        levels, unsorted = await holiday_classifier.classify_all(film_info, lambda msg: print(msg, file=sys.stderr))
        left_out = {mid for mid, level in levels.items() if level == "none" and mid not in ALWAYS_INCLUDE}
        for mid, level in levels.items():
            if mid not in left_out:
                inverted_index[LEVEL_TAGS.get(level, LEVEL_TAGS["adjacent"])].append(mid)
        for mid in left_out:
            del movie_metadata[mid]
        for index in (inverted_index, genre_index):
            for tag in list(index):
                index[tag] = [mid for mid in index[tag] if mid not in left_out]
                if not index[tag]: del index[tag]
        if left_out:
            print(f"Left out {len(left_out)} films with no real holiday connection (see holiday_levels_review.md).", file=sys.stderr)

        # The Musical pill, from the answers saved in musicals.json; new films join it once they're asked about
        musical, unanswered = musical_classifier.pill_films(list(movie_metadata))
        if musical: genre_index[musical_classifier.GENRE] = musical
        if unanswered:
            print(f"Musical pill: {musical_classifier.plural(unanswered, 'film')} with no answer yet (make musicals).", file=sys.stderr)

        print("\n--- Checking the trailers still play on YouTube ---", file=sys.stderr)
        await check_trailers(movie_metadata, trailer_options, lambda msg: print(msg, file=sys.stderr))

        # The page shows a quiet note when films are waiting for a holiday level or the data is stale
        status = {"built": date.today().isoformat(), "unsorted": unsorted}
        output = {"index": inverted_index, "movies": movie_metadata, "genres": genre_index, "status": status}
        with open(BASE_DIR / "holiday_engine.json", "w") as f:
            json.dump(output, f, indent=2)

        print(f"\nBuild Success: {len(movie_metadata)} titles saved.")
        if unsorted:
            print(f"Warning: {unsorted} titles have no holiday level yet.", file=sys.stderr)
        if failed_lookups:
            print(f"Warning: {failed_lookups} movie lookups failed and were skipped.", file=sys.stderr)

if __name__ == "__main__":
    asyncio.run(main())