import asyncio
import aiohttp
import json
import os
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent
TOTAL_MAX_LIMIT = 1500       # Safety cap, not a target
CONCURRENT_REQUESTS = 10 
MIN_VOTES = 10               # Waived for this year's releases, which haven't had time to collect votes
THIS_YEAR = str(date.today().year)
MAX_RETRIES = 5
MAX_PAGES_PER_HOLIDAY = 100  # Safety cap on polling depth

HOLIDAY_QUERIES = {
    "Hanukkah": "7328|210405|335345|272884",
    "Kwanzaa": "260454|335346",
    "Lunar New Year": "356063|250496|373453",  # Includes 'Chinese New Year' and 'Spring Festival'
    "Thanksgiving": "4543",
    "New Year's": "613|209287",  # Combines 'New Year's Eve' and 'New Year'
    "Christmas": "207317"        # Strict Christmas keyword vs 65 includes more tangential releases
}
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
    "musical": "musical", "singing": "musical", "songs": "musical",
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

# TMDB keywords that say nothing about the movie's content
JUNK_TAGS = {"aftercreditsstinger", "duringcreditsstinger", "woman director", "holiday"}

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
                    if resp.status == 429: await asyncio.sleep(2 + (2 ** attempt))
                    else: return None
            except (aiohttp.ClientError, asyncio.TimeoutError): await asyncio.sleep(2 ** attempt)
    return None

async def fetch_movie_details(session, semaphore, movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {"append_to_response": "keywords,release_dates"}
    data = await fetch_with_retry(session, url, params, semaphore)
    if not data: return None
    
    keywords = [k['name'] for k in data.get('keywords', {}).get('keywords', [])]
    
    # Explicit Genre Mapping
    raw_genres = data.get('genres', [])
    genres = []
    for g in raw_genres:
        if g['id'] in CORE_GENRES:
            genres.append(CORE_GENRES[g['id']])
            
    cert = "NR"
    for country in data.get('release_dates', {}).get('results', []):
        if country['iso_3166_1'] == 'US':
            for release in country['release_dates']:
                if release.get('certification'):
                    cert = release['certification']
                    break
            break
    return keywords, cert, genres

async def main():
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    movie_metadata = {}
    inverted_index = defaultdict(list)
    genre_index = defaultdict(list)
    final_ids = []
    failed_lookups = 0
    
    headers = {"Authorization": f"Bearer {load_api_key()}"}
    async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as session:
        print(f"Set Flicks: Starting Movie Data Source Build...", file=sys.stderr)
        
        searches = [(name, ids, {"vote_count.gte": MIN_VOTES}) for name, ids in HOLIDAY_QUERIES.items()]
        searches += [(name, ids, {"primary_release_date.gte": f"{THIS_YEAR}-01-01"}) for name, ids in HOLIDAY_QUERIES.items()]
        
        for holiday_name, keyword_id, filters in searches:
            print(f"\n--- Fetching: {holiday_name} ---", file=sys.stderr)
            page, holiday_count = 1, 0
            
            while True:
                # Limit polling depth
                if page > MAX_PAGES_PER_HOLIDAY:
                    print(f"[{holiday_name}] Reached max page limit ({MAX_PAGES_PER_HOLIDAY}). Moving on.", file=sys.stderr)
                    break

                print(f"[{holiday_name}] Page {page}...", file=sys.stderr, end='\r')
                url = "https://api.themoviedb.org/3/discover/movie"
                params = {
                    "with_keywords": keyword_id, **filters,
                    "sort_by": "popularity.desc", "page": page,
                    "with_original_language": "en", 
                    "certification_country": "US", 
                    "with_release_type": "2|3|4|6", # Added 4 (Netflix/Digital) and 6 (Hallmark/TV)
                    "region": "US"
                }
                
                resp = await fetch_with_retry(session, url, params, semaphore)
                if not resp or not resp.get('results'): break
                
                candidates = resp['results']
                tasks = [fetch_movie_details(session, semaphore, c['id']) for c in candidates]
                details = await asyncio.gather(*tasks)
                
                valid_in_batch = 0
                for i, detail in enumerate(details):
                    m, mid = candidates[i], str(candidates[i]['id'])
                    if detail is None:
                        failed_lookups += 1
                        continue
                    keywords, cert, genres = detail
                    
                    # Already added under an earlier holiday: just record this holiday too
                    if mid in movie_metadata:
                        holiday_tag = holiday_name.lower()
                        if holiday_name != "Christmas" and mid not in inverted_index[holiday_tag]:
                            inverted_index[holiday_tag].append(mid)
                        continue
                    
                    # Quality Gate
                    allowed_certs = ['G', 'PG', 'PG-13', 'TV-G', 'TV-PG', 'TV-Y7']
                    is_new = m.get('release_date', '').startswith(THIS_YEAR)
                    if cert not in allowed_certs or not m.get('poster_path') or (m.get('vote_count', 0) < MIN_VOTES and not is_new):
                        continue
                    
                    # For events like New Year's/Thanksgiving, require a "Holiday" or "Christmas" tag
                    # to ensure the movie is likely actually about the Holiday Season
                    if holiday_name in ["New Year's", "Thanksgiving"]:
                        anchor_tags = {"holiday", "holidays", "christmas"}
                        if not any(k.lower() in anchor_tags for k in keywords): 
                            continue

                    if len(final_ids) >= TOTAL_MAX_LIMIT: break
                    
                    final_ids.append(mid)
                    movie_metadata[mid] = {
                        "title": m['title'], "overview": m['overview'], 
                        "rating": m['vote_average'],
                        "votes": m.get('vote_count', 0), # UPDATED: Included Vote Count
                        "cert": cert, "date": m.get('release_date', '0000-00-00'),
                        "poster": f"https://image.tmdb.org/t/p/w500{m['poster_path']}"
                    }
                    
                    # Vibes -> Lowercase
                    movie_vibes = {v.lower() for v in keywords} - JUNK_TAGS
                    movie_vibes.add(cert.lower())
                    
                    # Prevent 'christmas' tag flooding
                    # Only add the holiday name if it is NOT Christmas, or if needed for other holidays
                    if holiday_name.lower() != "christmas":
                        movie_vibes.add(holiday_name.lower())
                    
                    overview_lower = m['overview'].lower()
                    for pattern, vibe in VIBE_PATTERNS:
                        if vibe == "holiday horror" and not HORROR_GENRES.intersection(genres): continue
                        if pattern.search(overview_lower): movie_vibes.add(vibe.lower())
                    
                    for v in movie_vibes: inverted_index[v].append(mid)
                    for g in genres: genre_index[g].append(mid)
                        
                    holiday_count += 1
                    valid_in_batch += 1
                
                print(f"[{holiday_name}] Added {valid_in_batch}. Total: {len(final_ids)}", file=sys.stderr)
                if len(final_ids) >= TOTAL_MAX_LIMIT or len(candidates) < 20: break
                page += 1

        if not final_ids:
            sys.exit("\nBuild failed: no titles found. Keeping the existing holiday_engine.json.")
        
        output = {"index": inverted_index, "movies": movie_metadata, "genres": genre_index}
        with open(BASE_DIR / "holiday_engine.json", "w") as f:
            json.dump(output, f, indent=2)
        
        print(f"\nBuild Success: {len(final_ids)} titles saved.")
        if failed_lookups:
            print(f"Warning: {failed_lookups} movie lookups failed and were skipped.", file=sys.stderr)

if __name__ == "__main__":
    asyncio.run(main())