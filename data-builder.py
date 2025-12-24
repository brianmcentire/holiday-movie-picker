import asyncio
import aiohttp
import json
import sys
import random
from collections import defaultdict

# --- CONFIGURATION ---
API_KEY = "YOUR_TMDB_API_KEY"
CHRISTMAS_LIMIT = 100  
TOTAL_MAX_LIMIT = 250
CONCURRENT_REQUESTS = 10 
MIN_VOTES = 10
MAX_RETRIES = 5
MAX_PAGES_PER_HOLIDAY = 20 # New constant to limit polling depth

HOLIDAY_QUERIES = {
    "Hanukkah": "7328|210405|335345",
    "Kwanzaa": "335346",
    "Lunar New Year": "236402",
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
    "supernatural": "supernatural", "ghost": "supernatural", "spirit": "supernatural", "magic": "magic",
    
    # --- THE DARK SIDE ---
    "killer": "holiday horror", "serial killer": "holiday horror", "slasher": "holiday horror",
    "murder": "holiday horror", "blood": "holiday horror", "dark": "holiday horror",
    "sinister": "holiday horror", "terror": "holiday horror", "scary": "holiday horror",
    
    # --- TROPES & THEMES ---
    "saving christmas": "saving christmas", "save christmas": "saving christmas",
    "spirit of christmas": "christmas spirit", "party": "christmas party",
    "celebration": "christmas party", "office party": "christmas party",
    "vacation": "vacation", "trip": "vacation", "travel": "vacation",
    "remake": "remake", "based on": "literary", "novel": "literary", "book": "literary",
    "sequel": "sequel", "reindeer": "reindeer", "snowman": "snowman", "frosty": "snowman",
    "elves": "elves", "elf": "elves", "workshop": "elves",
    
    # --- TIME & EVENTS ---
    "new year": "new year's eve", "thanksgiving": "thanksgiving", "halloween": "holiday overlap",
    "valentine": "holiday overlap", "winter": "winter", "snow": "winter", "icy": "winter",
    "19th century": "vintage", "victorian": "vintage", "black and white": "vintage",
    "classic": "vintage", "retro": "vintage", "old-fashioned": "vintage"
}

async def fetch_with_retry(session, url, params, semaphore):
    for attempt in range(MAX_RETRIES):
        async with semaphore:
            try:
                async with session.get(url, params=params, timeout=10) as resp:
                    if resp.status == 200: return await resp.json()
                    if resp.status == 429: await asyncio.sleep(2 + (2 ** attempt))
                    else: return None
            except: await asyncio.sleep(2 ** attempt)
    return None

async def fetch_movie_details(session, semaphore, movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {"api_key": API_KEY, "append_to_response": "keywords,release_dates"}
    data = await fetch_with_retry(session, url, params, semaphore)
    if not data: return [], "NR", []
    
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
    
    async with aiohttp.ClientSession() as session:
        print(f"Set Flicks: Starting Movie Data Source Build...", file=sys.stderr)
        
        for holiday_name, keyword_id in HOLIDAY_QUERIES.items():
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
                    "api_key": API_KEY, "with_keywords": keyword_id,
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
                for i, (keywords, cert, genres) in enumerate(details):
                    m, mid = candidates[i], str(candidates[i]['id'])
                    
                    # Quality Gate
                    allowed_certs = ['G', 'PG', 'PG-13', 'TV-G', 'TV-PG', 'TV-Y7']
                    if mid in movie_metadata or cert not in allowed_certs or m.get('vote_count', 0) < MIN_VOTES: 
                        continue
                    
                    # For events like New Year's/Thanksgiving, require a "Holiday" or "Christmas" tag
                    # to ensure the movie is likely actually about the Holiday Season
                    if holiday_name in ["New Year's", "Thanksgiving"]:
                        anchor_tags = {"holiday", "holidays", "christmas"}
                        if not any(k.lower() in anchor_tags for k in keywords): 
                            continue

                    if holiday_name == "Christmas" and holiday_count >= CHRISTMAS_LIMIT: continue
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
                    movie_vibes = {v.lower() for v in keywords}
                    movie_vibes.add(cert.lower())
                    
                    # Prevent 'christmas' tag flooding
                    # Only add the holiday name if it is NOT Christmas, or if needed for other holidays
                    if holiday_name.lower() != "christmas":
                        movie_vibes.add(holiday_name.lower())
                    
                    overview_lower = m['overview'].lower()
                    for trigger, vibe in VIBE_TRIGGERS.items():
                        if trigger in overview_lower: movie_vibes.add(vibe.lower())
                    
                    for v in movie_vibes: inverted_index[v].append(mid)
                    for g in genres: genre_index[g].append(mid)
                        
                    holiday_count += 1
                    valid_in_batch += 1
                
                print(f"[{holiday_name}] Added {valid_in_batch}. Total: {len(final_ids)}", file=sys.stderr)
                if len(final_ids) >= TOTAL_MAX_LIMIT or len(candidates) < 20: break
                page += 1

        output = {"index": inverted_index, "movies": movie_metadata, "genres": genre_index}
        with open("holiday_engine.json", "w") as f:
            json.dump(output, f, indent=2)
        
        print(f"\nBuild Success: {len(final_ids)} titles saved.")

if __name__ == "__main__":
    asyncio.run(main())