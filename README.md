# Season's Screenings 🎬
*A holiday movie matchmaker.* Created for a Python and JavaScript learning experience.

Live demo here - https://brianmcentire.github.io/holiday-movie-picker/

A curated holiday movie discovery engine built with a high-concurrency Python API client and a zero-dependency static HTML and JS frontend.

### Architecture
* **Builder:** `data-builder.py` uses `asyncio` and `aiohttp` to fetch and normalize data from TMDB, including the actors, directors and writers that search can find each film by.
* **Holiday levels:** `holiday_classifier.py` asks Claude how much the holiday matters to each film (see below).
* **Frontend:** `index.html` is a standalone static site that filters the generated `holiday_engine.json` in real-time.
* **Musical pill:** `musical_classifier.py` asks Claude which films are musicals or music shows (see below).
* **Watch list:** `watchlist.js` adds a personal list (bookmark movies, mark them watched, reorder, share a copy), stored on the device with no accounts or servers. It's a separate module: removing it means deleting the file, its `<script>` tag and the lines in `index.html` marked `watch list hook`.
* **Usage stats:** `analytics.js` counts visits and shared links with GoatCounter; it's also separate and removable.
* **Outside scores (personal):** `scores-builder.py` fetches IMDb and Rotten Tomatoes scores from MDBList into `scores.json`, for the owner's Tampermonkey script in `userscripts/`. The site itself never loads it (see below).
* **Design decisions** and the reasons for them are in [DESIGN.md](DESIGN.md).

### Usage
1.  Get a free API Read Access Token from your [TMDB API settings](https://www.themoviedb.org/settings/api).
2.  Put it in a `.env` file in this folder: `TMDB_API_KEY=your_token`. For holiday levels, also add an Anthropic API key: `ANTHROPIC_API_KEY=your_key`
3.  Install the dependencies: `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`
4.  Run the builder: `.venv/bin/python data-builder.py`
5.  Run `make dev`, then open http://localhost:8000. The page reloads by itself when the page, its scripts or the data change. `make dev-stop` stops the server.

Opening `index.html` directly won't work, because browsers block it from loading the data file.

### Holiday levels
Each film gets a level: **holiday story** (the holiday is the story, like *Elf*), **holiday setting** (set at the holidays, and the season shapes it, like *The Apartment*) or **holiday adjacent** (the holidays are just the backdrop, like *Die Hard*). Films with no real holiday connection are left out.

Claude Opus 5.5 classifies each film and says how sure it is, and Claude Haiku 5.5 gives an independent second opinion. When they disagree, or Opus isn't sure, Opus referees at higher effort. Results are saved in `holiday_levels.json`, so each film is classified once; new films cost a fraction of a cent each.

`holiday_levels_review.md` lists the calls worth a human look. To overrule one, add it to `OVERRIDES` in `holiday_classifier.py`. After changing the classifier's instructions, bump `PROMPT_VERSION` to classify every film again (about $10).

### Musical pill
The Genres pill **Musical** holds musicals and music shows: concerts, variety specials and documentaries about musicians. Claude Opus 5.5 and Claude Sonnet 5.5 each say what kind of film it is, and Claude Fable 5.1 makes the call when they disagree or aren't sure. Answers are saved in `musicals.json`, so each film is asked once, and Fable's calls are listed in `musicals_review.md`.

* **New films:** the Monday refresh labels them, Fable only for the ones Opus and Sonnet disagree on or aren't sure about. That's about 1.5¢ a film, plus about 5¢ for each one Fable decides. `make musicals` does the same by hand.
* **If labeling fails** (no credits, API trouble), the new movie data still publishes, with the new films outside the pill. The run is then marked failed, so GitHub emails you. A run that would need more than 50 films labeled stops instead; do those with `--batch` by hand.
* **The whole catalog** (done once on Nov 11): `python musical_classifier.py ask --batch` uses the Batch API at half price; it can take up to a day, usually under an hour. It then says how many films Fable would decide and what that would cost, and `python musical_classifier.py decide` runs Fable.
* To overrule a call, add the film's TMDB id to `OVERRIDES` in `musical_classifier.py`, then run `python musical_classifier.py apply`.

### Automatic refresh
A GitHub Action (`.github/workflows/rebuild-data.yml`) rebuilds `holiday_engine.json` every Monday in November and December. If the data changed, it commits the new file, then rebuilds the live site. You can also start it from the Actions tab.

One-time setup: run `gh secret set TMDB_API_KEY` and `gh secret set ANTHROPIC_API_KEY` in this folder, pasting each key when prompted.

If the classifier can't run (missing key, no credits, API trouble), the new movie data still publishes, with the new films unsorted. The same goes for the Musical pill's labels (see above). The run is then marked failed, so GitHub emails you, and the page's footer shows a quiet note until it's fixed. The footer also notes when the movie list hasn't refreshed in over 10 days during November or December.

GitHub pauses scheduled workflows in public repos after 60 days without activity, so each fall, before November, turn it back on with `gh workflow enable rebuild-data.yml`.

### Outside scores (personal)
`scores.json` holds each film's IMDb rating, Tomatometer and Popcornmeter, plus its IMDb id and Rotten Tomatoes address, from [MDBList](https://mdblist.com). Only the Tampermonkey script in `userscripts/scores.user.js` reads it; the page doesn't load it.

* Needs a free MDBList API key (mdblist.com, Preferences, API) in `.env` as `MDBLIST_API_KEY=your_key`. Run `make scores` to refresh it by hand.
* The Monday refresh runs it too, after the movie data, with the key from `gh secret set MDBLIST_API_KEY`. A problem with MDBList never stops the movie data from publishing.
* A whole refresh takes about 23 requests (50 films each), paced 1.5 seconds apart, out of the free 1,000 a day.
* If MDBList is down, refuses the key or leaves a score out, the scores from the last good run stay.
* **Installing the script:** with Tampermonkey in Chrome, open https://raw.githubusercontent.com/brianmcentire/holiday-movie-picker/main/userscripts/scores.user.js and click Install. Tampermonkey then updates it by itself whenever a pushed version has a higher `@version` (it checks daily; Tampermonkey's menu has "Check for userscript updates" to check now).

### Future ideas
* **Pick for me:** a button that spotlights one film from the current filters.
* **Falling snow:** a soothing, gentle snowfall in the page background.
* **Released filter:** a group like Content Ratings with plain decade buttons (Before 1970 · '70s & '80s · '90s & 2000s · 2010 & later), all on by default, so people who skip older movies can turn them off. Factual labels on purpose: names like "Retro" make a judgment, and whether a film feels dated depends on the film and the viewer (A Christmas Story and Die Hard still play for kids who can't stand Christmas Vacation).
* **Ageless exceptions:** a short hand-picked list of older films that play for every generation (the Charlie Brown, Rudolph, Grinch and Frosty specials, maybe It's a Wonderful Life) that stays visible even when Before 1970 is off.

*Data provided by [The Movie Database (TMDB)](https://www.themoviedb.org).*
