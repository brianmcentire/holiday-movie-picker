# Season's Screenings 🎬
*A holiday movie matchmaker.* Created for a Python and JavaScript learning experience.

Live demo here - https://brianmcentire.github.io/holiday-movie-picker/

A curated holiday movie discovery engine built with a high-concurrency Python API client and a zero-dependency static HTML and JS frontend.

### Architecture
* **Builder:** `data-builder.py` uses `asyncio` and `aiohttp` to fetch and normalize data from TMDB.
* **Holiday levels:** `holiday_classifier.py` asks Claude how much the holiday matters to each film (see below).
* **Frontend:** `index.html` is a standalone static site that filters the generated `holiday_engine.json` in real-time.

### Usage
1.  Get a free API Read Access Token from your [TMDB API settings](https://www.themoviedb.org/settings/api).
2.  Put it in a `.env` file in this folder: `TMDB_API_KEY=your_token`. For holiday levels, also add an Anthropic API key: `ANTHROPIC_API_KEY=your_key`
3.  Install the dependencies: `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`
4.  Run the builder: `.venv/bin/python data-builder.py`
5.  Run `make dev`, then open http://localhost:8000. The page reloads by itself when `index.html` or the data changes. `make dev-stop` stops the server.

Opening `index.html` directly won't work, because browsers block it from loading the data file.

### Holiday levels
Each film gets a level: **holiday story** (the holiday is the story, like *Elf*), **holiday setting** (set at the holidays, and the season shapes it, like *The Apartment*) or **holiday adjacent** (the holidays are just the backdrop, like *Die Hard*). Films with no real holiday connection are left out.

Claude Opus 5.5 classifies each film and says how sure it is, and Claude Haiku 5.5 gives an independent second opinion. When they disagree, or Opus isn't sure, Opus referees at higher effort. Results are saved in `holiday_levels.json`, so each film is classified once; new films cost a fraction of a cent each.

`holiday_levels_review.md` lists the calls worth a human look. To overrule one, add it to `OVERRIDES` in `holiday_classifier.py`. After changing the classifier's instructions, bump `PROMPT_VERSION` to classify every film again (about $10).

### Automatic refresh
A GitHub Action (`.github/workflows/rebuild-data.yml`) rebuilds `holiday_engine.json` every Monday in November and December. If the data changed, it commits the new file, then rebuilds the live site. You can also start it from the Actions tab.

One-time setup: run `gh secret set TMDB_API_KEY` and `gh secret set ANTHROPIC_API_KEY` in this folder, pasting each key when prompted.

If the classifier can't run (missing key, no credits, API trouble), the new movie data still publishes, with the new films unsorted. The run is then marked failed, so GitHub emails you, and the page's footer shows a quiet note until it's fixed. The footer also notes when the movie list hasn't refreshed in over 10 days during November or December.

GitHub pauses scheduled workflows in public repos after 60 days without activity, so each fall, before November, turn it back on with `gh workflow enable rebuild-data.yml`.

### Future ideas
* **Pick for me:** a button that spotlights one film from the current filters.
* **Falling snow:** a soothing, gentle snowfall in the page background.

*Data provided by [The Movie Database (TMDB)](https://www.themoviedb.org).*
