# Season's Screenings 🎬
*A holiday movie matchmaker.* Created for a Python and JavaScript learning experience.

Live demo here - https://brianmcentire.github.io/holiday-movie-picker/

A curated holiday movie discovery engine built with a high-concurrency Python API client and a zero-dependency static HTML and JS frontend.

### Architecture
* **Builder:** `data-builder.py` uses `asyncio` and `aiohttp` to fetch and normalize data from TMDB.
* **Frontend:** `index.html` is a standalone static site that filters the generated `holiday_engine.json` in real-time.

### Usage
1.  Get a free API Read Access Token from your [TMDB API settings](https://www.themoviedb.org/settings/api).
2.  Put it in a `.env` file in this folder: `TMDB_API_KEY=your_token`
3.  Install the dependencies: `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`
4.  Run the builder: `.venv/bin/python data-builder.py`
5.  Start a local server with `python3 -m http.server`, then open http://localhost:8000.

Opening `index.html` directly won't work, because browsers block it from loading the data file.

### Automatic refresh
A GitHub Action (`.github/workflows/rebuild-data.yml`) rebuilds `holiday_engine.json` every Monday in November and December. If the data changed, it commits the new file, then rebuilds the live site. You can also start it from the Actions tab.

One-time setup: run `gh secret set TMDB_API_KEY` in this folder and paste your token when prompted.

GitHub pauses scheduled workflows in public repos after 60 days without activity, so each fall, before November, turn it back on with `gh workflow enable rebuild-data.yml`.

### Future ideas
* **Pick for me:** a button that spotlights one film from the current filters.
* **Falling snow:** a soothing, gentle snowfall in the page background.

*Data provided by [The Movie Database (TMDB)](https://www.themoviedb.org).*
