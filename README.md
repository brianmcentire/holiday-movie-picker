# holiday-movie-picker
Holiday movie selector single page application. Created for a Python and JavaScript learning experience.

Live demo here - https://brianmcentire.github.io/holiday-movie-picker/

# Set Flicks: Holiday Edition 🎬

A curated holiday movie discovery engine built with a high-concurrency Python API client and a zero-dependency static HTML and JS frontend.

### Architecture
* **Builder:** `data-builder.py` uses `asyncio` and `aiohttp` to fetch and normalize data from TMDB.
* **Frontend:** `index.html` is a standalone static site that filters the generated `holiday_engine.json` in real-time.

### Usage
1.  Run the builder: `python3 data-builder.py`
2.  Open `index.html` in any web browser.

*Data provided by [The Movie Database (TMDB)](https://www.themoviedb.org).*
