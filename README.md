# Watchman

The world right now, sourced and dated. A phone-first reading room: world news, the U.S., Indiana and Marion County,
what Washington did, and the numbers that matter, every item with its source and date.

- `site/` the app (one page) and its data (`site/data/db.json`, photos in `site/img/`), served by GitHub Pages.
- `engine/` the Python engine that reads public sources. `.github/workflows/engine.yml` runs it every hour.
- `tools/export.py` writes each run into the site.

API keys live only in this repository's encrypted Actions secrets.
