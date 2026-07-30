# Louisiana vs. All Y'all Sports Automation

This repository is the production source for LVAY schedules, scores, power
ratings, Google Sheets review exports, and the JSON APIs consumed by
WordPress.

## Production path

```text
Render cron (every 4 hours)
  -> POST /api/pipeline/run
  -> scrape active LHSAA sports
  -> calculate power ratings
  -> export review data to Google Sheets
  -> serve schedules/rankings to WordPress through the API
```

Render deploys the `main` branch. Football scraping is intentionally controlled
by `ENABLE_FOOTBALL`; the 2026 preseason schedule remains available while the
official LHSAA source is incomplete.

## Repository map

- `render.yaml` — production web service, disk, and four-hour cron definition.
- `trigger_pipeline.py` — secure cron trigger and completion monitor.
- `scheduled_tasks.py` — season-aware pipeline orchestration.
- `scraper.py`, `scraper_volleyball.py` — LHSAA schedule/result ingestion.
- `run_power_rankings*.py`, `power_rating_engine.py` — rating calculations.
- `sheets_exporter.py` — Google Sheets review and correction workflow.
- `server.py` — API, health/status endpoints, season archives, and triggers.
- `school_database.py`, `winter_alignment_2026.json` — school metadata.
- `wordpress-*.php` — source copies of active or historical WordPress snippets.
- `test_*.py` — regression checks for official rules and production incidents.

Historical JSON and compressed archive files are production inputs. Do not
delete them as cleanup.

See [OPERATIONS.md](OPERATIONS.md) for deployment, monitoring, recovery, and
season rollover procedures.
