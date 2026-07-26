# LVAY Season Rollover

This is the reusable workflow for every LVAY sport.

## Season states

- `preseason`: schedules may be imported from a secondary source; scores and
  ratings are blank.
- `active`: the official scraper is authoritative and results may update.
- `final`: the season is complete.
- `archived`: the final data is permanently preserved.

Completed seasons must have `season_registry.is_locked=1`. The scraper refuses
to write to a locked sport/season.

## Rollover checklist

1. Validate the final schedules, scores, ratings and brackets.
2. Set the completed season to `final`/locked in `season_registry`.
3. Export an off-database snapshot for disaster recovery.
4. Create the new `season_registry` row as `preseason`/unlocked.
5. Import available preseason schedules into `season_schools` and `games`.
6. Keep the official scraper disabled until its source is sufficiently
   complete.
7. Reconcile the preseason source against the official source.
8. Enable the scraper and change the new season to `active`.
9. Preserve all classification, district and division assignments by season.

## Football 2025 → 2026

- Football 2025 is registered as `final` and locked.
- `football_2026_preseason.json` contains the normalized preseason source.
- `ENABLE_FOOTBALL=false` pauses both scraping and rating/export processing.
- When disabled, application startup imports the preseason JSON only into
  Football 2026.
- `/api/schedules/football?season=2025` serves the archive.
- `/api/schedules/football?season=2026` serves the preseason schedule.
- `/api/seasons/football` supplies the archive navigation.

## Activating official Football 2026

1. Compare the official LHSAA schedules against the preseason import.
2. Resolve missing opponents, dates, home/away conflicts and OOS records.
3. Change `ENABLE_FOOTBALL` to `true`.
4. Change Football 2026 from `preseason` to `active`.
5. Deploy and run one supervised pipeline.
6. Validate the website and Google Sheets before resuming the four-hour job.

