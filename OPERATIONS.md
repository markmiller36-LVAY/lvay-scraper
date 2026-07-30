# LVAY Automation Operations

## Source of truth

- GitHub repository: `markmiller36-LVAY/lvay-scraper`
- Production branch: `main`
- Render web service: `lvay-scraper`
- Render cron job: `lvay-pipeline-scheduler`
- Persistent database: `/data/lvay_v2.db`
- Schedule: `0 */4 * * *` (every four hours)

Do not deploy from the duplicate local folders. Make production changes from a
clean branch based on the latest `origin/main`, review the diff, run the full
test suite, then merge through a pull request.

## What a successful run means

The cron job must:

1. Receive HTTP 202 (new run) or 409 (an existing run is still active).
2. Poll the protected pipeline status endpoint.
3. Exit successfully only after the pipeline reports `completed`.
4. Exit with failure when a scraper, calculation, or Sheets export fails.

The web service persists each run in `pipeline_runs`. `/api/status` exposes the
latest run plus recent per-sport scrape records. `/api/health` returns 200 only
when the service can read its persistent SQLite database.

## Routine checks

After a deployment:

1. Confirm the Render deployment uses `main`.
2. Open `/api/health` and verify `status: ok`.
3. Open `/api/status` and verify the expected record counts remain present.
4. Confirm the next cron run finishes successfully, not merely starts.
5. Spot-check one active sport in Google Sheets and on WordPress.

During a season, investigate:

- any failed cron run;
- an active sport with an `error` scrape status;
- a sudden zero or large drop in games;
- a pipeline that remains `running` beyond one hour;
- a Sheets export failure;
- an unexpected change in division, district, or classification coverage.

## Season controls

`scheduled_tasks.py` selects sports by month:

- Football and volleyball: August–November
- Basketball and soccer: October–March
- Baseball and softball: February–May

Football also requires `ENABLE_FOOTBALL=true`. Leave it false until the LHSAA
source is ready to replace preseason schedules.

Season-specific alignment and rules must remain keyed by season. Never replace
historical classifications globally when a new two-year LHSAA cycle begins.
Follow `SEASON_ROLLOVER.md` before enabling a new season.

## Data safety

- Final archives are protected by `season_registry.is_locked`.
- Manual correction tabs in Google Sheets are preserved by the exporters.
- Secrets belong in Render environment variables or secret files, never Git.
- Audit databases, PDFs, screenshots, and temporary JSON outputs stay local
  and are ignored by Git unless intentionally promoted to a production input.
- Historical source files and compressed archives tracked by Git are required
  for the website archive and disaster recovery.

## Recovery

If a deployment fails:

1. Stop making unrelated changes.
2. Identify the last known-good commit in Render.
3. Revert the failing pull request through GitHub.
4. Confirm `/api/health`.
5. Verify `/api/status` record counts before manually triggering a pipeline.

If the database must be rebuilt, deploy the current `main` schema first, restore
the persistent database backup if available, then import season schedules and
OOS data before recalculating rankings. Never unlock or overwrite a final
archive as part of routine recovery.

## Repository cleanup policy

Safe cleanup:

- delete remote branches only after Git confirms they are merged into `main`;
- remove generated audit artifacts from local working folders;
- archive redundant local clones after preserving all uncommitted files.

Do not delete:

- season archive files;
- alignment or official override files;
- regression tests;
- WordPress snippet source until the active snippet mapping is documented.
