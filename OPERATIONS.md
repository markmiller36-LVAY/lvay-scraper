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

## Feed protection (added 2026-10-02)

- Browsers can only read the API from louisianavsallyall.com (and www,
  `*.wpcomstaging.com`, localhost). Add more with the `EXTRA_ALLOWED_ORIGINS`
  env var (comma-separated). WordPress PHP, Apps Script and the cron are
  server-to-server and are not affected.
- `/api/scrape/*`, `/api/build/*`, `/api/fix/*`, `/api/import/*` and
  `/api/recalculate/*` now require `PIPELINE_TOKEN`. To run one by hand in a
  browser, add `?key=YOUR_PIPELINE_TOKEN` to the URL (or send the
  `X-Pipeline-Token` header). Without a key they return 401.
- `/api/rankings/calculate` stays open because the Google Sheet "LVAY Tools"
  menu calls it without a key.
- Public read endpoints are limited to `PUBLIC_RATE_LIMIT_PER_MINUTE`
  requests per visitor IP (default 240). `/api/health`, `/api/status` and
  requests carrying the token are exempt. Set the env var to 0 to disable.
- All API responses send `X-Robots-Tag: noindex, nofollow`.

## Social posts (Facebook + Instagram)

`social_poster.py` builds score, standings and power-rating graphics (1080x1350
JPEG carousels, brand fonts in `assets/fonts/`) from the same feed the website
uses. The cron job calls `POST /api/social/run` at 8:30 AM Central, Aug–mid Dec:

- Sat: Big Games (both teams top 10 in their division) + Friday finals by class
- Sun: Power Ratings, top 10 per division
- Mon–Fri: district standings for 5A, 4A, 3A, 2A, 1A

Each post is saved to `/data/social/`, recorded in `social_posts`, and emailed
to `SOCIAL_APPROVER_EMAIL` (default lvaypipeline@gmail.com) with a review link.
Nothing is published until "Approve & post" is pressed on the review page.
Set `SOCIAL_AUTO_APPROVE=true` to publish without review.

Render environment for publishing: `META_PAGE_ID`, `META_PAGE_TOKEN`
(long-lived Page token with pages_manage_posts, pages_read_engagement,
instagram_basic, instagram_content_publish), `META_IG_USER_ID`, and optionally
`SOCIAL_PUBLIC_BASE`. Without them, approving records the OK but posts nothing.

Manual test: `POST /api/social/run?date=YYYY-MM-DD&kind=ratings` with the
pipeline token. Recent posts: `GET /api/social/posts` (token).
