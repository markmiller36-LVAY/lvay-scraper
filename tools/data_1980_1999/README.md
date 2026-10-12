# Football 1980–1999 source data (HSFHA-LA)

- `hsfha_la_football_1980_1999.tsv` — every Louisiana game 1980–1999 from https://hsfha.org/w/Special:LA/seasons,
  pulled Oct 11, 2026 (31,894 games, matching HSFHA's per-season totals). Same columns as tools/data_2000_2009.
- `hsfha_la_football_1980_1999_dedup.tsv` — after `tools/dedupe_hsfha.py`: HSFHA lists 505 of these games twice
  (both copies flagged "attention" on HSFHA, 368 with different scores). The lower game id is kept and the other
  score is written into `notes`. 31,389 games.
- Classes before 1991 are letters on HSFHA (A–AAAA → 1A–4A). Named bowl games (Shrimp Bowl, Jaycee Bowl…) are
  treated as regular-season games, not playoffs.
- Site shows 1990 on (Mark, Oct 11): `python3 tools/build_football_from_hsfha.py tools/data_1980_1999/hsfha_la_football_1980_1999_dedup.tsv tools/data_2000_2009/site_names_2010_2024.json write 1990-1999`
- All 1980–1999 rows (one per team per game) are in Airtable bases "LVAY Football History 1980–1989"
  (appK9XPkS4cXuZhxP) and "LVAY Football History 1990–1999" (appoTTijFOE4P4AUA).
