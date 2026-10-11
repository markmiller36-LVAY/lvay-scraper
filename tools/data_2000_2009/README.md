# Football 2000–2009 source data (HSFHA-LA)

- `hsfha_la_football_2000_2009.tsv` — every Louisiana game 2000–2009 from the High School Football
  Historians Association, Louisiana section (https://hsfha.org/w/Special:LA/seasons), pulled Oct 11, 2026
  from the site's public JSON (`api.php?section=LA&model=seasons&method=games_by_date&season_code=YYYY&game_week=…`).
  One row per game (16,786). Columns: id, season, date, gametype, round, bracket, played, team/opp name, school
  code (LA.xx-nnn), site (h/a/n, from `team`'s side), scores, decisions (W/L/T, X = adjusted), association,
  class, district, coach, venue, city, overtime, notes.
- `site_names_2010_2024.json` — school names already in the archive, used to match HSFHA names.
- Build: `python3 tools/build_2000_2009_from_hsfha.py tools/data_2000_2009/hsfha_la_football_2000_2009.tsv tools/data_2000_2009/site_names_2010_2024.json write`
- Same games, one row per team, live in Airtable base "LVAY Football History 2000–2009" (app0mNACSri2Kchhz).
- Rules: canceled/unplayed games dropped (56); forfeits kept with no score (`forfeit: "FF"`); 2005 4A final
  (Bastrop 41–12 Breaux Bridge, title later vacated) keeps the on-field result; non-LHSAA Louisiana schools
  get a season row only if they already have an LVAY page (blank class/district), otherwise they are opponents only.
