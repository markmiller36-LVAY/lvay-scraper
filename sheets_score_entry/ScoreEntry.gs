/**
 * LVAY — Enter Week Scores
 *
 * A side panel for typing in football scores LHSAA has not posted yet.
 * Each saved game adds one row per in-state team to "Football Overrides".
 * The pipeline applies those rows on its next run (every 30 minutes on
 * Thursday/Friday game nights) and keeps them for the rest of the season.
 * Once LHSAA posts its own result for a game, LHSAA's result is used, and any
 * difference from the score typed here is printed in the run log for review.
 */

const SCORE_ENTRY_SEASON = "2026";
const SCORE_ENTRY_SCORES_TAB = "Football Scores (" + SCORE_ENTRY_SEASON + ")";
const SCORE_ENTRY_OVERRIDES_TAB = "Football Overrides (" + SCORE_ENTRY_SEASON + ")";
const SCORE_ENTRY_TAG = "[LVAY form]";

/** Menu item: opens the panel. */
function showScoreEntry() {
  const html = HtmlService.createHtmlOutputFromFile("ScoreEntry")
    .setTitle("Enter Week Scores");
  SpreadsheetApp.getUi().showSidebar(html);
}

function scoreEntryHeaderMap_(headers) {
  const map = {};
  headers.forEach(function (name, i) {
    map[String(name).trim().toLowerCase()] = i;
  });
  return map;
}

function scoreEntryWeekNumber_(value) {
  const n = parseInt(String(value).replace(/[^0-9]/g, ""), 10);
  return isNaN(n) ? null : n;
}

function scoreEntryDateOnly_(text) {
  return String(text).trim().split(" ")[0];
}

/** Games already entered through this panel and still active. */
function scoreEntryEnteredKeys_() {
  const sheet = SpreadsheetApp.getActive().getSheetByName(SCORE_ENTRY_OVERRIDES_TAB);
  const keys = {};
  if (!sheet || sheet.getLastRow() < 2) return keys;
  const values = sheet.getDataRange().getDisplayValues();
  const col = scoreEntryHeaderMap_(values[0]);
  values.slice(1).forEach(function (row) {
    const active = String(row[col["active"]]).trim().toLowerCase();
    if (active !== "true") return;
    if (String(row[col["notes"]]).indexOf(SCORE_ENTRY_TAG) !== 0) return;
    keys[[
      String(row[col["school"]]).trim().toLowerCase(),
      String(row[col["game_date"]]).trim(),
      String(row[col["opponent"]]).trim().toLowerCase(),
    ].join("|")] = true;
  });
  return keys;
}

/** Reads every game from the Scores tab, grouped into one entry per game. */
function scoreEntryAllGames_() {
  const sheet = SpreadsheetApp.getActive().getSheetByName(SCORE_ENTRY_SCORES_TAB);
  if (!sheet) {
    throw new Error("Can't find the '" + SCORE_ENTRY_SCORES_TAB + "' tab.");
  }
  const values = sheet.getDataRange().getDisplayValues();
  const col = scoreEntryHeaderMap_(values[0]);
  ["school", "week", "date", "h/a", "opponent", "w/l"].forEach(function (name) {
    if (col[name] === undefined) {
      throw new Error("The Scores tab is missing its '" + name + "' column.");
    }
  });

  const games = {};
  values.slice(1).forEach(function (row) {
    const school = String(row[col["school"]]).trim();
    const opponent = String(row[col["opponent"]]).trim();
    const date = String(row[col["date"]]).trim();
    const week = scoreEntryWeekNumber_(row[col["week"]]);
    if (!school || !opponent || !date || !week) return;

    const pair = [school.toLowerCase(), opponent.toLowerCase()].sort();
    const id = pair.join("|") + "|" + scoreEntryDateOnly_(date);
    const game = games[id] || (games[id] = {
      id: id, week: week, date: date, sides: {}, scored: false,
    });
    game.sides[school] = {
      school: school,
      opponent: opponent,
      gameDate: date,
      homeAway: String(row[col["h/a"]]).trim().toUpperCase(),
    };
    if (String(row[col["w/l"]]).trim()) game.scored = true;
  });

  return Object.keys(games).map(function (id) {
    const game = games[id];
    const names = Object.keys(game.sides);
    const first = game.sides[names[0]];
    let home = first.homeAway === "A" ? first.opponent : first.school;
    let away = first.homeAway === "A" ? first.school : first.opponent;
    if (first.homeAway !== "H" && first.homeAway !== "A") {
      home = first.school;
      away = first.opponent;
    }
    game.home = home;
    game.away = away;
    game.label = away + " at " + home + "  (" + scoreEntryDateOnly_(game.date) + ")";
    return game;
  });
}

/** Panel: list of weeks, plus the week to show first. */
function scoreEntryWeeks() {
  const games = scoreEntryAllGames_();
  const weeks = {};
  let suggested = null;
  const today = new Date();
  games.forEach(function (game) {
    weeks[game.week] = true;
    const played = new Date(scoreEntryDateOnly_(game.date)) <= today;
    if (!game.scored && played && (suggested === null || game.week > suggested)) {
      suggested = game.week;
    }
  });
  const list = Object.keys(weeks).map(Number).sort(function (a, b) { return a - b; });
  return { weeks: list, suggested: suggested || list[0] || null };
}

/** Panel: the week's games that still need a score. */
function scoreEntryGames(week) {
  const entered = scoreEntryEnteredKeys_();
  return scoreEntryAllGames_()
    .filter(function (game) {
      if (game.week !== Number(week) || game.scored) return false;
      return !Object.keys(game.sides).some(function (name) {
        const side = game.sides[name];
        return entered[[
          side.school.toLowerCase(), side.gameDate, side.opponent.toLowerCase(),
        ].join("|")];
      });
    })
    .sort(function (a, b) { return a.label < b.label ? -1 : 1; })
    .map(function (game) {
      return { id: game.id, label: game.label, home: game.home, away: game.away };
    });
}

/**
 * Panel: saves the scores.
 * entries: [{ id: "...", homeScore: 21, awayScore: 14 }, ...]
 */
function scoreEntrySave(week, entries) {
  if (!entries || !entries.length) throw new Error("Add at least one game.");
  const lock = LockService.getDocumentLock();
  lock.waitLock(20000);
  try {
    const byId = {};
    scoreEntryAllGames_().forEach(function (game) { byId[game.id] = game; });
    const sheet = SpreadsheetApp.getActive().getSheetByName(SCORE_ENTRY_OVERRIDES_TAB);
    if (!sheet) throw new Error("Can't find the '" + SCORE_ENTRY_OVERRIDES_TAB + "' tab.");
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getDisplayValues()[0];
    const col = scoreEntryHeaderMap_(headers);
    const stamp = Utilities.formatDate(new Date(), "America/Chicago", "M/d/yyyy h:mm a");

    const rows = [];
    const saved = [];
    entries.forEach(function (entry) {
      const game = byId[entry.id];
      if (!game) throw new Error("A game in the list could not be found. Reopen the panel and try again.");
      const homeScore = parseInt(entry.homeScore, 10);
      const awayScore = parseInt(entry.awayScore, 10);
      if (isNaN(homeScore) || isNaN(awayScore) || homeScore < 0 || awayScore < 0) {
        throw new Error("Enter both scores for " + game.label + ".");
      }
      Object.keys(game.sides).forEach(function (name) {
        const side = game.sides[name];
        const isHome = side.school === game.home;
        const own = isHome ? homeScore : awayScore;
        const other = isHome ? awayScore : homeScore;
        const result = own > other ? "W" : (own < other ? "L" : "T");
        const row = new Array(headers.length).fill("");
        row[col["sport"]] = "football";
        row[col["season"]] = SCORE_ENTRY_SEASON;
        row[col["school"]] = side.school;
        row[col["game_date"]] = side.gameDate;
        row[col["opponent"]] = side.opponent;
        row[col["active"]] = true;
        row[col["override_win_loss"]] = result;
        row[col["override_score"]] = own + "-" + other;
        row[col["override_home_away"]] = side.homeAway;
        row[col["notes"]] = SCORE_ENTRY_TAG + " Week " + game.week + ", entered " + stamp;
        rows.push(row);
      });
      saved.push(game.away + " " + awayScore + ", " + game.home + " " + homeScore);
    });

    const start = sheet.getLastRow() + 1;
    const range = sheet.getRange(start, 1, rows.length, headers.length);
    // Store dates and scores as plain text so they match LHSAA's exactly.
    sheet.getRange(start, col["game_date"] + 1, rows.length, 1).setNumberFormat("@");
    sheet.getRange(start, col["override_score"] + 1, rows.length, 1).setNumberFormat("@");
    sheet.getRange(start, col["season"] + 1, rows.length, 1).setNumberFormat("@");
    range.setValues(rows);
    sheet.getRange(start, col["active"] + 1, rows.length, 1).insertCheckboxes();
    return saved;
  } finally {
    lock.releaseLock();
  }
}
