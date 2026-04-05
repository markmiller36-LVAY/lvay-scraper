"""
LVAY - Multi-Sport Power Rating Engine
========================================
Official LHSAA formulas from 2025-2026 Handbook:

FOOTBALL (14.12):
  Win=10, Loss=0, Tie=5, Forfeit=1
  Div bonus: +2 per division higher
  OppQ: (Opp Wins / Opp GP) x 10

BASEBALL (10.10):
  Win=20, Loss=0, Tie=5, Double Forfeit=+1 to winner
  Div bonus: +2 per class & division higher
  OppQ: Opponent's Wins (raw — no multiplier)
  Power Rating = Total Points / Games Played

SOFTBALL (19.7.3):
  Win=20, Loss=0, Tie=5, Double Forfeit=+1 to winner
  Div bonus: +2 per class & division higher
  OppQ: Opponent's Wins (raw)
  Power Rating = Total Points / Games Played

BASKETBALL 1A-5A (11.6):
  Win=25, Loss=0
  Div bonus: +2 per class & division higher
  Play-up bonus: +2
  OppQ: (Opp Wins / Opp GP) x 34
  Power Rating = Total Points / Games Played

BASKETBALL Class B&C (11.6):
  Win=8, Loss=0
  Div bonus: +2 per class & division higher
  OppQ: (Opp Wins / Opp GP) x 44
  Power Rating = Total Points / Games Played

SOCCER (18.5.5):
  Win=5 + Opp Wins (100%)
  Loss=0 + Opp Wins (50%)
  Tie=2.5 + Opp Wins (75%)
  NO division bonus
  Power Rating = Total Points / Games Played
"""

from dataclasses import dataclass, field
from typing import Optional

# ─── DIVISION HIERARCHY ───────────────────────────────────────────────────────
# Unified I > II > III > IV regardless of Select/Non-Select track

DIVISION_RANK = {
    "Division I":              4,
    "Division II":             3,
    "Division III":            2,
    "Division IV":             1,
    "Non-Select Division I":   4,
    "Non-Select Division II":  3,
    "Non-Select Division III": 2,
    "Non-Select Division IV":  1,
    "Select Division I":       4,
    "Select Division II":      3,
    "Select Division III":     2,
    "Select Division IV":      1,
    "Division I (Play Up)":    4,
}

CLASS_RANK = {
    "5A": 5, "4A": 4, "3A": 3, "2A": 2, "1A": 1,
    "B": 1, "C": 0,
}

PLAYOFF_SIZES = {
    "non-select": 28,
    "select":     24,
    "soccer":     24,
}

# ─── SPORT CONFIGS ────────────────────────────────────────────────────────────

SPORT_CONFIGS = {
    "football": {
        "win_points":        10,
        "loss_points":       0,
        "tie_points":        5,
        "forfeit_bonus":     1,
        "div_bonus_per_div": 2,
        "has_div_bonus":     True,
        "opp_quality":       "win_pct_x10",
    },
    "baseball": {
        "win_points":        20,
        "loss_points":       0,
        "tie_points":        5,
        "forfeit_bonus":     1,
        "div_bonus_per_div": 2,
        "has_div_bonus":     True,
        "opp_quality":       "raw_wins",
    },
    "softball": {
        "win_points":        20,
        "loss_points":       0,
        "tie_points":        5,
        "forfeit_bonus":     1,
        "div_bonus_per_div": 2,
        "has_div_bonus":     True,
        "opp_quality":       "raw_wins",
    },
    "basketball_1a5a": {
        "win_points":        25,
        "loss_points":       0,
        "tie_points":        0,
        "forfeit_bonus":     0,
        "div_bonus_per_div": 2,
        "play_up_bonus":     2,
        "has_div_bonus":     True,
        "opp_quality":       "win_pct_x34",
    },
    "basketball_bc": {
        "win_points":        8,
        "loss_points":       0,
        "tie_points":        0,
        "forfeit_bonus":     0,
        "div_bonus_per_div": 2,
        "has_div_bonus":     True,
        "opp_quality":       "win_pct_x44",
    },
    "soccer": {
        "win_points":        5,
        "loss_points":       0,
        "tie_points":        2.5,
        "forfeit_bonus":     0,
        "div_bonus_per_div": 0,
        "has_div_bonus":     False,
        "opp_quality":       "soccer_weighted",
    },
}


def get_sport_config(sport: str, classification: str = "") -> dict:
    """Return the correct config for a sport, handling basketball split."""
    if sport == "basketball":
        if classification.upper() in ("B", "C"):
            return SPORT_CONFIGS["basketball_bc"]
        return SPORT_CONFIGS["basketball_1a5a"]
    return SPORT_CONFIGS.get(sport, SPORT_CONFIGS["football"])


# ─── DATA CLASSES ─────────────────────────────────────────────────────────────

@dataclass
class GameResult:
    team:                  str
    opponent:              str
    result:                str       # W, L, T, DF
    sport:                 str
    opponent_wins:         int  = 0
    opponent_losses:       int  = 0
    opponent_division:     str  = ""
    opponent_class:        str  = ""
    opponent_out_of_state: bool = False
    playing_up:            bool = False
    week:                  int  = 0

    @property
    def opponent_gp(self) -> int:
        return self.opponent_wins + self.opponent_losses

    @property
    def opponent_win_pct(self) -> float:
        return self.opponent_wins / self.opponent_gp if self.opponent_gp > 0 else 0.0

    @property
    def opponent_div_rank(self) -> int:
        return DIVISION_RANK.get(self.opponent_division, 0)


@dataclass
class Team:
    name:           str
    division:       str
    classification: str
    sport:          str
    playing_up:     bool = False

    @property
    def div_rank(self) -> int:
        return 4 if self.playing_up else DIVISION_RANK.get(self.division, 0)


@dataclass
class GamePoints:
    game:        GameResult
    base:        float = 0.0
    div_bonus:   float = 0.0
    opp_quality: float = 0.0

    @property
    def total(self) -> float:
        return self.base + self.div_bonus + self.opp_quality


@dataclass
class TeamRating:
    name:         str
    sport:        str
    power_rating: float
    wins:         int
    losses:       int
    ties:         int
    games_played: int
    division:     str  = ""
    rank:         int  = 0
    breakdown:    list = field(default_factory=list)

    @property
    def record(self) -> str:
        if self.ties:
            return f"{self.wins}-{self.losses}-{self.ties}"
        return f"{self.wins}-{self.losses}"


# ─── ENGINE ───────────────────────────────────────────────────────────────────

class PowerRatingEngine:

    def __init__(self):
        self.teams: dict[str, Team] = {}
        self.games: dict[str, list[GameResult]] = {}

    def add_team(self, team: Team):
        self.teams[team.name] = team
        if team.name not in self.games:
            self.games[team.name] = []

    def add_game(self, game: GameResult):
        if game.team not in self.games:
            self.games[game.team] = []
        self.games[game.team].append(game)

    def score_game(self, game: GameResult, team: Team) -> GamePoints:
        gp     = GamePoints(game=game)
        config = get_sport_config(team.sport, team.classification)

        # Skip non-counting games
        if game.result in ("OD", "JV", "PPD"):
            return gp

        oos = game.opponent_out_of_state

        # Base points — OOS games get full base points (win=10, loss=0)
        # Only div bonus is excluded for OOS games
        if game.result == "W":
            gp.base = config["win_points"]
        elif game.result == "L":
            gp.base = config["loss_points"]
        elif game.result == "T":
            gp.base = config["tie_points"]
        elif game.result == "DF":
            gp.base = config.get("forfeit_bonus", 0)

        # Division bonus
        # In-state: opponent must be higher in BOTH class AND division
        # OOS: award +2 per class level higher (class only — no Select/NS in other states)
        if config["has_div_bonus"]:
            opp_class_rank  = CLASS_RANK.get(game.opponent_class, 0)
            team_class_rank = CLASS_RANK.get(team.classification, 0)
            div_diff        = game.opponent_div_rank - team.div_rank
            class_diff      = opp_class_rank - team_class_rank
            if oos:
                if class_diff > 0:
                    gp.div_bonus = class_diff * config["div_bonus_per_div"]
            else:
                if div_diff > 0 and class_diff > 0:
                    gp.div_bonus = div_diff * config["div_bonus_per_div"]
            # Play-up bonus (basketball)
            if game.playing_up and config.get("play_up_bonus"):
                gp.div_bonus += config["play_up_bonus"]

        # Opponent quality
        style = config["opp_quality"]
        if style == "win_pct_x10":
            gp.opp_quality = game.opponent_win_pct * 10
        elif style == "win_pct_x34":
            gp.opp_quality = game.opponent_win_pct * 34
        elif style == "win_pct_x44":
            gp.opp_quality = game.opponent_win_pct * 44
        elif style == "raw_wins":
            gp.opp_quality = float(game.opponent_wins)
        elif style == "soccer_weighted":
            if game.result == "W":
                gp.opp_quality = game.opponent_win_pct * 1.0
            elif game.result == "L":
                gp.opp_quality = game.opponent_win_pct * 0.5
            elif game.result == "T":
                gp.opp_quality = game.opponent_win_pct * 0.75

        return gp

    def rate_team(self, team_name: str) -> Optional[TeamRating]:
        if team_name not in self.teams:
            return None
        team  = self.teams[team_name]
        games = self.games.get(team_name, [])
        if not games:
            return None

        total = 0.0
        counted = wins = losses = ties = 0
        breakdown = []

        for game in games:
            if game.result in ("OD", "JV", "PPD"):
                continue
            gp = self.score_game(game, team)
            total   += gp.total
            counted += 1
            if game.result == "W":   wins   += 1
            elif game.result == "L": losses += 1
            elif game.result == "T": ties   += 1
            breakdown.append({
                "week":     game.week,
                "opponent": game.opponent,
                "result":   game.result,
                "base":     round(gp.base, 4),
                "div":      round(gp.div_bonus, 4),
                "oppq":     round(gp.opp_quality, 4),
                "total":    round(gp.total, 4),
            })

        if counted == 0:
            return None

        return TeamRating(
            name=team_name,
            sport=team.sport,
            power_rating=round(total / counted, 2),
            wins=wins,
            losses=losses,
            ties=ties,
            games_played=counted,
            division=team.division,
            breakdown=breakdown,
        )

    def rate_all(self) -> list[TeamRating]:
        ratings = [r for name in self.teams if (r := self.rate_team(name))]
        ratings.sort(key=lambda r: r.power_rating, reverse=True)
        for i, r in enumerate(ratings):
            r.rank = i + 1
        return ratings


# ─── PLAYOFF PREDICTOR ────────────────────────────────────────────────────────

class PlayoffPredictor:
    """Scenario simulator — flip any game, instantly see new standings."""

    def __init__(self, engine: PowerRatingEngine):
        self.engine    = engine
        self.overrides = {}

    def flip(self, team: str, week: int, result: str):
        self.overrides[f"{team}::{week}"] = result

    def reset(self):
        self.overrides = {}

    def simulate(self) -> list[TeamRating]:
        import copy
        eng = copy.deepcopy(self.engine)
        for key, result in self.overrides.items():
            team_name, week = key.split("::")
            if team_name in eng.games:
                for g in eng.games[team_name]:
                    if str(g.week) == str(week):
                        g.result = result
                opp = eng.games[team_name][0].opponent if eng.games[team_name] else None
                if opp and opp in eng.games:
                    for g in eng.games[opp]:
                        if str(g.week) == str(week):
                            if result == "W":   g.result = "L"
                            elif result == "L": g.result = "W"
                            elif result == "T": g.result = "T"
        return eng.rate_all()

    def what_if(self, team: str, week: int, wins: bool) -> dict:
        self.flip(team, week, "W" if wins else "L")
        standings = self.simulate()
        self.reset()
        for r in standings:
            if r.name == team:
                return {"team": team, "week": week,
                        "scenario": "wins" if wins else "loses",
                        "new_rank": r.rank, "new_pr": r.power_rating,
                        "record": r.record}
        return {"error": "Team not found"}


# ─── TEST ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== FOOTBALL ===")
    eng = PowerRatingEngine()
    eng.add_team(Team("Calvary Baptist", "Select Division III", "2A", "football"))
    eng.add_team(Team("Huntington",      "Select Division I",   "5A", "football"))
    eng.add_game(GameResult("Calvary Baptist", "Huntington", "W", "football",
                            opponent_wins=6, opponent_losses=4,
                            opponent_division="Select Division I", week=8))
    eng.add_game(GameResult("Huntington", "Calvary Baptist", "L", "football",
                            opponent_wins=7, opponent_losses=3,
                            opponent_division="Select Division III", week=8))
    for r in eng.rate_all():
        print(f"  #{r.rank} {r.name} PR={r.power_rating} {r.record}")
        for g in r.breakdown:
            print(f"    Wk{g['week']} vs {g['opponent']}: {g['result']} | "
                  f"Base:{g['base']} + Div:{g['div']} + OppQ:{g['oppq']} = {g['total']}")
