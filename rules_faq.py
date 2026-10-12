"""
Plain-English answers to common LHSAA rules questions (Hey Y'all).

Written from the 2026-2027 LHSAA Handbook and reviewed in the doc
"Hey Y'all — Plain-English Rules Answers" (Oct 12, 2026). Each answer is
shown first; the cited bylaws appear underneath as handbook cards.
Eligibility answers carry ELIGIBILITY_NOTE (Mark, Oct 12).
"""

from __future__ import annotations

import re

ELIGIBILITY_NOTE = (
    "This is a general guide to the LHSAA rules, not a ruling. Every "
    "student's situation is different. Direct all eligibility questions to "
    "your school's athletic director and head coach.")

# (id, title, patterns, answer, bylaws, eligibility?)
FAQ = [
    ("power_rating", "How are football power ratings figured?",
     [r"power ?ratings?\b.*\b(figur|calculat|comput|determin|work|done|"
      r"formula|mean)", r"\bhow\b.*\bpower ?ratings?\b",
      r"\bwhat (is|are) (a |the )?power ratings?\b"],
     "Every regular-season game earns points. A win is worth 10, a tie 5 "
     "and a loss 0. Add 2 points for each class and division the opponent "
     "is above you, win or lose. Then add a strength bonus: your opponent's "
     "wins divided by its games played, times 10. Add up every game's "
     "points and divide by the number of games you played, rounded to two "
     "decimals.", ["14.12.1", "14.12.2"], False),
    ("bonus_points", "Do you get bonus points for playing a bigger school?",
     [r"\bbonus\b", r"\b(bigger|larger|higher) (school|class|team|"
      r"classification)", r"\bplay(ing)? up\b", r"\bextra points?\b"],
     "Yes. You get 2 extra points for each class and division your "
     "opponent is above you, even if you lose. There's no bonus or penalty "
     "for playing a smaller school.", ["14.12.1"], False),
    ("oos_rating", "How do out-of-state opponents count?",
     [r"\bout ?of ?state\b.*\b(power|rating|count|class)",
      r"\b(power|rating|count)\b.*\bout ?of ?state\b"],
     "An out-of-state team is given a Louisiana class based on its "
     "enrollment, using the LHSAA's classification numbers. If that team "
     "still has games left after the LHSAA's 10th playing date, the game "
     "doesn't count in your power rating. Only an out-of-state opponent's "
     "first ten games count toward its record.", ["14.12.3"], False),
    ("jv_games", "Do games against JV or \"B\" teams count?",
     [r"\b(jv|junior varsity|b team)\b"],
     "No. A game against a junior varsity or \"B\" team doesn't count "
     "toward your power rating or your strength factor.", ["14.12.4"],
     False),
    ("canceled", "What if a game gets canceled?",
     [r"\bcancel"],
     "If both schools agree to cancel a non-district game, it's left out of "
     "both teams' power ratings. If they agree to cancel a district game, "
     "it's left out of the district standings.", ["14.7.4"], False),
    ("playoff_field", "How many teams make the football playoffs?",
     [r"\bhow many (teams|schools)\b.*\b(playoffs?|bracket|make|get in|"
      r"qualify)", r"\b(wild ?card|wildcard)\b",
      r"\bhow big\b.*\bbracket"],
     "Each Non-Select division (I–IV) has a 28-team bracket and each "
     "Select division (I–IV) has a 24-team bracket.", ["14.9"], False),
    ("district_champ", "Does winning your district get you in?",
     [r"\bdistrict (champ|title|winner|crown)", r"\bwin(ning)? (the |your )?"
      r"district\b", r"\bautomatic(ally)? (qualif|bid|berth|in)"],
     "Yes. The district champion, or every team tied for first, qualifies "
     "automatically. The rest of the bracket is filled with wildcard teams "
     "by highest power rating. A district tie is never broken with an "
     "extra playoff game.", ["14.10", "14.8.4"], False),
    ("seeding", "How are playoff teams seeded?",
     [r"\bseed(ed|ing|s)?\b(?!.*\b(tie|tied|same|identical)\b)"],
     "By power rating: 1–28 in each Non-Select division and 1–24 "
     "in each Select division.", ["14.11.1"], False),
    ("tiebreak", "What if two teams have the same power rating?",
     [r"\b(tie ?break|tiebreak)", r"\b(same|identical|tied)\b.*\b(power|"
      r"rating|seed)", r"\bstrength factor\b"],
     "If they played each other, the winner gets the higher seed. If not, "
     "the higher strength factor wins. The strength factor is the "
     "opponents' classes plus the opponents' wins, divided by games played. "
     "After that comes the best record against your own class, then "
     "district record, then overall record. With three or more teams tied, "
     "the strength factor comes first, then head-to-head.",
     ["14.11.2", "14.11.3", "14.13"], False),
    ("playoff_when", "When and where are playoff games played?",
     [r"\b(when|where|what (day|night|time))\b.*\bplayoff",
      r"\bplayoff\b.*\b(when|where|what day|what night|kickoff|home field|"
      r"host)", r"\bsuperdome\b", r"\bstate championship\b.*\b(where|when)",
      r"\b(where|when)\b.*\bstate (championship|title|final)"],
     "Playoff games are on Friday nights unless both teams agree to play "
     "Thursday or Saturday. If the schools can't agree on a time, kickoff is "
     "7:30 p.m. Through the semifinals, the home team can use its regular "
     "home field. The state championships are played at the Caesars "
     "Superdome (December 10–12 in 2026).",
     ["14.14.6", "14.14.8", "14.16.1"], False),
    ("classification", "How is a school's class decided?",
     [r"\bhow (is|are|do)\b.*\b(class|classes|classification)\b.*\b(decid|"
      r"determin|set|pick|chosen|work)", r"\bwhat (decides|determines|sets)"
      r"\b.*\bclass", r"\benrollment\b"],
     "By enrollment: every student in grades 9–12 as of October 1 of a "
     "classification year, as reported to the Louisiana Department of "
     "Education. Classes are reset every two years.",
     ["Constitution|8.3.1", "Constitution|8.3.2", "Constitution|8.2.1"], False),
    ("game_limit", "How many games can a team play?",
     [r"\bhow many (games|contests)\b", r"\b(scrimmage|jamboree)s?\b",
      r"\bbowl game\b"],
     "Ten regular-season games, no more than one per week "
     "(Sunday–Saturday). Before the season, a team gets one scrimmage "
     "and one jamboree. A team can also play one bowl game after the "
     "season.", ["14.2.1", "14.2.2", "14.2.3"], False),
    ("mercy", "Is there a mercy rule?",
     [r"\bmercy\b", r"\brunning clock\b", r"\b42\b.*\bpoints?\b",
      r"\bpoint differential\b"],
     "Yes. Once a team leads by 42 or more points, the clock runs "
     "continuously for the rest of the game, even if the lead later drops "
     "below 42. The coaches and the referee can also agree to run the "
     "clock, shorten a period or end the game at any time.", ["14.3.9"],
     False),
    ("overtime", "How does overtime work?",
     [r"\bovertime\b", r"\b(ot|extra period)\b"],
     "Every tied game, district, non-district or playoff, goes to overtime "
     "using the NFHS ten-yard procedure until there's a winner. Starting "
     "with the third overtime, a team that scores a touchdown must go for "
     "two.", ["14.3.5"], False),
    ("weather", "What happens when weather stops a game?",
     [r"\b(weather|lightning|storm|rain(ed)? out|suspend)"],
     "The referee, or the host principal, can delay or suspend a game when "
     "weather is dangerous. After a 45-minute delay, the two principals "
     "decide with the referee whether to call it; they can wait 30 more "
     "minutes when travel or rescheduling is a problem. Unless both schools "
     "agree to call it a complete game, it picks up where it stopped. A "
     "suspended Thursday or Friday game is finished the next day, or by "
     "agreement any day before midnight the following Monday.",
     ["14.7.1", "14.7.2", "14.7.3"], False),
    ("forfeit", "What happens if a team forfeits?",
     [r"\bforfeit"],
     "A school can forfeit a non-district game with an official LHSAA form; "
     "it goes on the records as a forfeit loss and win and can't be "
     "replayed. A school that forfeits two or more games in a season is "
     "removed from district play and isn't eligible for the playoffs. In "
     "football, forfeiting two or more scheduled games counts as dropping "
     "the sport, and the school can't return to varsity football for two "
     "years.", ["Athletic Contests|6.21.2", "Athletic Contests|6.21.3.1", "14.5"], False),
    ("oos_games", "Can Louisiana schools play out-of-state teams?",
     [r"\b(play|schedule)\b.*\bout ?of ?state\b",
      r"\bout ?of ?state\b.*\b(allowed|can|play|schedule)"],
     "Yes, as long as the opponent is a member in good standing of its own "
     "state's high school association (part of the NFHS), or is allowed to "
     "play that association's schools.", ["Athletic Contests|6.3.1"], False),
    ("spring", "How does spring practice work?",
     [r"\bspring\b(?!.*\b8th|eighth\b)"],
     "Spring practice is 10 days, with or without pads. A school picks one "
     "window: winter (January 11–February 6, 2027) or late spring "
     "(April 26, 2027 to the end of school), or skips spring. It can't "
     "split the days between windows. One scrimmage is allowed. Schools "
     "that skip spring can start fall practice a week earlier (July 27 "
     "instead of August 3 in 2026).", ["14.17.1", "14.17.3"], False),
    ("eighth_grade", "Can 8th graders practice with the high school team?",
     [r"\b(8th|eighth) grade"],
     "In spring practice, yes, if they live in the school's attendance "
     "zone (or attend a middle school in that zone), are pre-enrolled, "
     "registered with the LHSAA, and have their medical and permission "
     "forms on file. Once an 8th grader practices, that becomes their "
     "school for eligibility. Transferring later without a genuine family "
     "move makes them ineligible for one year.", ["14.17.2"], True),
    ("age", "How old can a player be?",
     [r"\bhow old\b", r"\bage\b", r"\b19\b", r"\btoo old\b",
      r"\bnineteen\b"],
     "A player can't compete once they turn 19 before August 1 of that "
     "school year. A player who turns 19 on or after August 1 stays "
     "eligible for the rest of that school year.", ["1.6"], True),
    ("semesters", "How many years can a player play?",
     [r"\bhow many (years|seasons|semesters)\b.*\b(play|eligib)",
      r"\bsemesters?\b", r"\b(redshirt|fifth year|5th year)\b"],
     "Eight straight semesters, counted from when the student starts 9th "
     "grade, whether or not they play every season.", ["1.4"], True),
    ("grades", "What grades do you need to play?",
     [r"\b(grades?|gpa|c average|carnegie|credits?|academic|passing|"
      r"scholastic)\b(?!.*\b(8th|eighth|9th|ninth)\b)"],
     "For the fall semester, a student needs at least six Carnegie credits "
     "from the year before and a C average. For the spring semester, they "
     "need to pass enough classes to earn at least three credits in the "
     "fall. Schools can set tougher standards, but not easier ones.",
     ["1.11.1", "1.11.2"], True),
    ("transfer_season", "Can a transfer play the same season?",
     [r"\btransfer\w*\b.*\b(same|this|that|rest of the|mid ?-?)\s*season",
      r"\bmid ?-?season\b.*\btransfer"],
     "Not in the same sport. A student who played in a game, scrimmage or "
     "jamboree at the old school is ineligible in that sport for the rest "
     "of the season at the new one. The exception is a genuine family move "
     "of 50 miles or more. No transfer can become eligible once the season "
     "is more than 60% complete.", ["1.13.2"], True),
    ("transfer", "What's the transfer rule?",
     [r"\btransfer", r"\bchang(e|ing) schools?\b", r"\bsit out\b"],
     "Changing schools for any reason after becoming eligible counts as a "
     "transfer. Moving between member schools in the same attendance zone "
     "makes a student ineligible for varsity for one calendar year; 10th "
     "and 11th graders can still play sub-varsity. There are exceptions, "
     "including certain court orders, leaving a failing school, and "
     "approved hardship cases.", ["1.13", "1.13.3"], True),
    ("ejection", "What happens when a player is ejected?",
     [r"\beject", r"\bthrown out\b", r"\bkicked out\b", r"\bfight"],
     "For unsportsmanlike or flagrant conduct, a first ejection brings an "
     "official warning. The player must finish the NFHS Learn "
     "Sportsmanship course within three school days or sit until it's done. "
     "A second ejection means probation and a suspension through the next "
     "game. For fighting, even a first ejection means a suspension through "
     "the next game at that level, including playoffs. The player can't "
     "sit on the bench or travel with the team for that game.",
     ["Penalty Code|5.11.6", "Penalty Code|5.11.7"], True),
    ("freshmen", "Do incoming freshmen have to live in the school's "
     "attendance zone?",
     [r"\b(freshm[ae]n|9th grade|ninth grade|incoming)\b",
      r"\battendance zone\b", r"\bschool of (first )?choice\b"],
     "Not for LHSAA eligibility, starting in 2026–27, but the local "
     "school board still has the final say. A student entering 9th grade "
     "for the first time is immediately eligible for every sport at every "
     "level, varsity included, at the LHSAA school they choose. That only "
     "works if the student is allowed to enroll there. Each school board "
     "decides whether its public schools accept students from outside "
     "their zone. If the board doesn't allow it, the freshman still goes to "
     "their zoned school, unless they choose a private, magnet or charter "
     "school they're admitted to. An 8th grader who signed the 8th Grade "
     "Spring Practice Participation Form is already committed to that "
     "school. After the first choice, any later move falls under the "
     "regular transfer rules. Principals passed this 169–168 on "
     "February 26, 2026.", ["1.3.2"], True),
]

# Most specific first when several match.
PRIORITY = ["freshmen", "eighth_grade", "transfer_season", "tiebreak",
            "oos_rating", "oos_games", "bonus_points", "power_rating",
            "jv_games", "district_champ", "playoff_field", "seeding",
            "playoff_when"]
_ORDER = {fid: i for i, fid in enumerate(PRIORITY)}


def match(question_norm):
    """Return the FAQ entry for a normalized question, or None."""
    hits = []
    for i, entry in enumerate(FAQ):
        if any(re.search(p, question_norm) for p in entry[2]):
            hits.append((_ORDER.get(entry[0], 100 + i), entry))
    if not hits:
        return None
    hits.sort(key=lambda h: h[0])
    return hits[0][1]
