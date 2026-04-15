"""
LHSAA School Database
=====================

Division assignments built from official LHSAA 2025 final power rankings.
Alignment (class + district) from Football.pdf 2025-2026 alignment document.

Current cycle: 2024-2026
Next update: Before 2026 season when new reclassification is released.
"""

import re

# ──────────────────────────────────────────────────────────────────────────────
# SCHOOL NAME NORMALIZATION / ALIASES
# ──────────────────────────────────────────────────────────────────────────────

SCHOOL_ALIASES = {
    "Acadiana Renaissance Charter": "Acadiana Renaissance Charter Academy",
    "Avoyelles Public Charter": "Avoyelles Public Charter School",
    "David Thibodaux": "David Thibodaux STEM Magnet",
    "False River": "False River Academy",
    "Morris Jeff": "Morris Jeff Community School",
    "New Orleans Military & Maritime": "New Orleans Military and Maritime Academy",
    "V.B. Glencoe Charter": "V. B. Glencoe Charter School",
}

# ──────────────────────────────────────────────────────────────────────────────
# DIVISION ASSIGNMENTS
# ──────────────────────────────────────────────────────────────────────────────

SELECT_D1 = [
    "Edna Karr", "Teurlings Catholic", "St. Augustine", "Catholic - B.R.",
    "Alexandria", "Tioga", "John Curtis Christian", "Evangel Christian",
    "St. Paul's", "St. Thomas More", "Archbishop Rummel", "Brother Martin",
    "Hammond", "Acadiana", "Riverdale", "McDonogh #35", "Jesuit", "Bonnabel",
    "Carencro", "Captain Shreve", "Liberty", "Holy Cross", "Lafayette",
    "Pineville", "Comeaux", "Woodlawn - B.R.", "Warren Easton", "John Ehret",
    "Huntington", "Southwood", "L. W. Higgins", "C.E. Byrd", "Scotlandville",
    "Ponchatoula", "West Jefferson", "East Jefferson", "Ben Franklin",
]

SELECT_D2 = [
    "St. Charles", "Loyola Prep", "Lake Charles College Prep", "University Lab",
    "Madison Prep", "Vandebilt Catholic", "Archbishop Shaw", "E.D. White",
    "St. Michael the Archangel", "Haynes Academy", "George Washington Carver",
    "John F. Kennedy", "Booker T. Washington - Shr.", "Leesville",
    "Patrick Taylor - Science/Tech.", "Lord Beaconsfield Landry", "Northside",
    "Belaire", "Kenner Discovery Health Science", "Istrouma",
    "Frederick A Douglass", "Livingston Collegiate", "Abramson", "Loranger",
    "Buckeye", "Archbishop Hannan", "Helix Mentorship Academy", "Peabody",
    "Washington-Marion", "Eleanor McMain", "McKinley", "Sophie B. Wright",
    "Booker T. Washington - N.O.", "Tara", "Bolton Academy",
    "Young Audiences Charter", "The Willow School",
]

SELECT_D3 = [
    "Lafayette Christian", "Notre Dame", "Jewel Sumner", "Isidore Newman",
    "Dunham", "Calvary Baptist", "Lafayette Renaissance Charter Academy",
    "Bunkie", "Catholic - N.I.", "Amite", "Metairie Park Country Day",
    "Northlake Christian", "Holy Savior Menard", "Slaughter Community Charter",
    "Episcopal", "Parkview Baptist", "D'Arbonne Woods Charter", "De La Salle",
    "Pope John Paul II", "Thomas Jefferson", "St. Louis Catholic",
    "Beekman Charter", "Walter L. Cohen", "Glen Oaks", "Independence",
    "Rosepine", "Sarah T. Reed", "Houma Christian", "North Caddo",
    "St. Thomas Aquinas", "Jefferson Rise Charter", "Collegiate Baton Rouge",
    "Fisher", "Capitol", "Magnolia School of Excellence",
]

SELECT_D4 = [
    "Westminster Christian", "Riverside Academy", "Hamilton Christian",
    "Catholic - P.C.", "Ascension Catholic", "Ascension Episcopal",
    "Ouachita Christian", "Covenant Christian", "St. Edmund", "Southern Lab",
    "Kentwood", "St. John", "Sacred Heart", "St. Frederick",
    "St. Martin's Episcopal", "Opelousas Catholic", "Ascension Christian",
    "Delta Charter", "Cedar Creek", "Westminster Christian - Lafayette",
    "St. Mary's", "Vermilion Catholic", "Central Catholic",
    "Lincoln Preparatory School", "Central Private", "Delhi Charter",
    "Glenbrook", "Hanson Memorial", "Berchmans Academy", "Northwood - Lena",
    "Crescent City", "Thrive Academy", "Block", "Highland Baptist", "Pickering",
]

NS_D1 = [
    "Ruston", "Neville", "Destrehan", "Denham Springs", "Central - B.R.",
    "Parkway", "Northwood - Shrev.", "Southside", "Terrebonne", "West Monroe",
    "Hahnville", "East Ascension", "Zachary", "Ouachita Parish", "Thibodaux",
    "Westgate", "Airline", "Mandeville", "St. Amant", "Salmen", "West Ouachita",
    "Slidell", "South Lafourche", "Natchitoches Central", "Prairieville", "Barbe",
    "Covington", "Dutchtown", "Chalmette", "H.L. Bourgeois", "Northshore",
    "Live Oak", "Sam Houston", "East St. John", "Benton", "Central Lafourche",
    "Fontainebleau", "Sulphur", "Haughton", "Walker", "New Iberia",
]

NS_D2 = [
    "North DeSoto", "Iowa", "Belle Chasse", "Lakeshore", "Plaquemine", "Brusly",
    "Franklin Parish", "Franklinton", "Jennings", "Lutcher", "West Feliciana",
    "Minden", "Cecilia", "Pearl River", "Northwest", "DeRidder", "Eunice",
    "South Terrebonne", "Opelousas", "Carroll", "Wossman", "Bossier", "Iota",
    "St. Martinville", "Rayne", "Abbeville", "Albany", "Livonia", "Broadmoor",
    "Assumption", "A.J. Ellender", "Woodlawn - Shrev.", "Breaux Bridge", "Grant",
    "South Beauregard", "North Vermilion", "Morgan City", "Bastrop", "LaGrange",
    "Beau Chene",
]

NS_D3 = [
    "Jena", "Sterlington", "St. James", "Kinder", "Erath", "Oak Grove",
    "Loreauville", "Mansfield", "Marksville", "Church Point", "Richwood",
    "Union Parish", "Pine", "Many", "Donaldsonville", "Avoyelles",
    "St. Helena College & Career Acad.", "Westlake", "Red River", "Kaplan",
    "Rayville", "Caldwell Parish", "North Webster", "Patterson", "Winnfield",
    "Ville Platte", "Port Allen", "Bogalusa", "Port Barre", "Mamou", "Lakeside",
    "Green Oaks", "Baker", "Vidalia", "Springfield", "Madison", "Crowley",
    "Oakdale", "Berwick", "Pine Prairie",
]

NS_D4 = [
    "Haynesville", "Mangham", "South Plaquemines", "Jeanerette", "Logansport",
    "Ringgold", "North Iberville", "East Feliciana", "Vinton", "Jonesboro-Hodge",
    "Ferriday", "Elton", "Welsh", "Grand Lake", "West St. Mary", "West St. John",
    "Homer", "Franklin", "General Trass", "Basile", "Montgomery", "Lake Arthur",
    "LaSalle", "Northeast", "North Central", "DeQuincy", "Delcambre", "Arcadia",
    "Varnado", "East Iberville", "Merryville", "Oberlin", "Centerville",
    "White Castle", "East Beauregard", "Delhi", "Lakeview", "Plain Dealing",
    "Gueydan",
]

# ──────────────────────────────────────────────────────────────────────────────
# 2025-2026 ALIGNMENT (Class + District from Football.pdf)
# ──────────────────────────────────────────────────────────────────────────────

ALIGNMENT = {
    # 5A
    "Airline": {"class": "5A", "district": 1},
    "Benton": {"class": "5A", "district": 1},
    "C.E. Byrd": {"class": "5A", "district": 1},
    "Captain Shreve": {"class": "5A", "district": 1},
    "Evangel Christian": {"class": "5A", "district": 1},
    "Haughton": {"class": "5A", "district": 1},
    "Huntington": {"class": "5A", "district": 1},
    "Natchitoches Central": {"class": "5A", "district": 1},
    "Parkway": {"class": "5A", "district": 1},
    "Alexandria": {"class": "5A", "district": 2},
    "Neville": {"class": "5A", "district": 2},
    "Ouachita Parish": {"class": "5A", "district": 2},
    "Pineville": {"class": "5A", "district": 2},
    "Ruston": {"class": "5A", "district": 2},
    "West Monroe": {"class": "5A", "district": 2},
    "Acadiana": {"class": "5A", "district": 3},
    "Barbe": {"class": "5A", "district": 3},
    "Carencro": {"class": "5A", "district": 3},
    "Lafayette": {"class": "5A", "district": 3},
    "New Iberia": {"class": "5A", "district": 3},
    "Sam Houston": {"class": "5A", "district": 3},
    "Southside": {"class": "5A", "district": 3},
    "Sulphur": {"class": "5A", "district": 3},
    "Catholic - B.R.": {"class": "5A", "district": 4},
    "Central - B.R.": {"class": "5A", "district": 4},
    "Liberty": {"class": "5A", "district": 4},
    "Scotlandville": {"class": "5A", "district": 4},
    "Woodlawn - B.R.": {"class": "5A", "district": 4},
    "Zachary": {"class": "5A", "district": 4},
    "Denham Springs": {"class": "5A", "district": 5},
    "Dutchtown": {"class": "5A", "district": 5},
    "East Ascension": {"class": "5A", "district": 5},
    "Live Oak": {"class": "5A", "district": 5},
    "Prairieville": {"class": "5A", "district": 5},
    "St. Amant": {"class": "5A", "district": 5},
    "Walker": {"class": "5A", "district": 5},
    "Covington": {"class": "5A", "district": 6},
    "Hammond": {"class": "5A", "district": 6},
    "Mandeville": {"class": "5A", "district": 6},
    "Ponchatoula": {"class": "5A", "district": 6},
    "St. Paul's": {"class": "5A", "district": 6},
    "Chalmette": {"class": "5A", "district": 7},
    "Fontainebleau": {"class": "5A", "district": 7},
    "Northshore": {"class": "5A", "district": 7},
    "Salmen": {"class": "5A", "district": 7},
    "Slidell": {"class": "5A", "district": 7},
    "Central Lafourche": {"class": "5A", "district": 8},
    "Destrehan": {"class": "5A", "district": 8},
    "East St. John": {"class": "5A", "district": 8},
    "H.L. Bourgeois": {"class": "5A", "district": 8},
    "Hahnville": {"class": "5A", "district": 8},
    "Terrebonne": {"class": "5A", "district": 8},
    "Thibodaux": {"class": "5A", "district": 8},
    "Archbishop Rummel": {"class": "5A", "district": 9},
    "Brother Martin": {"class": "5A", "district": 9},
    "Edna Karr": {"class": "5A", "district": 9},
    "Holy Cross": {"class": "5A", "district": 9},
    "Jesuit": {"class": "5A", "district": 9},
    "John Curtis Christian": {"class": "5A", "district": 9},
    "St. Augustine": {"class": "5A", "district": 9},
    "Warren Easton": {"class": "5A", "district": 9},
    "Ben Franklin": {"class": "5A", "district": 10},
    "Bonnabel": {"class": "5A", "district": 10},
    "East Jefferson": {"class": "5A", "district": 10},
    "John Ehret": {"class": "5A", "district": 10},
    "L. W. Higgins": {"class": "5A", "district": 10},
    "Riverdale": {"class": "5A", "district": 10},
    "West Jefferson": {"class": "5A", "district": 10},

    # 4A
    "Booker T. Washington - Shr.": {"class": "4A", "district": 1},
    "Bossier": {"class": "4A", "district": 1},
    "Loyola Prep": {"class": "4A", "district": 1},
    "Minden": {"class": "4A", "district": 1},
    "North DeSoto": {"class": "4A", "district": 1},
    "Northwood - Shrev.": {"class": "4A", "district": 1},
    "Southwood": {"class": "4A", "district": 1},
    "Woodlawn - Shrev.": {"class": "4A", "district": 1},
    "Franklin Parish": {"class": "4A", "district": 2},
    "Grant": {"class": "4A", "district": 2},
    "Peabody": {"class": "4A", "district": 2},
    "Tioga": {"class": "4A", "district": 2},
    "West Ouachita": {"class": "4A", "district": 2},
    "Wossman": {"class": "4A", "district": 2},
    "DeRidder": {"class": "4A", "district": 3},
    "Eunice": {"class": "4A", "district": 3},
    "Iowa": {"class": "4A", "district": 3},
    "LaGrange": {"class": "4A", "district": 3},
    "Leesville": {"class": "4A", "district": 3},
    "Washington-Marion": {"class": "4A", "district": 3},
    "Comeaux": {"class": "4A", "district": 4},
    "North Vermilion": {"class": "4A", "district": 4},
    "Northside": {"class": "4A", "district": 4},
    "Rayne": {"class": "4A", "district": 4},
    "St. Thomas More": {"class": "4A", "district": 4},
    "Teurlings Catholic": {"class": "4A", "district": 4},
    "Westgate": {"class": "4A", "district": 4},
    "Beau Chene": {"class": "4A", "district": 5},
    "Breaux Bridge": {"class": "4A", "district": 5},
    "Cecilia": {"class": "4A", "district": 5},
    "Livonia": {"class": "4A", "district": 5},
    "Opelousas": {"class": "4A", "district": 5},
    "Belaire": {"class": "4A", "district": 6},
    "Broadmoor": {"class": "4A", "district": 6},
    "Brusly": {"class": "4A", "district": 6},
    "Istrouma": {"class": "4A", "district": 6},
    "McKinley": {"class": "4A", "district": 6},
    "Plaquemine": {"class": "4A", "district": 6},
    "St. Michael the Archangel": {"class": "4A", "district": 6},
    "Tara": {"class": "4A", "district": 6},
    "West Feliciana": {"class": "4A", "district": 6},
    "Archbishop Hannan": {"class": "4A", "district": 7},
    "Franklinton": {"class": "4A", "district": 7},
    "Lakeshore": {"class": "4A", "district": 7},
    "Loranger": {"class": "4A", "district": 7},
    "Pearl River": {"class": "4A", "district": 7},
    "A.J. Ellender": {"class": "4A", "district": 8},
    "Assumption": {"class": "4A", "district": 8},
    "E.D. White": {"class": "4A", "district": 8},
    "Lutcher": {"class": "4A", "district": 8},
    "Morgan City": {"class": "4A", "district": 8},
    "South Lafourche": {"class": "4A", "district": 8},
    "South Terrebonne": {"class": "4A", "district": 8},
    "Vandebilt Catholic": {"class": "4A", "district": 8},
    "Archbishop Shaw": {"class": "4A", "district": 9},
    "Belle Chasse": {"class": "4A", "district": 9},
    "Kenner Discovery Health Science": {"class": "4A", "district": 9},
    "St. Charles": {"class": "4A", "district": 9},
    "The Willow School": {"class": "4A", "district": 9},
    "Abramson": {"class": "4A", "district": 10},
    "Eleanor McMain": {"class": "4A", "district": 10},
    "Frederick A Douglass": {"class": "4A", "district": 10},
    "George Washington Carver": {"class": "4A", "district": 10},
    "McDonogh #35": {"class": "4A", "district": 10},

    # 3A
    "Bastrop": {"class": "3A", "district": 1},
    "Carroll": {"class": "3A", "district": 1},
    "North Webster": {"class": "3A", "district": 1},
    "Richwood": {"class": "3A", "district": 1},
    "Sterlington": {"class": "3A", "district": 1},
    "Buckeye": {"class": "3A", "district": 2},
    "Bunkie": {"class": "3A", "district": 2},
    "Caldwell Parish": {"class": "3A", "district": 2},
    "Jena": {"class": "3A", "district": 2},
    "Marksville": {"class": "3A", "district": 2},
    "Vidalia": {"class": "3A", "district": 2},
    "Jennings": {"class": "3A", "district": 3},
    "Lake Charles College Prep": {"class": "3A", "district": 3},
    "South Beauregard": {"class": "3A", "district": 3},
    "St. Louis Catholic": {"class": "3A", "district": 3},
    "Westlake": {"class": "3A", "district": 3},
    "Church Point": {"class": "3A", "district": 4},
    "Crowley": {"class": "3A", "district": 4},
    "Iota": {"class": "3A", "district": 4},
    "Mamou": {"class": "3A", "district": 4},
    "Northwest": {"class": "3A", "district": 4},
    "Pine Prairie": {"class": "3A", "district": 4},
    "Ville Platte": {"class": "3A", "district": 4},
    "Abbeville": {"class": "3A", "district": 5},
    "Erath": {"class": "3A", "district": 5},
    "Kaplan": {"class": "3A", "district": 5},
    "St. Martinville": {"class": "3A", "district": 5},
    "Collegiate Baton Rouge": {"class": "3A", "district": 6},
    "Glen Oaks": {"class": "3A", "district": 6},
    "Helix Mentorship Academy": {"class": "3A", "district": 6},
    "Madison Prep": {"class": "3A", "district": 6},
    "Parkview Baptist": {"class": "3A", "district": 6},
    "Port Allen": {"class": "3A", "district": 6},
    "University Lab": {"class": "3A", "district": 6},
    "Berwick": {"class": "3A", "district": 7},
    "Donaldsonville": {"class": "3A", "district": 7},
    "Patterson": {"class": "3A", "district": 7},
    "St. James": {"class": "3A", "district": 7},
    "Albany": {"class": "3A", "district": 8},
    "Amite": {"class": "3A", "district": 8},
    "Bogalusa": {"class": "3A", "district": 8},
    "Jewel Sumner": {"class": "3A", "district": 8},
    "Pine": {"class": "3A", "district": 8},
    "Springfield": {"class": "3A", "district": 8},
    "Fisher": {"class": "3A", "district": 9},
    "Haynes Academy": {"class": "3A", "district": 9},
    "Jefferson Rise Charter": {"class": "3A", "district": 9},
    "Patrick Taylor - Science/Tech.": {"class": "3A", "district": 9},
    "Thomas Jefferson": {"class": "3A", "district": 9},
    "Young Audiences Charter": {"class": "3A", "district": 9},
    "Bolton Academy": {"class": "3A", "district": 10},
    "Booker T. Washington - N.O.": {"class": "3A", "district": 10},
    "De La Salle": {"class": "3A", "district": 10},
    "John F. Kennedy": {"class": "3A", "district": 10},
    "Livingston Collegiate": {"class": "3A", "district": 10},
    "Lord Beaconsfield Landry": {"class": "3A", "district": 10},
    "Sophie B. Wright": {"class": "3A", "district": 10},

    # 2A
    "Calvary Baptist": {"class": "2A", "district": 1},
    "D'Arbonne Woods Charter": {"class": "2A", "district": 1},
    "Green Oaks": {"class": "2A", "district": 1},
    "Homer": {"class": "2A", "district": 1},
    "Magnolia School of Excellence": {"class": "2A", "district": 1},
    "North Caddo": {"class": "2A", "district": 1},
    "Union Parish": {"class": "2A", "district": 1},
    "Beekman Charter": {"class": "2A", "district": 2},
    "Delhi Charter": {"class": "2A", "district": 2},
    "Ferriday": {"class": "2A", "district": 2},
    "Madison": {"class": "2A", "district": 2},
    "Mangham": {"class": "2A", "district": 2},
    "Oak Grove": {"class": "2A", "district": 2},
    "Ouachita Christian": {"class": "2A", "district": 2},
    "Rayville": {"class": "2A", "district": 2},
    "Lakeside": {"class": "2A", "district": 3},
    "Mansfield": {"class": "2A", "district": 3},
    "Many": {"class": "2A", "district": 3},
    "Red River": {"class": "2A", "district": 3},
    "Winnfield": {"class": "2A", "district": 3},
    "DeQuincy": {"class": "2A", "district": 4},
    "Pickering": {"class": "2A", "district": 4},
    "East Beauregard": {"class": "2A", "district": 4},
    "Rosepine": {"class": "2A", "district": 4},
    "Vinton": {"class": "2A", "district": 4},
    "Avoyelles": {"class": "2A", "district": 5},
    "Holy Savior Menard": {"class": "2A", "district": 5},
    "Kinder": {"class": "2A", "district": 5},
    "Oakdale": {"class": "2A", "district": 5},
    "Port Barre": {"class": "2A", "district": 5},
    "Lafayette Christian": {"class": "2A", "district": 6},
    "Lafayette Renaissance Charter Academy": {"class": "2A", "district": 6},
    "Lake Arthur": {"class": "2A", "district": 6},
    "Notre Dame": {"class": "2A", "district": 6},
    "Welsh": {"class": "2A", "district": 6},
    "Catholic - N.I.": {"class": "2A", "district": 7},
    "Delcambre": {"class": "2A", "district": 7},
    "Franklin": {"class": "2A", "district": 7},
    "Houma Christian": {"class": "2A", "district": 7},
    "Loreauville": {"class": "2A", "district": 7},
    "West St. Mary": {"class": "2A", "district": 7},
    "Baker": {"class": "2A", "district": 8},
    "Capitol": {"class": "2A", "district": 8},
    "Dunham": {"class": "2A", "district": 8},
    "East Feliciana": {"class": "2A", "district": 8},
    "Episcopal": {"class": "2A", "district": 8},
    "Northeast": {"class": "2A", "district": 8},
    "Slaughter Community Charter": {"class": "2A", "district": 8},
    "Independence": {"class": "2A", "district": 9},
    "Northlake Christian": {"class": "2A", "district": 9},
    "Pope John Paul II": {"class": "2A", "district": 9},
    "St. Helena College & Career Acad.": {"class": "2A", "district": 9},
    "St. Thomas Aquinas": {"class": "2A", "district": 9},
    "Isidore Newman": {"class": "2A", "district": 10},
    "Metairie Park Country Day": {"class": "2A", "district": 10},
    "Sarah T. Reed": {"class": "2A", "district": 10},
    "South Plaquemines": {"class": "2A", "district": 10},
    "Walter L. Cohen": {"class": "2A", "district": 10},

    # 1A
    "Arcadia": {"class": "1A", "district": 1},
    "Ringgold": {"class": "1A", "district": 1},
    "Cedar Creek": {"class": "1A", "district": 1},
    "Glenbrook": {"class": "1A", "district": 1},
    "Haynesville": {"class": "1A", "district": 1},
    "Jonesboro-Hodge": {"class": "1A", "district": 1},
    "Lincoln Preparatory School": {"class": "1A", "district": 1},
    "Plain Dealing": {"class": "1A", "district": 1},
    "Block": {"class": "1A", "district": 2},
    "Delhi": {"class": "1A", "district": 2},
    "Delta Charter": {"class": "1A", "district": 2},
    "General Trass": {"class": "1A", "district": 2},
    "St. Frederick": {"class": "1A", "district": 2},
    "Lakeview": {"class": "1A", "district": 3},
    "LaSalle": {"class": "1A", "district": 3},
    "Logansport": {"class": "1A", "district": 3},
    "Montgomery": {"class": "1A", "district": 3},
    "Northwood - Lena": {"class": "1A", "district": 3},
    "St. Mary's": {"class": "1A", "district": 3},
    "Basile": {"class": "1A", "district": 4},
    "Elton": {"class": "1A", "district": 4},
    "Grand Lake": {"class": "1A", "district": 4},
    "Hamilton Christian": {"class": "1A", "district": 4},
    "Merryville": {"class": "1A", "district": 4},
    "Oberlin": {"class": "1A", "district": 4},
    "Berchmans Academy": {"class": "1A", "district": 5},
    "Catholic - P.C.": {"class": "1A", "district": 5},
    "North Central": {"class": "1A", "district": 5},
    "Opelousas Catholic": {"class": "1A", "district": 5},
    "Sacred Heart": {"class": "1A", "district": 5},
    "St. Edmund": {"class": "1A", "district": 5},
    "Westminster Christian": {"class": "1A", "district": 5},
    "Ascension Episcopal": {"class": "1A", "district": 6},
    "Gueydan": {"class": "1A", "district": 6},
    "Highland Baptist": {"class": "1A", "district": 6},
    "Vermilion Catholic": {"class": "1A", "district": 6},
    "Westminster Christian - Lafayette": {"class": "1A", "district": 6},
    "Centerville": {"class": "1A", "district": 7},
    "Central Catholic": {"class": "1A", "district": 7},
    "Covenant Christian": {"class": "1A", "district": 7},
    "Hanson Memorial": {"class": "1A", "district": 7},
    "Jeanerette": {"class": "1A", "district": 7},
    "Ascension Catholic": {"class": "1A", "district": 8},
    "Ascension Christian": {"class": "1A", "district": 8},
    "East Iberville": {"class": "1A", "district": 8},
    "North Iberville": {"class": "1A", "district": 8},
    "St. John": {"class": "1A", "district": 8},
    "White Castle": {"class": "1A", "district": 8},
    "Central Private": {"class": "1A", "district": 9},
    "Kentwood": {"class": "1A", "district": 9},
    "Southern Lab": {"class": "1A", "district": 9},
    "Thrive Academy": {"class": "1A", "district": 9},
    "Crescent City": {"class": "1A", "district": 10},
    "Riverside Academy": {"class": "1A", "district": 10},
    "St. Martin's Episcopal": {"class": "1A", "district": 10},
    "Varnado": {"class": "1A", "district": 10},
    "West St. John": {"class": "1A", "district": 10},
}

# ──────────────────────────────────────────────────────────────────────────────
# SUPPLEMENTAL SCHOOLS
# ──────────────────────────────────────────────────────────────────────────────

SUPPLEMENTAL_SCHOOLS = {
    "Acadiana Renaissance Charter Academy": {"class": "3A", "district": 5},
    "Avoyelles Public Charter School": {"class": "B", "district": 5},
    "David Thibodaux STEM Magnet": {"class": "4A", "district": 4},
    "False River Academy": {"class": "C", "district": 7},
    "Morris Jeff Community School": {"class": "3A", "district": 10},
    "New Orleans Military and Maritime Academy": {"class": "4A", "district": 10},
    "St. Joseph's - Plaucheville": {"class": "C", "district": 4},
    "V. B. Glencoe Charter School": {"class": "C", "district": 6},
    "Anacoco": {"class": "B", "district": 4},
    "Bell City": {"class": "B", "district": 6},
    "Calvin": {"class": "C", "district": 2},
    "Castor": {"class": "B", "district": 1},
    "Choudrant": {"class": "B", "district": 2},
    "Claiborne Christian": {"class": "C", "district": 1},
    "Converse": {"class": "B", "district": 3},
    "Dodson": {"class": "C", "district": 2},
    "Downsville": {"class": "B", "district": 2},
    "Doyle": {"class": "3A", "district": 8},
    "Doyline": {"class": "B", "district": 1},
    "Ebarb": {"class": "C", "district": 3},
    "Ecole Classique": {"class": "1A", "district": 10},
    "Elizabeth": {"class": "B", "district": 4},
    "Episcopal of Acadiana": {"class": "B", "district": 6},
    "Evans": {"class": "C", "district": 3},
    "Fairview": {"class": "B", "district": 4},
    "Family Christian": {"class": "C", "district": 7},
    "Family Community": {"class": "B", "district": 2},
    "Florien": {"class": "B", "district": 3},
    "Forest": {"class": "B", "district": 2},
    "French Settlement": {"class": "2A", "district": 9},
    "Georgetown": {"class": "C", "district": 2},
    "Glenmora": {"class": "B", "district": 5},
    "Grace Christian": {"class": "B", "district": 5},
    "Hackberry": {"class": "C", "district": 5},
    "Harrisonburg": {"class": "B", "district": 5},
    "Hathaway": {"class": "B", "district": 6},
    "Hicks": {"class": "B", "district": 4},
    "Holden": {"class": "B", "district": 7},
    "Hornbeck": {"class": "C", "district": 3},
    "Lacassine": {"class": "B", "district": 6},
    "Maurepas": {"class": "C", "district": 7},
    "Midland": {"class": "2A", "district": 6},
    "Monterey": {"class": "B", "district": 5},
    "Mt. Hermon": {"class": "B", "district": 7},
    "Negreet": {"class": "B", "district": 3},
    "Northside Christian": {"class": "C", "district": 6},
    "Oak Hill": {"class": "B", "district": 5},
    "Pitkin": {"class": "B", "district": 4},
    "Plainview": {"class": "C", "district": 4},
    "Pleasant Hill": {"class": "C", "district": 3},
    "Quitman": {"class": "B", "district": 1},
    "Rapides": {"class": "2A", "district": 5},
    "Reeves": {"class": "C", "district": 6},
    "Saline": {"class": "C", "district": 1},
    "Simpson": {"class": "C", "district": 3},
    "Simsboro": {"class": "B", "district": 1},
    "Singer": {"class": "C", "district": 5},
    "South Cameron": {"class": "C", "district": 5},
    "Stanley": {"class": "B", "district": 3},
    "Starks": {"class": "C", "district": 5},
    "Summerfield": {"class": "C", "district": 1},
    "Weston": {"class": "B", "district": 1},
    "Zwolle": {"class": "B", "district": 3},
}

# ──────────────────────────────────────────────────────────────────────────────
# NORMALIZATION
# ──────────────────────────────────────────────────────────────────────────────

def normalize_school_name(name: str) -> str:
    if not name:
        return ""

    s = str(name).strip()

    s = s.replace("’", "'").replace("‘", "'")
    s = s.replace("“", '"').replace("”", '"')
    s = s.replace("–", "-").replace("—", "-")
    s = s.replace("&", "and")

    s = re.sub(r"\s+", " ", s).strip()
    return s


# ──────────────────────────────────────────────────────────────────────────────
# BUILD MASTER LOOKUP
# ──────────────────────────────────────────────────────────────────────────────

def build_schools():
    schools = {}

    division_lists = [
        (SELECT_D1, "Select Division I", "select"),
        (SELECT_D2, "Select Division II", "select"),
        (SELECT_D3, "Select Division III", "select"),
        (SELECT_D4, "Select Division IV", "select"),
        (NS_D1, "Non-Select Division I", "non-select"),
        (NS_D2, "Non-Select Division II", "non-select"),
        (NS_D3, "Non-Select Division III", "non-select"),
        (NS_D4, "Non-Select Division IV", "non-select"),
    ]

    for school_list, division, track in division_lists:
        for name in school_list:
            align = ALIGNMENT.get(name, {})
            schools[name] = {
                "name": name,
                "division": division,
                "track": track,
                "class": align.get("class"),
                "district": align.get("district"),
            }

    for name, align in ALIGNMENT.items():
        if name not in schools:
            schools[name] = {
                "name": name,
                "division": "Unknown",
                "track": "unknown",
                "class": align.get("class"),
                "district": align.get("district"),
            }

    for name, info in SUPPLEMENTAL_SCHOOLS.items():
        if name not in schools:
            schools[name] = {
                "name": name,
                "division": "Unknown",
                "track": "unknown",
                "class": info["class"],
                "district": info["district"],
            }

    return schools


SCHOOLS = build_schools()
NORMALIZED_SCHOOLS = {
    normalize_school_name(name): info
    for name, info in SCHOOLS.items()
}
NORMALIZED_ALIASES = {
    normalize_school_name(alias): canonical
    for alias, canonical in SCHOOL_ALIASES.items()
}

# ──────────────────────────────────────────────────────────────────────────────
# LOOKUP HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def get_school(name):
    if not name:
        return None

    raw = str(name).strip()
    normalized = normalize_school_name(raw)

    if raw in SCHOOLS:
        return SCHOOLS[raw]

    alias = SCHOOL_ALIASES.get(raw)
    if alias and alias in SCHOOLS:
        return SCHOOLS[alias]

    if normalized in NORMALIZED_SCHOOLS:
        return NORMALIZED_SCHOOLS[normalized]

    alias = NORMALIZED_ALIASES.get(normalized)
    if alias:
        if alias in SCHOOLS:
            return SCHOOLS[alias]

        alias_normalized = normalize_school_name(alias)
        if alias_normalized in NORMALIZED_SCHOOLS:
            return NORMALIZED_SCHOOLS[alias_normalized]

    return None


def get_division(name: str) -> str:
    school = get_school(name)
    return school["division"] if school else "Unknown"


def get_class(name: str) -> str:
    school = get_school(name)
    return school["class"] if school else "Unknown"


def get_track(name: str) -> str:
    school = get_school(name)
    return school["track"] if school else "unknown"


# ──────────────────────────────────────────────────────────────────────────────
# LOCAL TEST
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"Total schools: {len(SCHOOLS)}")

    test_names = [
        "Airline",
        "Calvary Baptist",
        "Haynesville",
        "Westminster Christian",
        "North DeSoto",
        "Jena",
        "Lafayette Christian",
        "Mangham",
        "Acadiana Renaissance Charter",
        "David Thibodaux",
        "Morris Jeff",
        "V.B. Glencoe Charter",
        "False River",
        "New Orleans Military & Maritime",
    ]

    for name in test_names:
        school = get_school(name)
        if school:
            print(
                f"  {name}: "
                f"{school['class']} D{school['district']} | {school['division']}"
            )
        else:
            print(f"  {name}: NOT FOUND")
