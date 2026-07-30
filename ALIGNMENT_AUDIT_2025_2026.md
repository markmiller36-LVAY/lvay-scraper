# 2025-2026 Archived Alignment Audit

Audit date: July 30, 2026

## Scope

The live archived schedule/ranking assignments were compared with the saved
official LHSAA 2025-2026 regular-season alignment reports and the official
2024-2026 playoff divisional report.

Season keys used by the application:

- Football and volleyball: `2025`
- Basketball, soccer, baseball, and softball: `2026`

## Results before correction

| Sport | Live schools | Official schools | Assignment mismatches |
| --- | ---: | ---: | ---: |
| Football | 304 | 308 | 0 |
| Volleyball | 258 | 264 | 0 |
| Boys Basketball | 380 | 384 | 0 |
| Girls Basketball | 374 | 379 | 1 |
| Boys Soccer | 180 | 189 | 0 |
| Girls Soccer | 161 | 184 | 0 |
| Baseball | 343 | 355 | 2 |
| Softball | 347 | 358 | 2 |

Fewer live schools than official schools means those schools had no live
ranking/schedule record. Every live school matched an official school name;
there were no unresolved names.

## Confirmed corrections

- Harrisonburg is Class B, District 5, and Class B for the final 2025-2026
  girls basketball, baseball, and softball archive data. The later sport
  reports supersede its Class C placement in the April 2024 master file.
- Helix Mentorship Academy baseball is Class 3A, District 3.
- Booker T. Washington - N.O. softball is Class 3A, District 9.

## Protection against season leakage

The official assignments are stored in
`sport_alignments_2025_2026.json` and only apply to their archived season
keys. They do not automatically apply to the 2026-2027 cycle.

The source-building script is `tools/build_sport_alignments.py`.
