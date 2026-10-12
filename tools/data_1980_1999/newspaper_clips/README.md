# Newspaper scoreboard transcriptions

One file per scoreboard page, one game per line (`Team A 21, Team B 14`), exactly as printed
(paper's own school abbreviations). These are the evidence behind "Verified" ratings
(see project doc football-verification-badges.md): every line that matches the archive
in `tools/compare_clip.py` counts as checked against that page.

File name: `<game Friday>_<paper>_<issue date>_p<page>.txt`

| File | Paper / page | Covers |
|---|---|---|
| 1990-10-12_shreveport-times_1990-10-13_p26.txt | Shreveport Times Sat Oct 13 1990, p26 (4B): City Scores + State Scores + LISA | 1990 week 7 |
| 1990-10-19_shreveport-times_1990-10-20_p14.txt | Shreveport Times Sat Oct 20 1990, p14 (4B): City Scores + State Scores + LISA | 1990 week 8 |
| 1990-10-26_shreveport-times_1990-10-27_p18.txt | Shreveport Times Sat Oct 27 1990, p18 (4B): Area Scores + State Scores + LISA | 1990 week 9 |
| 1990-11-02_shreveport-times_1990-11-03_p23.txt | Shreveport Times Sat Nov 3 1990, p23 (5B): Area Scores (Thu + Fri) + State Scores + LISA | 1990 week 10 |

Re-check: `python3 tools/compare_clip.py <file> <season> <Friday YYYY-MM-DD>`
