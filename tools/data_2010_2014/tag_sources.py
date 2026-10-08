"""Tag CSV rows changed by newspaper_fixes.psv with their source. Usage: tag_sources.py fixes.psv teams.txt names.json csv"""
import csv, json, sys, datetime
fx, teams, names, path = sys.argv[1:5]
N = json.load(open(names)); T = {}
for ln in open(teams):
    if not ln.startswith('#'):
        p = ln.split('|'); T[p[0]] = (N.get(p[1]) or p[2].split(' (')[0])
def nm(x): return x.split('~')[1] if x.startswith('~') else T[x]
F = []
for ln in open(fx):
    if ln.startswith('#') or not ln.strip(): continue
    k, yy, md, a, b, sa, sb, src = ln.rstrip('\n').split('|')
    m, d = map(int, md.split('-'))
    F.append((2000 + int(yy), datetime.date(2000 + int(yy), m, d), {nm(a), nm(b)}, src))
R = list(csv.DictReader(open(path))); n = 0
for r in R:
    mo, da, yr = map(int, r['Date'].split('/'))
    dt = datetime.date(yr, mo, da)
    for y, d0, pair, src in F:
        if int(r['Season']) == y and abs((dt - d0).days) <= 2 and {r['School'], r['Opponent']} == pair:
            r['Source'] = src + ' (MaxPreps had a different/missing score)'; n += 1
w = csv.DictWriter(open(path, 'w', newline=''), fieldnames=list(R[0])); w.writeheader(); w.writerows(R)
print('tagged', n)
