"""Compare a transcribed clipping (one 'Team A 21, Team B 14' per line) with HSFHA games.
Usage: compare_clip.py <clip.txt> <season> <game date YYYY-MM-DD>"""
import csv, re, sys, datetime as D
sys.path.insert(0, __import__('os').path.dirname(__import__('os').path.abspath(__file__)))
import io, contextlib
from build_football_from_hsfha import apply_fixes
clip, season, gdate = sys.argv[1], sys.argv[2], D.date.fromisoformat(sys.argv[3])
src = __import__('os').path.dirname(__import__('os').path.abspath(__file__))+'/data_1980_1999/hsfha_la_football_1980_1999_dedup.tsv' if int(season) < 2000 else __import__('os').path.dirname(__import__('os').path.abspath(__file__))+'/data_2000_2009/hsfha_la_football_2000_2009_dedup.tsv'
R = list(csv.DictReader(open(src, encoding='utf-8'), delimiter='\t'))
with contextlib.redirect_stdout(io.StringIO()): apply_fixes(R)
R = [x for x in R if x['season'] == season]
AB = {'n': 'north', 's': 'south', 'e': 'east', 'w': 'west', 'c': 'central', 'capt': 'captain', 'bro': 'brother',
      'cath': 'catholic', 'christ': 'christian', 'bapt': 'baptist', 'wash': 'washington', 'acad': 'academy',
      'cent': 'central', 'priv': 'private', 'plaq': 'plaquemines', 'hgts': 'heights', 'btw': 'booker t washington',
      'lc': 'lake charles', 'sullivan': 'sullivan', 'king': 'king', 'jewell': 'jewel'}
STOP = {'high', 'school', 'the', 'br', 'pc', 'ni', 'no', 'sv', 'shreveport', 'miss', 'texas', 'tex', 'ark', 'fla', 'la', 'academy', 'of', 'cent', 'prep'}
def tok(n):
    n = re.sub(r'\(.*?\)', ' ', n.lower()).replace('.', ' ').replace("'", '').replace(',', ' ')
    w = []
    for t in re.split(r'[\s\-/]+', n):
        if not t: continue
        t = AB.get(t, t)
        w += t.split()
    return set(w) - STOP
def sim(a, b):
    A, B = tok(a), tok(b)
    return len(A & B) / max(1, min(len(A), len(B))) if A and B else 0
out = {'same': [], 'diff': [], 'date': [], 'new': [], 'unplayed': []}
for line in open(clip):
    m = re.match(r'^(.+?) (\d+), (.+?) (\d+)', line.strip())
    if not m: continue
    a, sa, b, sb = m.group(1), m.group(2), m.group(3), m.group(4)
    best, bs = None, 0
    for x in R:
        for (t, o, ts, os_) in ((x['team'], x['opp'], x['team_score'], x['opp_score']), (x['opp'], x['team'], x['opp_score'], x['team_score'])):
            s = min(sim(a, t), sim(b, o))
            if s >= 0.99 or (s >= 0.5 and best is None):
                d = abs((D.date.fromisoformat(x['date']) - gdate).days) if x['date'] else 99
                score = s - d / 1000
                if score > bs: best, bs = (x, t, o, ts, os_, d), score
    if not best:
        out['new'].append(line.strip()); continue
    x, t, o, ts, os_, d = best
    desc = f"{line.strip():42s} | on file: {x['date']} {t} {ts or '?'}, {o} {os_ or '?'}  (id {x['id']})"
    if x['played'] != '1': out['unplayed'].append(desc)
    elif d > 3: out['date'].append(desc)
    elif (ts, os_) != (sa, sb): out['diff'].append(desc)
    else: out['same'].append(desc)
print({k: len(v) for k, v in out.items()})
for k in ('diff', 'date', 'unplayed', 'new'):
    if out[k]: print(f"\n== {k.upper()} ==\n" + '\n'.join(out[k]))
