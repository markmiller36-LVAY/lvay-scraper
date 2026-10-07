import gzip,json,sys,collections,re
AL={'landrywalker':'lordbeaconsfieldlandry'}
def N(x):
    x=re.sub('[^a-z]','',x.lower()); return AL.get(x,x)
SP='tools/'
G=[l.rstrip('\n').split('|') for l in open(SP+'br2020.psv') if l.strip()]
pair={}
for b,r,t1,s1,h1,t2,s2,h2 in G:
    pair[frozenset([N(t1),N(t2)])]=(r,b)
path='/home/claude/lvay-scraper/football_archives_greenlit.json.gz'
d=json.load(gzip.open(path,'rt'))
S=d['seasons']['2020']['schools']
ORD={'1':1,'Regional':2,'Quarterfinal':3,'Semifinal':4,'State Championship':5}
hit=0;miss=[]
for s in S:
    for g in s['games']:
        if g['phase'].startswith('Regular'): continue
        k=frozenset([N(s['school']),N(g['opponent'])])
        if k in pair: g['week']=pair[k][0]; hit+=1
        else: g['week']='1'; miss.append((s['school'],g['opponent'],g['score']))
    reg=[g for g in s['games'] if g['phase'].startswith('Regular')]
    po=sorted([g for g in s['games'] if not g['phase'].startswith('Regular')],key=lambda g:ORD.get(str(g['week']),9))
    s['games']=reg+po
print('matched',hit,'unmatched',len(miss)); print(miss[:40])
if len(sys.argv)>1 and sys.argv[1]=='write':
    json.dump(d,gzip.open(path,'wt'),separators=(',',':')); print('written')
