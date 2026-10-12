import csv,gzip,json,collections,datetime as dt,sys
WRITE=len(sys.argv)>1
P='football_archives_greenlit.json.gz'
d=json.load(gzip.open(P,'rt',encoding='utf-8'))
def pd(s):
    m,dd,y=s.split('/'); return dt.date(int(y),int(m),int(dd))
flag=set()
for f in ['tools/data_1980_1999/hsfha_la_football_1980_1999.tsv','tools/data_2000_2009/hsfha_la_football_2000_2009.tsv']:
    for r in csv.DictReader(open(f,encoding='utf-8'),delimiter='\t'):
        if '?' in (r.get('notes') or '') and r.get('date','').count('-')==2:
            y,m,dd=r['date'].split('-'); flag.add((r['season'],f"{int(m)}/{int(dd)}/{y}",tuple(sorted([r['team_score'],r['opp_score']]))))
decide={}; log=[]
for y in sorted(d['seasons']):
    if not ('1990'<=y<='2009'): continue
    for sc in d['seasons'][y]['schools']:
        grp=collections.defaultdict(list)
        for g in sc['games']:
            if g.get('score') and g.get('phase')=='Regular Season':
                grp[(g.get('opponent'),g.get('score'))].append(g)
        for (o,s),gs in grp.items():
            if len(gs)<2: continue
            key=(y,tuple(sorted([sc['school'],o])),tuple(sorted(s.split('-'))))
            if key in decide: continue
            dates=[g['game_date'] for g in gs]
            others=[pd(g['game_date']) for g in sc['games'] if g not in gs and g.get('game_date')]
            clash=[x for x in dates if any(abs((pd(x)-o2).days)<=3 for o2 in others)]
            fl=[x for x in dates if (y,x,key[2]) in flag]
            if len(clash)==1: drop=clash[0];why='clash'
            elif len(fl)==1: drop=fl[0];why='hsfha-flag'
            elif sum(1 for x in dates if pd(x).month==11 and pd(x).day>=12)==1: drop=[x for x in dates if pd(x).month==11 and pd(x).day>=12][0];why='playoff-week'
            else: drop=min(dates,key=pd);why='earlier'
            decide[key]=drop; log.append((y,sc['school'],o,s,dates,'drop',drop,why))
for l in log: print(*l)
n=0
for y in d['seasons']:
    for sc in d['seasons'][y]['schools']:
        keep=[]
        for g in sc['games']:
            k=(y,tuple(sorted([sc['school'],g.get('opponent')])),tuple(sorted(str(g.get('score')).split('-'))))
            if g.get('phase')=='Regular Season' and k in decide and g.get('game_date')==decide[k] and sum(1 for h in sc['games'] if h.get('opponent')==g.get('opponent') and h.get('score')==g.get('score') and h.get('phase')=='Regular Season')>1:
                n+=1; continue
            keep.append(g)
        if len(keep)!=len(sc['games']):
            sc['games']=keep
            w=sum(1 for g in keep if str(g.get('result')).upper().startswith('W'));l=sum(1 for g in keep if str(g.get('result')).upper().startswith('L'));t=sum(1 for g in keep if str(g.get('result')).upper().startswith('T'))
            sc.update(wins=w,losses=l,ties=t,games_played=w+l+t,record=f"{w}-{l}"+(f"-{t}" if t else ''))
    if 'game_count' in d['seasons'][y]: d['seasons'][y]['game_count']=sum(len(sc['games']) for sc in d['seasons'][y]['schools'])
print('rows removed',n,'games',len(decide))
if WRITE:
    with gzip.open(P,'wt',encoding='utf-8') as f: json.dump(d,f,separators=(',',':'))
