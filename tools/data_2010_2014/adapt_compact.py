import json,re,sys
D=sys.argv[1]
T={}; ST={}; NM={}
YK=["10-11","11-12","12-13","13-14","14-15"]
for ln in open(D+"/teams.txt"):
    if ln.startswith("#"): continue
    p=ln.rstrip("\n").split("|")
    i=int(p[0]); T[i]=p[1]; NM[p[1]]=re.sub(r"\s*\(.*\)\s*$","",p[2])
    for k,v in enumerate(p[3:8]):
        v=v.split('"')[-1].strip()
        ST[f"{p[1]}|{YK[k]}"]=v
NM.update({"baton-rouge/the-church-academy-crusaders":"The Church Academy","jonesville/central-panthers":"Central (Jonesville)"})
G={}; n=0; bad=0
for f in ["g0.txt","h0.txt","h1.txt","h2.txt","h3.txt"]:
    for ln in open(D+"/"+f):
        p=ln.rstrip("\n").split("|")
        if len(p)<6: bad+=1; continue
        try: yy=int(p[0]); ti=int(p[2])
        except: bad+=1; continue
        yk=YK[yy-10]; ds=p[1]+"-20%02d"%yy
        o=p[3]
        if o.startswith("~"):
            q=o.split("~"); on,ost,os_="",q[2] if len(q)>2 else "",q[1]
            # strip doubled first letter artifact (e.g. CCentreville)
            if len(os_)>2 and os_[0]==os_[1] and os_[0].isupper(): os_=os_[1:]
            oslug=""; on=os_
        else:
            try: oslug=T[int(o)]; ost="la"; on=""
            except: bad+=1; continue
        fl=p[6] if len(p)>6 else ""
        n+=1
        G[str(n)]=[[T[ti],yk,ds,oslug,ost,on,p[4],p[5],"d" in fl,"p" in fl]]
# newspaper corrections
import os, datetime
FX=os.environ.get("FIXES")
def res(a,b):
    return ("W" if a>b else "L" if a<b else "T")+f"{max(a,b) if a!=b else a}-{min(a,b) if a!=b else b}"
if FX:
    for ln in open(FX):
        if ln.startswith("#") or not ln.strip(): continue
        k,yy,md,a,b,sa,sb,src=ln.rstrip("\n").split("|"); yy=int(yy); sa=int(sa); sb=int(sb)
        yk=YK[yy-10]; ds=md+"-20%02d"%yy
        def side(x):
            if x.startswith("~"):
                q=x.split("~"); return "",q[2],q[1]
            return T[int(x)],"la",""
        A=side(a); B=side(b)
        d0=datetime.date(2000+yy,int(md.split("-")[0]),int(md.split("-")[1]))
        if k=="FIX":
            hit=0
            for gid,ps in G.items():
                p=ps[0]
                if p[1]!=yk: continue
                pd=p[2].split("-")
                try: dd=datetime.date(int(pd[2]),int(pd[0]),int(pd[1]))
                except: continue
                if abs((dd-d0).days)>2: continue
                if p[0]==A[0] and p[3]==B[0]: p[7]=res(sa,sb); hit+=1
                elif p[0]==B[0] and p[3]==A[0]: p[7]=res(sb,sa); hit+=1
            print("FIX",a,b,"lines changed",hit)
        else:
            n+=1
            if A[0]: G[str(n)]=[[A[0],yk,ds,B[0],B[1],B[2],"",res(sa,sb),False,False]]
            if B[0]:
                n+=1; G[str(n)]=[[B[0],yk,ds,A[0],A[1],A[2],"",res(sb,sa),False,False]]
            print("ADD",a,b)
json.dump({"G":G,"ST":ST,"NM":NM},open(sys.argv[2],"w"))
print(n,bad)
