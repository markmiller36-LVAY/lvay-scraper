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
json.dump({"G":G,"ST":ST,"NM":NM},open(sys.argv[2],"w"))
print(n,bad)
