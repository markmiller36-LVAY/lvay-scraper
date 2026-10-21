<?php
/**
 * LVAY - Volleyball Power Ratings: phone fit + pick-your-winners bracket.
 * - Phones: #, Team, PR, Record and GP fit on screen (no sideways scroll).
 * - Bracket view: tap the arrow next to a team to move it to the next round.
 *   Changing a pick clears that team's later wins. Picks last until the page is left.
 * Only prints on /volleyball/power-ratings/.
 */
add_action('wp_footer', function () {
    $path = trailingslashit(strtolower(wp_parse_url($_SERVER['REQUEST_URI'] ?? '', PHP_URL_PATH) ?: ''));
    if (strpos($path, '/volleyball/power-ratings/') === false && strpos($path, '/volleyball/power-rankings/') === false) {
        return;
    }
    ?>
<style id="lvay-vbpk-css">
/* Phone: fit #, Team, PR, Record, GP on screen (no sideways scroll) */
@media (max-width:767px){
 html body .lvay-vb-rankings.lvay-vb-rankings .lvay-vb-table-scroll{overflow-x:visible !important}
 html body .lvay-vb-rankings.lvay-vb-rankings .lvay-vb-table-scroll table{min-width:0 !important;width:100% !important;table-layout:auto !important}
 html body .lvay-vb-rankings.lvay-vb-rankings table th,html body .lvay-vb-rankings.lvay-vb-rankings table td{padding:7px 5px !important;width:auto !important;min-width:0 !important;max-width:none !important}
 html body .lvay-vb-rankings.lvay-vb-rankings table th:first-child,html body .lvay-vb-rankings.lvay-vb-rankings table td:first-child{width:22px !important;padding-left:6px !important;padding-right:2px !important}
 html body .lvay-vb-rankings.lvay-vb-rankings table .lvay-tcol{white-space:normal !important;width:auto !important;max-width:none !important;line-height:1.1 !important}
 html body .lvay-vb-rankings.lvay-vb-rankings table td:not(.lvay-tcol):not(:first-child),html body .lvay-vb-rankings.lvay-vb-rankings table th:not(.lvay-tcol):not(:first-child){width:1% !important;white-space:nowrap !important}
 html body .lvay-vb-rankings.lvay-vb-rankings table td:last-child,html body .lvay-vb-rankings.lvay-vb-rankings table th:last-child{padding-right:6px !important}
 html body .lvay-vb-rankings td.lvay-tc .lvay-cls::before{margin-left:4px !important}
}
/* Bracket: pick your own winners */
.lvb .lvb-sc{position:relative}
.lvb .lvb-pk{position:absolute;inset:0;width:100%;height:100%;margin:0;padding:0;border:0;background:transparent;color:#008584;font:700 11px/1 Arial,sans-serif;cursor:pointer;display:flex;align-items:center;justify-content:center}
.lvb .lvb-pk:hover,.lvb .lvb-pk:focus{background:#008584;color:#fff;outline:none}
.lvb .lvb-win .lvb-pk{background:#008584;color:#fff}
.lvb .lvb-win .lvb-n,.lvb .lvb-win .lvb-n a{font-weight:700 !important}
.lvb .lvb-lose{opacity:.45}
.lvb .lvb-champ span.lvb-champ-n{display:flex;align-items:center;justify-content:center;font-weight:700;color:#008584}
.lvay-pb-reset{display:inline-flex;align-items:center;margin-left:8px;padding:8px 14px 6px;border:1.5px solid #3a3a3a;border-radius:3px;background:#fff;color:#3a3a3a;font:600 17px/1 Teko,Oswald,"Arial Narrow",sans-serif;letter-spacing:.08em;text-transform:uppercase;cursor:pointer}
.lvay-pb-reset:hover{background:#3a3a3a;color:#fff}
.lvay-pb-pickhint{margin:0 0 6px;font:600 12px/1.3 Raleway,Arial,sans-serif;color:#555}
</style>
<script id="lvay-vbpk-js">
(function(){
 if(window.__lvbPick)return;window.__lvbPick=true;
 var PICKS={};
 function esc(s){return String(s).replace(/[&<>"]/g,function(c){return{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];});}
 function parse(lvb){
  var gs=[].slice.call(lvb.querySelectorAll('.lvb-g')),S=gs.length+1,R=Math.round(Math.log(S)/Math.log(2)),rounds=[],k=0,teams={};
  for(var r=0;r<R;r++){var n=S/Math.pow(2,r+1);rounds.push(gs.slice(k,k+n));k+=n;}
  var base=rounds[0].map(function(g){return [].slice.call(g.querySelectorAll('.lvb-t')).map(function(t){
   if(t.classList.contains('lvb-bye'))return 'bye';var s=t.querySelector('.lvb-s'),nm=t.querySelector('.lvb-n');if(!s||!nm)return null;
   var seed=+s.textContent.trim();if(!seed)return null;var a=nm.querySelector('a');teams[seed]={name:nm.getAttribute('title')||nm.textContent.replace(/\*$/,''),href:a?a.getAttribute('href'):''};return seed;});});
  var sc=lvb.querySelector('.lvb-sc');return {rounds:rounds,base:base,teams:teams,scw:sc?sc.style.width:''};
 }
 function render(lvb){
  var box=lvb.closest('.lvay-pb');if(!box)return;var id=box.id||'pb';var P=PICKS[id]||(PICKS[id]={});
  if(!lvb.__pk){lvb.__pk=parse(lvb);lvb.setAttribute('data-pk','1');addHead(box,id,lvb);}
  var st=lvb.__pk,R=st.rounds.length,occ=[],win=[];
  for(var r=0;r<R;r++){occ[r]=[];win[r]=[];
   for(var i=0;i<st.rounds[r].length;i++){
    var pair=r===0?st.base[i].slice():[win[r-1][2*i],win[r-1][2*i+1]];
    if(r>0)pair.sort(function(x,y){return (x==null?999:x)-(y==null?999:y);});
    occ[r][i]=pair;var a=pair[0],b=pair[1],w=null;
    if(a==='bye')w=b;else if(b==='bye')w=a;else{var p=P[r+'-'+i];if(p!=null&&(p===a||p===b))w=p;else delete P[r+'-'+i];}
    win[r][i]=w==null?null:w;
   }}
  for(r=0;r<R;r++)for(i=0;i<st.rounds[r].length;i++){
   var g=st.rounds[r][i],rows=g.querySelectorAll('.lvb-t'),pr=occ[r][i],both=pr[0]!=null&&pr[1]!=null&&pr[0]!=='bye'&&pr[1]!=='bye';
   for(var j=0;j<2;j++){var row=rows[j],sd=pr[j];if(!row)continue;
    if(r>0){
     if(sd==null){row.innerHTML='<span class="lvb-n"></span><span class="lvb-sc" style="width:'+st.scw+'"></span>';}
     else{var t=st.teams[sd]||{name:''},nm=esc(t.name)+(j===0?'*':'');if(t.href)nm='<a href="'+esc(t.href)+'">'+nm+'</a>';
      row.innerHTML='<span class="lvb-s">'+sd+'</span><span class="lvb-n" title="'+esc(t.name)+'">'+nm+'</span><span class="lvb-sc" style="width:'+st.scw+'"></span>';}
    }
    row.classList.toggle('lvb-win',both&&win[r][i]===sd);row.classList.toggle('lvb-lose',both&&win[r][i]!=null&&win[r][i]!==sd);
    var scEl=row.querySelector('.lvb-sc');if(scEl){scEl.innerHTML=both?'<button type="button" class="lvb-pk" data-box="'+esc(id)+'" data-k="'+r+'-'+i+'" data-seed="'+sd+'" aria-label="Pick '+esc((st.teams[sd]||{}).name||'')+' to win">&#9654;</button>':'';}
   }
  }
  var ch=lvb.querySelector('.lvb-champ'),champ=win[R-1]&&win[R-1][0];
  if(ch){var sp=ch.querySelector('span');if(sp){sp.className=champ?'lvb-champ-n':'';sp.textContent=champ?(st.teams[champ]||{}).name||'':'';}}
 }
 function addHead(box,id,lvb){
  if(box.querySelector('.lvay-pb-reset'))return;var pr=box.querySelector('.lvay-pb-print');
  var b=document.createElement('button');b.type='button';b.className='lvay-pb-reset';b.textContent='Reset picks';b.style.marginLeft='8px';
  b.addEventListener('click',function(){PICKS[id]={};var l=box.querySelector('.lvb');if(l)render(l);});
  if(pr&&pr.parentNode)pr.parentNode.insertBefore(b,pr.nextSibling);
  var h=document.createElement('div');h.className='lvay-pb-pickhint';h.textContent='Tap ▶ next to a team to send them to the next round. Picks clear when you leave the page.';
  var sc=box.querySelector('.lvay-pb-scroll');if(sc)sc.parentNode.insertBefore(h,sc);
 }
 document.addEventListener('click',function(e){
  var bt=e.target.closest&&e.target.closest('.lvb-pk');if(!bt)return;e.preventDefault();e.stopPropagation();
  var id=bt.getAttribute('data-box'),k=bt.getAttribute('data-k'),sd=+bt.getAttribute('data-seed'),P=PICKS[id]||(PICKS[id]={});
  P[k]=(P[k]===sd)?undefined:sd;if(P[k]===undefined)delete P[k];
  var l=bt.closest('.lvb');if(l)render(l);
 },true);
 function scan(){[].forEach.call(document.querySelectorAll('.lvay-pb .lvb:not([data-pk])'),function(l){try{render(l);}catch(err){if(window.console)console.warn('lvb picks',err);}});}
 setInterval(scan,600);
 if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',scan);else scan();
})();
</script>
    <?php
}, 101);
