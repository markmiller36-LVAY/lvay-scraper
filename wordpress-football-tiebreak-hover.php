/**
 * LVAY Football Power Ratings — tiebreaker popup on the * / ** marks (Nov 7, 2026).
 *
 * Hover (desktop) or tap (phone) a * or ** next to a team: shows the LHSAA
 * Handbook 2026-27 seeding tiebreakers (Bylaw 14.11.2 for two teams, 14.11.3
 * for three or more) and highlights the step that decided this tie.
 * The marks come from snippet #34 (span.lvay-tb); ** = head to head, * = SF.
 */
function lvay_fb_tiebreak_hover() {
    if (is_admin()) return;
    $path = isset($_SERVER['REQUEST_URI']) ? strtok($_SERVER['REQUEST_URI'], '?') : '';
    if (strpos($path, '/football/power-ratings') !== 0) return;
    ?>
<style>
.lvay-tb{cursor:help;padding:0 3px;border-radius:4px;text-decoration:underline dotted #008584}
.lvay-tb:hover,.lvay-tb.on{background:#e4f3f3}
#lvayTbPop{position:absolute;z-index:2147482000;display:none;width:min(380px,calc(100vw - 24px));background:#fff;color:#1d2424;
border:1px solid #cfe3e3;border-radius:10px;box-shadow:0 10px 30px rgba(0,0,0,.18);font:400 13px/1.4 Roboto,Arial,sans-serif;overflow:hidden}
#lvayTbPop .h{background:#008584;color:#fff;padding:10px 14px}
#lvayTbPop .h b{display:block;font:600 19px/1.1 Teko,Arial,sans-serif;letter-spacing:.03em;text-transform:uppercase}
#lvayTbPop .h span{font-size:12px;opacity:.9}
#lvayTbPop .t{padding:8px 14px 2px;font-size:12px;color:#5a6464}
#lvayTbPop ol{list-style:none;margin:0;padding:6px 10px 10px}
#lvayTbPop li{padding:7px 8px;border-radius:7px;color:#8a9494;display:flex;gap:8px}
#lvayTbPop li i{font-style:normal;font-weight:700;min-width:20px}
#lvayTbPop li.use{background:#e4f3f3;color:#0d3b3a;border-left:4px solid #008584;font-weight:600}
#lvayTbPop li.use em{display:block;font-style:normal;font-weight:400;color:#2b5756;margin-top:2px}
#lvayTbPop .f{border-top:1px solid #eef3f3;padding:7px 14px;font-size:11px;color:#7a8484}
#lvayTbPop .x{position:absolute;top:6px;right:8px;background:none;border:0;color:#fff;font-size:20px;line-height:1;cursor:pointer;display:none}
@media (hover:none){#lvayTbPop .x{display:block}}
</style>
<script>
(function(){
  var pop=null, openFor=null, hideT=null;
  var TWO=[
    ['1','Head to head','If the two schools played each other, the winner is seeded higher.','h2h'],
    ['2','Strength factor','If they did not play, the higher strength factor (Bylaw 14.13.1) is seeded higher.','sf'],
    ['3a','Record vs. same class','Best won-lost % against teams in the school’s same classification.','rec'],
    ['3b','District record','Best won-lost % in district play.','rec'],
    ['3c','Overall record','Best won-lost % against all teams played.','rec']];
  var MANY=[
    ['1','Strength factor','The higher strength factor (Bylaw 14.13) is seeded higher.','sf'],
    ['2','Head to head','If two teams are still tied on strength factor and played each other, the winner is seeded higher.','h2h'],
    ['3a','Record vs. same class','Best won-lost % against teams in the school’s same classification.','rec'],
    ['3b','District record','Best won-lost % in district play.','rec'],
    ['3c','Overall record','Best won-lost % against all teams played.','rec']];
  function cells(tr){return tr?tr.querySelectorAll('td'):[];}
  function col(tr,label,fallback){
    var t=tr.closest('table'), ths=t?t.querySelectorAll('thead th'):[], i;
    for(i=0;i<ths.length;i++){ if(ths[i].textContent.trim().toUpperCase()===label) return i; }
    return fallback;
  }
  function info(tr){
    var c=cells(tr), a=tr.querySelector('a');
    var txt=function(i){return ((c[i]||{}).textContent||'').trim();};
    return {tr:tr, rank:parseInt(txt(col(tr,'#',0)),10)||0, name:a?a.textContent.trim():'',
      pr:txt(col(tr,'PR',5)), sf:parseFloat(txt(col(tr,'SF',6)))||0};
  }
  function build(mark){
    var me=info(mark.closest('tr')), tbody=mark.closest('tbody'); if(!tbody) return '';
    var tied=[].map.call(tbody.querySelectorAll('tr'),info).filter(function(t){return t.pr===me.pr && t.tr.querySelector('.lvay-tb');});
    var method=mark.textContent.trim()==='**'?'h2h':'sf', many=tied.length>2, list=many?MANY:TWO;
    var others=tied.filter(function(t){return t.tr!==me.tr;});
    var used=null; list.forEach(function(s){ if(!used && s[3]===method) used=s[0]; });
    var detail='';
    if(method==='h2h'){
      var other=others[0]; if(many){ others.forEach(function(t){ if(t.sf===me.sf) other=t; }); }
      var w=me.rank<other.rank?me:other, l=w===me?other:me;
      detail=w.name+' beat '+l.name+', so '+w.name+' is seeded higher (#'+w.rank+' vs. #'+l.rank+').';
    } else {
      detail=tied.slice().sort(function(a,b){return a.rank-b.rank;}).map(function(t){return t.name+' '+t.sf.toFixed(2);}).join(' • ')+
        (many?'':(' — they did not play each other.'));
    }
    var h='<button class="x" aria-label="Close">×</button><div class="h"><b>Tiebreaker: '+(method==='h2h'?'Head to head':'Strength factor')+'</b>'+
      '<span>Tied at '+me.pr+' with '+others.map(function(t){return t.name+' (#'+t.rank+')';}).join(', ')+'</span></div>'+
      '<div class="t">LHSAA Handbook, Bylaw '+(many?'14.11.3 — three or more teams tied':'14.11.2 — two teams tied')+'</div><ol>';
    list.forEach(function(s){
      var on=s[0]===used;
      h+='<li class="'+(on?'use':'')+'"><i>'+s[0]+'.</i><div><b>'+s[1]+'</b> — '+s[2]+(on?'<em>✔ '+detail+'</em>':'')+'</div></li>';
    });
    return h+'</ol><div class="f">Seeds go by power rating; these steps only break identical ratings.</div>';
  }
  function place(mark){
    var r=mark.getBoundingClientRect(), w=pop.offsetWidth, h=pop.offsetHeight;
    var left=Math.max(12, Math.min(window.scrollX+r.left-20, window.scrollX+document.documentElement.clientWidth-w-12));
    var top=window.scrollY+r.bottom+8; if(r.bottom+h+16>window.innerHeight && r.top>h+16) top=window.scrollY+r.top-h-8;
    pop.style.left=left+'px'; pop.style.top=top+'px';
  }
  function show(mark){
    clearTimeout(hideT);
    if(!pop){pop=document.createElement('div');pop.id='lvayTbPop';pop.setAttribute('role','dialog');document.body.appendChild(pop);
      pop.addEventListener('mouseenter',function(){clearTimeout(hideT);}); pop.addEventListener('mouseleave',later);
      pop.addEventListener('click',function(e){ if(e.target.classList.contains('x')) hide(); });}
    if(openFor) openFor.classList.remove('on');
    pop.innerHTML=build(mark); pop.style.display='block'; place(mark); openFor=mark; mark.classList.add('on');
  }
  function hide(){ if(pop) pop.style.display='none'; if(openFor) openFor.classList.remove('on'); openFor=null; }
  function later(){ clearTimeout(hideT); hideT=setTimeout(hide,180); }
  function arm(){
    document.querySelectorAll('.lvay-tb').forEach(function(m){
      if(m.dataset.tbArmed) return; m.dataset.tbArmed='1'; m.removeAttribute('title'); m.setAttribute('tabindex','0'); m.setAttribute('role','button');
      m.addEventListener('mouseenter',function(){ if(matchMedia('(hover:hover)').matches) show(m); });
      m.addEventListener('mouseleave',function(){ if(matchMedia('(hover:hover)').matches) later(); });
      m.addEventListener('click',function(e){ e.preventDefault(); e.stopPropagation(); if(openFor===m) hide(); else show(m); });
      m.addEventListener('keydown',function(e){ if(e.key==='Enter'||e.key===' '){ e.preventDefault(); show(m);} if(e.key==='Escape') hide(); });
    });
  }
  document.addEventListener('click',function(e){ if(pop && openFor && !pop.contains(e.target)) hide(); });
  document.addEventListener('keydown',function(e){ if(e.key==='Escape') hide(); });
  window.addEventListener('resize',function(){ if(openFor) place(openFor); });
  if(document.readyState!=='loading') arm(); else document.addEventListener('DOMContentLoaded',arm);
  setTimeout(arm,1500);
})();
</script>
    <?php
}
add_action('wp_footer', 'lvay_fb_tiebreak_hover', 99);
