<?php
/**
 * LVAY - Volleyball Power Ratings: match the football page look (current season).
 * Fonts, sizes, spacing, division headers, teal table header, search box and
 * "Current Season" label copied from /football/power-ratings/ (no split view).
 * Only prints on /volleyball/power-ratings/.
 */
add_action('wp_footer', function () {
    $path = trailingslashit(strtolower(wp_parse_url($_SERVER['REQUEST_URI'] ?? '', PHP_URL_PATH) ?: ''));
    if (strpos($path, '/volleyball/power-ratings/') === false && strpos($path, '/volleyball/power-rankings/') === false) {
        return;
    }
    ?>
<style id="lvay-vbfb-css">
/* Volleyball power ratings: match the football current-season look */
html body .lvay-vb-rankings > details{background:#fff !important;border:0 !important;margin:0 !important;border-radius:0 !important}
html body .lvay-vb-rankings > details:not(.lvay-allacc) > summary{display:flex !important;align-items:center !important;gap:7px !important;min-height:0 !important;padding:14px 4px 10px !important;background:#fff !important;color:#090909 !important;border:0 !important;border-bottom:2px solid #078b88 !important;border-radius:0 !important;font-family:"Alfa Slab One",Rockwell,Georgia,serif !important;font-size:24px !important;font-weight:400 !important;line-height:1.1 !important;cursor:pointer;list-style:none}
html body .lvay-vb-rankings > details:not(.lvay-allacc) > summary:hover{background:#f5fbfb !important;color:#078b88 !important}
html body .lvay-vb-rankings > details > summary::-webkit-details-marker{display:none}
html body .lvay-vb-rankings > details:not(.lvay-allacc) > summary > i{display:inline-block !important;flex:0 0 22px !important;width:22px !important;text-align:center;font-style:normal !important;font-family:Arial,sans-serif !important;font-size:32px !important;font-weight:400 !important;line-height:.7 !important;color:#5dc7c1 !important;-webkit-text-stroke:0 !important;text-shadow:none !important;transform:rotate(0deg);transition:transform .15s}
html body .lvay-vb-rankings > details[open] > summary > i{transform:rotate(90deg)}
html body .lvay-vb-rankings > details > summary .lvay-pb-jump{font-size:10.5px !important;padding:5px 7px !important;margin-left:10px !important}
html body .lvay-vb-rankings > details > .lvay-shr-bar,html body .lvay-vb-rankings > details > .lvay-vb-table-scroll,html body .lvay-vb-rankings > details > .lvay-np-wrap,html body .lvay-vb-rankings > details > .lvay-pb{margin-left:9px !important;margin-right:9px !important}
html body .lvay-vb-rankings .lvay-vb-table-scroll{background:#fff !important;margin-bottom:12px !important}
html body .lvay-vb-rankings table{width:100% !important;border-collapse:collapse !important;font-family:Teko,sans-serif !important;font-size:18px !important;color:#080808 !important}
html body .lvay-vb-rankings table th{padding:8px 10px !important;background:#078b88 !important;color:#fff !important;font-family:Teko,sans-serif !important;font-size:17px !important;font-weight:600 !important;letter-spacing:.25px !important;text-align:left !important;white-space:nowrap !important;vertical-align:middle !important;border:0 !important}
html body .lvay-vb-rankings table td{padding:7px 10px !important;background:#fff !important;color:#080808 !important;font-family:Teko,sans-serif !important;font-size:18px !important;font-weight:500 !important;line-height:1.05 !important;white-space:nowrap !important;vertical-align:middle !important;border:0 !important;border-bottom:1px solid #e6e6e6 !important}
html body .lvay-vb-rankings table tr:hover td{background:#d9eeee !important}
html body .lvay-vb-rankings table td a{color:#080808 !important;font-family:Teko,sans-serif !important;font-weight:600 !important;font-size:18px !important;text-decoration:none !important}
html body .lvay-vb-rankings table td a:hover{color:#078b88 !important;text-decoration:underline !important}
html body .lvay-vb-rankings table tbody tr.lvay-cut > td{border-bottom:2px dashed #b5432b !important}
html body .lvay-vb-rankings .lvay-np-tbl tbody td{background:#fafafa !important}
html body .lvay-vb-rankings .lvay-np-tbl td:first-child{color:#9aa3a3 !important}
html body .lvay-vb-rankings td.lvay-dcol,html body .lvay-vb-rankings th.lvay-dcol{font-size:15px !important;color:#555 !important}
html body .lvay-vb-rankings th.lvay-dcol{color:#fff !important}
.lvay-vb-ots{display:flex;flex-wrap:wrap;align-items:baseline;gap:4px 10px;margin:4px 0 6px;padding:0 2px;font:13px/1.35 Arial,sans-serif;color:#666}
.lvay-vb-ots b{font:700 12px/1 Arial,sans-serif;letter-spacing:.12em;text-transform:uppercase;color:#111;padding:5px 8px;background:#e3f3f3;border-left:3px solid #008584}
.lvay-vb-search{box-sizing:border-box !important;width:100% !important;height:50px !important;margin:0 0 26px !important;padding:10px 15px !important;border:2px solid #078b88 !important;border-radius:4px !important;background:#fff !important;color:#080808 !important;font-family:Teko,Arial,sans-serif !important;font-size:19px !important;outline:none}
.lvay-vb-search::placeholder{color:#a0a0a0 !important;opacity:1}
@media (max-width:767px){
 html body .lvay-vb-rankings > details:not(.lvay-allacc) > summary{font-size:23px !important;padding:12px 4px 9px !important}
 html body .lvay-vb-rankings table td,html body .lvay-vb-rankings table td a{font-size:15px !important}
 html body .lvay-vb-rankings table th{font-size:15px !important}
}
.lvay-prs-wrap{position:relative;margin:0 0 26px}
html body .lvay-prs-wrap .lvay-vb-search{margin:0 !important}
.lvay-prs-list{position:absolute;left:0;right:0;top:100%;z-index:9999;background:#fff;border:2px solid #078b88;border-top:0;max-height:320px;overflow:auto;display:none;box-shadow:0 8px 18px rgba(0,0,0,.15)}
.lvay-prs-list.on{display:block}
.lvay-prs-item{display:flex;justify-content:space-between;gap:10px;padding:8px 14px;cursor:pointer;font-family:Teko,Arial,sans-serif;font-size:19px;line-height:1.1;color:#111;border-bottom:1px solid #eee}
.lvay-prs-item small{color:#078b88;font-size:16px;white-space:nowrap}
.lvay-prs-item:hover,.lvay-prs-item.act{background:#e6f4f3}
.lvay-prs-none{padding:8px 14px;font-family:Teko,Arial,sans-serif;font-size:18px;color:#777}
html body .lvay-vb-rankings.lvay-vb-rankings.lvay-vb-rankings table tbody tr.lvay-prs-hit > td{background:#fff3b0 !important}
html body .lvay-vb-rankings.lvay-vb-rankings.lvay-vb-rankings table tbody tr.lvay-prs-hit > td:first-child{box-shadow:inset 4px 0 0 #078b88}
</style>
<script id="lvay-vbfb-js">
(function(){
 if(window.__lvayVbFb)return;window.__lvayVbFb=true;
 function go(){
  var root=document.querySelector('.lvay-vb-rankings');if(!root||document.querySelector('.lvay-vb-ots'))return;
  var m=location.search.match(/[?&]season=(\d{4})/),t=document.querySelector('.lvay-vb-title h1'),ty=t&&(t.textContent.match(/\d{4}/)||[])[0],yr=m?m[1]:(ty||String(new Date().getFullYear()));
  if(t){t.innerHTML=t.innerHTML.replace(/POWER RANKINGS/i,'POWER RATINGS');}
  var lab=document.createElement('div');lab.className='lvay-vb-ots';
  lab.innerHTML='<b>'+(m?yr+' Season':'Current '+yr+' Season')+'</b><span>Unofficial '+yr+' power ratings — LHSAA Divisions I–V.</span>';
  var s=document.createElement('input');s.type='search';s.className='lvay-vb-search';s.placeholder='Search for a school...';s.setAttribute('aria-label','Search for a school');
  root.parentNode.insertBefore(s,root);root.parentNode.insertBefore(lab,root);
  /* search: dropdown of schools; picking one opens that team's division (same as football #191) */
  s.setAttribute('autocomplete','off');
  var wrap=document.createElement('div');wrap.className='lvay-prs-wrap';s.parentNode.insertBefore(wrap,s);wrap.appendChild(s);
  var list=document.createElement('div');list.className='lvay-prs-list';list.setAttribute('role','listbox');wrap.appendChild(list);
  function divs(){return [].slice.call(root.querySelectorAll(':scope > details:not(.lvay-allacc)'));}
  function divName(d){var sm=d.querySelector('summary');if(!sm)return '';var t=sm.cloneNode(true);[].forEach.call(t.querySelectorAll('a,button,i'),function(x){x.remove();});return t.textContent.replace(/\s+/g,' ').trim();}
  function teamOf(r){var c=r.cells&&r.cells[1];return c?c.textContent.replace(/\s+/g,' ').trim():'';}
  var idx=null,cur=[],act=-1;
  function build(){idx=[];var seen={};divs().forEach(function(d){var dn=divName(d);[].forEach.call(d.querySelectorAll('table tbody tr'),function(r){if(r.closest('.lvay-pb'))return;var t=teamOf(r);if(!t||seen[t])return;seen[t]=1;idx.push({t:t,d:dn,g:d,r:r});});});idx.sort(function(a,b){return a.t.localeCompare(b.t);});}
  function show(){
   if(!idx||!idx.length)build();var q=s.value.toLowerCase().trim();act=-1;
   if(!q){list.classList.remove('on');list.innerHTML='';return;}
   cur=idx.filter(function(x){return x.t.toLowerCase().indexOf(q)!==-1;}).sort(function(a,b){return (a.t.toLowerCase().indexOf(q)===0?0:1)-(b.t.toLowerCase().indexOf(q)===0?0:1);}).slice(0,12);
   list.innerHTML=cur.length?cur.map(function(x,i){return '<div class="lvay-prs-item" role="option" data-i="'+i+'"><span>'+x.t.replace(/</g,'&lt;')+'</span><small>'+x.d.replace(/</g,'&lt;')+'</small></div>';}).join(''):'<div class="lvay-prs-none">No school found</div>';
   list.classList.add('on');
  }
  function pick(x){
   list.classList.remove('on');s.value=x.t;
   [].forEach.call(root.querySelectorAll('tbody tr'),function(r){r.style.display='';r.classList.remove('lvay-prs-hit');});
   [].forEach.call(root.querySelectorAll(':scope > details'),function(d){d.style.display='';if(d!==x.g)d.open=false;});
   x.g.open=true;x.r.classList.add('lvay-prs-hit');
   setTimeout(function(){var go=function(){return x.r.getBoundingClientRect().top+window.pageYOffset-window.innerHeight/3;};window.scrollTo({top:go(),behavior:'smooth'});setTimeout(function(){if(Math.abs(x.r.getBoundingClientRect().top-window.innerHeight/3)>40)window.scrollTo({top:go(),behavior:'instant'});},700);},80);
  }
  s.addEventListener('input',function(){idx=null;show();});
  s.addEventListener('focus',function(){if(s.value)show();});
  s.addEventListener('keydown',function(e){
   if(!list.classList.contains('on'))return;var its=list.querySelectorAll('.lvay-prs-item');
   if(e.key==='ArrowDown'||e.key==='ArrowUp'){e.preventDefault();if(!its.length)return;act=(act+(e.key==='ArrowDown'?1:-1)+its.length)%its.length;[].forEach.call(its,function(n,i){n.classList.toggle('act',i===act);});its[act].scrollIntoView({block:'nearest'});}
   else if(e.key==='Enter'){e.preventDefault();if(cur.length)pick(cur[act<0?0:act]);}
   else if(e.key==='Escape'){list.classList.remove('on');}
  });
  list.addEventListener('mousedown',function(e){var it=e.target.closest('.lvay-prs-item');if(!it)return;e.preventDefault();pick(cur[+it.getAttribute('data-i')]);});
  document.addEventListener('click',function(e){if(!wrap.contains(e.target))list.classList.remove('on');});
 }
 if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',go);else go();
})();
</script>
    <?php
}, 100);
