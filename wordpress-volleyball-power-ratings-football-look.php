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
  var was=null;
  s.addEventListener('input',function(){
   var q=s.value.trim().toLowerCase(),ds=root.querySelectorAll(':scope > details');
   if(q&&!was){was=[].map.call(ds,function(d){return d.open;});}
   ds.forEach(function(d,i){
    var hit=0;d.querySelectorAll('table tbody tr').forEach(function(r){var c=r.cells[1];if(!c)return;var ok=!q||c.textContent.toLowerCase().indexOf(q)>-1;r.style.display=ok?'':'none';if(ok)hit++;});
    if(q){d.style.display=hit?'':'none';if(hit&&!d.classList.contains('lvay-allacc'))d.open=true;}
    else{d.style.display='';if(was)d.open=was[i];}
   });
   if(!q)was=null;
  });
 }
 if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',go);else go();
})();
</script>
    <?php
}, 100);
