<?php
/**
 * LVAY – Volleyball Power Ratings: playoff cutoff + "Schools not playing in the playoffs"
 *
 * WPCode: PHP snippet, Auto Insert, Run Everywhere (it only prints on the
 * volleyball power ratings page).
 *
 * Matches the football page:
 *  - Schools LHSAA lists as not playing in the playoff are pulled out of the
 *    division table, the rest are renumbered 1,2,3…, and the pulled schools are
 *    shown underneath in a "Schools not playing in the playoffs" table (#  = —).
 *  - The 32nd eligible school gets the dashed "Playoff cutoff" line
 *    (LHSAA Bylaw 24.6.1: 32-team bracket in every volleyball division).
 *  - The existing "Bracket view" is rebuilt from the corrected order, so the
 *    projected bracket never seeds a school that isn't playing.
 *
 * The list comes from https://lvay-scraper.onrender.com/api/rankings/volleyball
 * (playoff_eligible on each row), which the scraper refreshes from LHSAA's
 * volleyball power rating report every pipeline run.
 */
add_action('wp_footer', function () {
    $path = trailingslashit(strtolower(wp_parse_url($_SERVER['REQUEST_URI'] ?? '', PHP_URL_PATH) ?: ''));
    if (strpos($path, '/volleyball/power-ratings/') === false && strpos($path, '/volleyball/power-rankings/') === false) {
        return;
    }
    ?>
<style id="lvay-vb-cut-css">
.lvay-vb-rankings table tbody tr.lvay-cut > td{border-bottom:2px dashed #b5432b !important}
.lvay-vb-rankings tr.lvay-cut > td.lvay-cut-tag{position:relative}
.lvay-vb-rankings tr.lvay-cut > td.lvay-cut-tag::after{content:"Playoff cutoff";position:absolute;right:6px;bottom:-7px;background:#fff;color:#b5432b;font:700 9px/12px Raleway,sans-serif;letter-spacing:.06em;text-transform:uppercase;padding:0 6px;border:1px solid #b5432b;border-radius:999px;white-space:nowrap;z-index:2}
.lvay-vb-rankings .lvay-np-hd{margin:18px 0 6px;font-weight:700;font-size:14px;letter-spacing:.04em;text-transform:uppercase;color:#6b7474}
.lvay-vb-rankings .lvay-np-tbl td:first-child{color:#9aa3a3}
.lvay-vb-rankings .lvay-np-tbl tbody td{background:#fafafa !important}
</style>
<script id="lvay-vb-cut-js">
(function(){
 if(window.__lvayVbCut)return;window.__lvayVbCut=true;
 var API='https://lvay-scraper.onrender.com/api/rankings/volleyball',FIELD=32;
 function key(s){return String(s||'').toLowerCase().replace(/[^a-z0-9]/g,'');}
 function seasonParam(){var m=location.search.match(/[?&]season=(\d{4})/);return m?m[1]:'';}
 function tagCell(tb,row){var h=tb.tHead&&tb.tHead.rows[0],ci=-1;
  if(h)for(var x=0;x<h.cells.length;x++){if(/^class$/i.test(h.cells[x].textContent.trim())){ci=x;break;}}
  return row.cells[ci>0?ci-1:1]||row.cells[row.cells.length-1];}

 function apply(notPlaying,field){
  var root=document.querySelector('.lvay-vb-rankings');if(!root)return;
  var divs=root.querySelectorAll(':scope > details');
  for(var d=0;d<divs.length;d++){
   var det=divs[d],tbl=det.querySelector('table');if(!tbl||!tbl.tBodies[0])continue;
   var tb=tbl.tBodies[0],moved=[];
   [].slice.call(tb.rows).forEach(function(r){
    var a=r.cells[1];if(!a)return;
    if(notPlaying[key(a.textContent.trim())]){moved.push(r);}
   });
   /* not-playing table under the division, built once */
   var wrap=det.querySelector('.lvay-np-wrap');
   if(moved.length){
    if(!wrap){wrap=document.createElement('div');wrap.className='lvay-np-wrap';
     wrap.innerHTML='<div class="lvay-np-hd">Schools not playing in the playoffs</div><div class="lvay-vb-table-scroll"><table class="lvay-np-tbl"><thead>'+tbl.tHead.innerHTML+'</thead><tbody></tbody></table></div>';
     var scroller=tbl.closest('.lvay-vb-table-scroll')||tbl;scroller.parentNode.insertBefore(wrap,scroller.nextSibling);}
    var ntb=wrap.querySelector('tbody');
    moved.forEach(function(r){r.cells[0].textContent='—';r.classList.remove('lvay-cut');ntb.appendChild(r);});
   }
   /* renumber the eligible schools and draw the cutoff after #FIELD */
   var n=0,rows=[].slice.call(tb.rows),total=rows.length;
   rows.forEach(function(r){n++;r.cells[0].textContent=n;
    var on=(n===field&&total>field);r.classList.toggle('lvay-cut',on);r.classList.toggle('lvay-below',n>field);
    var tag=on?tagCell(tbl,r):null;for(var k=0;k<r.cells.length;k++)r.cells[k].classList.toggle('lvay-cut-tag',r.cells[k]===tag);});
  }
  /* the bracket script rebuilds on resize; give it the corrected tables */
  try{window.dispatchEvent(new Event('resize'));}catch(e){}
 }

 function run(){
  var s=seasonParam(),url=API+(s?'?season='+s:'');
  fetch(url,{credentials:'omit'}).then(function(r){return r.json();}).then(function(data){
   var np={};
   /* archive seasons: the feed is the current season, so only draw the cutoff */
   if(!s||String(data.season)===s)(data.rankings||[]).forEach(function(r){if(r.playoff_eligible===false)np[key(r.school)]=1;});
   apply(np,data.playoff_field||FIELD);
  }).catch(function(){apply({},FIELD);});
 }
 window.__lvayVbCutApply=apply;
 if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',run);else run();
})();
</script>
    <?php
}, 99);
