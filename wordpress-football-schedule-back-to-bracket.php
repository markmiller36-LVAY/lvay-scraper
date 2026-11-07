/**
 * LVAY Football Schedules — "Back to Bracket" button (Nov 7, 2026).
 *
 * A fan's friend reported: on a phone, tapping a team in a bracket opens the
 * team's schedule, with no way back to the bracket. When the schedule page was
 * opened from a bracket on this site (Power Ratings "Bracket View", Football
 * Playoff Brackets, Playoff HQ), show a floating teal "Back to Bracket" pill.
 * It uses the browser's Back, so the fan lands at the same spot in the bracket.
 *
 * Install: Code Snippets, "Run snippet everywhere" (front end only).
 */
function lvay_fb_back_to_bracket() {
    if (is_admin()) return;
    $path = isset($_SERVER['REQUEST_URI']) ? strtok($_SERVER['REQUEST_URI'], '?') : '';
    if (strpos($path, '/football/schedules') !== 0) return;
    ?>
<style>
.lvay-b2b{position:fixed;left:12px;bottom:calc(16px + env(safe-area-inset-bottom));z-index:99990;
display:none;align-items:center;gap:6px;background:#008584;color:#fff!important;border:0;
border-radius:999px;padding:10px 16px;font:600 15px/1 Teko,Arial,sans-serif;letter-spacing:.04em;
text-transform:uppercase;box-shadow:0 4px 14px rgba(0,0,0,.25);text-decoration:none!important;cursor:pointer}
.lvay-b2b.on{display:inline-flex}
.lvay-b2b:hover{background:#006f6e}
@media (min-width:1025px){.lvay-b2b{bottom:24px;left:24px}}
</style>
<a href="#" class="lvay-b2b" id="lvayB2B" aria-label="Back to the bracket">&larr; Back to Bracket</a>
<script>
(function(){
  var KEY='lvay_b2b_from', btn=document.getElementById('lvayB2B'); if(!btn) return;
  var from='';
  try{
    var r=document.referrer?new URL(document.referrer):null;
    if(r && r.host===location.host && /bracket|power-ratings|playoff/i.test(r.pathname+r.search+r.hash)){
      from=document.referrer; sessionStorage.setItem(KEY,from);
    } else if(r && r.host===location.host && /\/football\/schedules/.test(r.pathname)){
      from=sessionStorage.getItem(KEY)||'';   /* moved between schools on the schedule page */
    } else { sessionStorage.removeItem(KEY); }
  }catch(e){}
  if(!from) return;
  btn.href=from; btn.classList.add('on');
  btn.addEventListener('click',function(e){
    /* Prefer real Back so the bracket keeps its scroll spot and open division. */
    try{ var r=new URL(document.referrer); if(document.referrer===from && history.length>1){ e.preventDefault(); history.back(); } }catch(_){}
  });
})();
</script>
    <?php
}
add_action('wp_footer', 'lvay_fb_back_to_bracket', 99);
