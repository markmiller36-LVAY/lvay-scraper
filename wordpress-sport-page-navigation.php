/**
 * LVAY Big Three cross-page navigation.
 *
 * Adds a fast, consistent Schedules / Power Rankings / Playoff Brackets
 * navigation strip to all eight automated sports. Paste into Code Snippets
 * without an opening PHP tag and run everywhere.
 */

function lvay_big_three_page_map() {
    return array(
        'schedules' => array(
            'schedule' => 'schedules',
            'rankings' => 'power-rankings',
            'brackets' => 'playoff-brackets',
        ),
        'baseball-schedules' => array(
            'schedule' => 'baseball-schedules',
            'rankings' => 'baseball-power-rankings',
            'brackets' => 'baseball-playoff-brackets',
        ),
        'softball-schedules' => array(
            'schedule' => 'softball-schedules',
            'rankings' => 'softball-power-rankings',
            'brackets' => 'softball-playoff-brackets',
        ),
        'volleyball-schedules' => array(
            'schedule' => 'volleyball-schedules',
            'rankings' => 'volleyball-power-rankings',
            'brackets' => 'volleyball-playoff-brackets',
        ),
        'boys-basketball-schedules' => array(
            'schedule' => 'boys-basketball-schedules',
            'rankings' => 'boys-basketball-power-rankings',
            'brackets' => 'boys-basketball-playoff-brackets',
        ),
        'girls-basketball-schedules' => array(
            'schedule' => 'girls-basketball-schedules',
            'rankings' => 'girls-basketball-power-rankings',
            'brackets' => 'girls-basketball-playoff-brackets',
        ),
        'boys-soccer-schedules' => array(
            'schedule' => 'boys-soccer-schedules',
            'rankings' => 'boys-soccer-power-rankings',
            'brackets' => 'boys-soccer-playoff-brackets',
        ),
        'girls-soccer-schedules' => array(
            'schedule' => 'girls-soccer-schedules',
            'rankings' => 'girls-soccer-power-rankings',
            'brackets' => 'girls-soccer-playoff-brackets',
        ),
    );
}

function lvay_big_three_context() {
    foreach (lvay_big_three_page_map() as $schedule_slug => $pages) {
        foreach ($pages as $view => $slug) {
            if (is_page($slug)) {
                return array(
                    'view' => $view,
                    'pages' => $pages,
                    'schedule_slug' => $schedule_slug,
                );
            }
        }
    }
    return null;
}

function lvay_big_three_assets() {
    $context = lvay_big_three_context();
    if (!$context) return;

    wp_register_style('lvay-big-three-nav', false);
    wp_enqueue_style('lvay-big-three-nav');
    wp_add_inline_style('lvay-big-three-nav', <<<'CSS'
.lvay-big-three-nav{
  display:grid;
  grid-template-columns:repeat(3,minmax(0,1fr));
  gap:8px;
  width:100%;
  margin:0 0 22px;
  font-family:Teko,Arial,sans-serif;
}
.lvay-big-three-nav.is-archive{
  grid-template-columns:repeat(2,minmax(0,1fr));
}
.lvay-big-three-nav a{
  display:flex;
  align-items:center;
  justify-content:center;
  min-height:48px;
  padding:9px 14px 7px;
  border:2px solid #078b88;
  background:#fff;
  color:#078b88!important;
  font-size:23px;
  font-weight:600;
  line-height:1;
  letter-spacing:.25px;
  text-align:center;
  text-decoration:none!important;
  text-transform:uppercase;
  transition:background-color .15s ease,color .15s ease,border-color .15s ease;
}
.lvay-big-three-nav a:hover,
.lvay-big-three-nav a:focus-visible{
  border-color:#050505;
  background:#050505;
  color:#fff!important;
  outline:none;
}
.lvay-big-three-nav a[aria-current="page"]{
  border-color:#333;
  background:#333;
  color:#fff!important;
  cursor:default;
}
@media(max-width:700px){
  .lvay-big-three-nav{
    gap:5px;
    margin-bottom:18px;
  }
  .lvay-big-three-nav a{
    min-height:44px;
    padding:7px 5px 5px;
    font-size:17px;
  }
}
CSS
    );

    $season = isset($_GET['season']) ? absint($_GET['season']) : 0;
    $is_archive = $season > 0;
    $links = array();
    foreach ($context['pages'] as $view => $slug) {
        if ($is_archive && $view === 'rankings') continue;
        $url = home_url('/' . $slug . '/');
        if ($season) $url = add_query_arg('season', $season, $url);
        $links[] = array(
            'view' => $view,
            'label' => array(
                'schedule' => 'Schedules',
                'rankings' => 'Power Rankings',
                'brackets' => 'Playoff Brackets',
            )[$view],
            'url' => $url,
            'current' => $view === $context['view'],
        );
    }

    wp_register_script('lvay-big-three-nav', false, array(), null, true);
    wp_enqueue_script('lvay-big-three-nav');
    wp_add_inline_script(
        'lvay-big-three-nav',
        'window.LVAY_BIG_THREE=' . wp_json_encode(array(
            'links' => $links,
            'archive' => $is_archive,
        )) . ';',
        'before'
    );
    wp_add_inline_script('lvay-big-three-nav', <<<'JS'
(function(){
  function mount(){
    var config=window.LVAY_BIG_THREE;
    if(!config || document.querySelector(".lvay-big-three-nav")) return;

    var root=document.querySelector(
      ".lvay-football-schedules,.lvay-rankings-design,.lvay-season-layout,"+
      ".lvay-bb-layout,.lvay-sb-layout,.lvay-w-layout"
    );
    if(!root) return;

    var main=root.matches("main") ? root : root.querySelector("main");
    if(!main) main=root;

    var nav=document.createElement("nav");
    nav.className="lvay-big-three-nav";
    if(config.archive) nav.classList.add("is-archive");
    nav.setAttribute("aria-label","Sport pages");
    config.links.forEach(function(link){
      var a=document.createElement("a");
      a.href=link.url;
      a.textContent=link.label;
      if(link.current) a.setAttribute("aria-current","page");
      nav.appendChild(a);
    });

    var title=main.querySelector(
      ".lvay-schedule-title,.lvay-bb-title,.lvay-sb-title,header,"+
      ".lvay-rankings-heading,.lvay-brackets-heading"
    );
    if(title) title.insertAdjacentElement("afterend",nav);
    else main.insertAdjacentElement("afterbegin",nav);
  }
  if(document.readyState==="loading") document.addEventListener("DOMContentLoaded",mount);
  else mount();
})();
JS
    );
}
add_action('wp_enqueue_scripts', 'lvay_big_three_assets', 120);
