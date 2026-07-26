/**
 * LVAY winter season pages: Boys/Girls Basketball and Boys/Girls Soccer.
 * Creates each sport's Schedule, Power Rankings, and Playoff Brackets page,
 * then supplies season-aware 2026 archives and 2027 preseason shells.
 * Paste into Code Snippets without an opening PHP tag.
 */

function lvay_winter_sports() {
    return array(
        'boys_basketball' => array(
            'label' => "BOYS' BASKETBALL", 'short' => 'bbb',
            'schedule' => 'boys-basketball-schedules',
            'rankings' => 'boys-basketball-power-rankings',
            'brackets' => 'boys-basketball-playoff-brackets',
            'type' => 'basketball', 'bracket_sport' => 2,
        ),
        'girls_basketball' => array(
            'label' => "GIRLS' BASKETBALL", 'short' => 'gbb',
            'schedule' => 'girls-basketball-schedules',
            'rankings' => 'girls-basketball-power-rankings',
            'brackets' => 'girls-basketball-playoff-brackets',
            'type' => 'basketball', 'bracket_sport' => 3,
        ),
        'boys_soccer' => array(
            'label' => "BOYS' SOCCER", 'short' => 'bsoc',
            'schedule' => 'boys-soccer-schedules',
            'rankings' => 'boys-soccer-power-rankings',
            'brackets' => 'boys-soccer-playoff-brackets',
            'type' => 'soccer',
            'bracket_pdf' => 'https://www.lhsaa.org/siteuploads/editorimg/file/Soccer/25-26%20Soccer/2026%20Boys%20Soccer%20Brackets.pdf',
        ),
        'girls_soccer' => array(
            'label' => "GIRLS' SOCCER", 'short' => 'gsoc',
            'schedule' => 'girls-soccer-schedules',
            'rankings' => 'girls-soccer-power-rankings',
            'brackets' => 'girls-soccer-playoff-brackets',
            'type' => 'soccer',
            'bracket_pdf' => 'https://www.lhsaa.org/siteuploads/editorimg/file/Soccer/25-26%20Soccer/2026%20Girls%20Soccer%20Brackets.pdf',
        ),
    );
}

function lvay_winter_install_pages() {
    if (get_option('lvay_winter_pages_v1')) return;
    foreach (lvay_winter_sports() as $cfg) {
        foreach (array(
            $cfg['schedule'] => ucwords(strtolower($cfg['label'])) . ' Schedules',
            $cfg['rankings'] => ucwords(strtolower($cfg['label'])) . ' Power Rankings',
            $cfg['brackets'] => ucwords(strtolower($cfg['label'])) . ' Playoff Brackets',
        ) as $slug => $title) {
            if (!get_page_by_path($slug)) {
                wp_insert_post(array(
                    'post_title' => $title, 'post_name' => $slug,
                    'post_status' => 'publish', 'post_type' => 'page',
                    'post_content' => '<!-- LVAY winter page -->',
                ));
            }
        }
    }
    update_option('lvay_winter_pages_v1', 1, false);
}
add_action('init', 'lvay_winter_install_pages');

function lvay_winter_context() {
    foreach (lvay_winter_sports() as $sport => $cfg) {
        foreach (array('schedule', 'rankings', 'brackets') as $view) {
            if (is_page($cfg[$view])) return array($sport, $cfg, $view);
        }
    }
    return null;
}

function lvay_winter_season() {
    $season = isset($_GET['season']) ? absint($_GET['season']) : 2027;
    return in_array($season, array(2026, 2027), true) ? $season : 2027;
}

function lvay_winter_get($path) {
    $key = 'lvay_w_' . md5($path);
    $cached = get_transient($key);
    if (is_array($cached)) return $cached;
    $response = wp_remote_get('https://lvay-scraper.onrender.com' . $path, array('timeout' => 30));
    if (is_wp_error($response)) return array();
    $data = json_decode(wp_remote_retrieve_body($response), true);
    if (!is_array($data)) return array();
    set_transient($key, $data, 10 * MINUTE_IN_SECONDS);
    return $data;
}

function lvay_winter_url($slug, $season = 2027) {
    $url = home_url('/' . $slug . '/');
    return $season === 2027 ? $url : add_query_arg('season', $season, $url);
}

function lvay_winter_nav($cfg, $view, $season) {
    $slug = $cfg[$view];
    return '<aside class="lvay-w-archive"><h3>SEASON ARCHIVES</h3>'
        . '<a class="' . ($season === 2027 ? 'active' : '') . '" href="' . esc_url(lvay_winter_url($slug)) . '">2026-2027</a>'
        . '<a class="' . ($season === 2026 ? 'active' : '') . '" href="' . esc_url(lvay_winter_url($slug, 2026)) . '">2025-2026</a>'
        . '<span>More seasons will be added as they are digitized.</span></aside>';
}

function lvay_winter_empty($season, $kind, $cfg, $view) {
    $archive = lvay_winter_url($cfg[$view], 2026);
    $messages = array(
        'schedule' => array('SCHEDULES COMING SOON', 'Schedules are not available yet.', 'This page will populate automatically when official LHSAA schedules enter the LVAY system.'),
        'rankings' => array('PRESEASON', 'Power rankings begin after games are played.', 'Rankings will populate automatically when official scores enter the LVAY system.'),
        'brackets' => array('COMING THIS POSTSEASON', 'Playoff brackets will appear here.', 'The completed 2025-2026 brackets remain preserved in the Season Archives.'),
    );
    $m = $messages[$kind];
    return '<section class="lvay-w-empty"><span>' . $m[0] . '</span><h2>' . $m[1] . '</h2><p>' . $m[2]
        . '</p><a href="' . esc_url($archive) . '">View the 2025-2026 archive</a></section>';
}

function lvay_winter_schedule($sport, $cfg, $season) {
    $data = lvay_winter_get('/api/schedules/winter/' . $sport . '?season=' . $season . '&summary=1');
    $schools = (isset($data['season'], $data['schools']) && (int)$data['season'] === $season) ? $data['schools'] : array();
    if (!$schools) return lvay_winter_empty($season, 'schedule', $cfg, 'schedule');
    $groups = array();
    foreach ($schools as $school) {
        $class = strtoupper(trim((string)($school['class_'] ?? '')));
        if ($class === 'B' || $class === 'C') $class = 'Class ' . $class;
        $district = trim((string)($school['district'] ?? ''));
        $groups[$class][$district][] = $school;
    }
    $out = '<div class="lvay-w-search"><input type="search" placeholder="Search for a school..." autocomplete="off"><div class="lvay-w-results" hidden></div></div><div class="lvay-w-groups">';
    $order = $cfg['type'] === 'soccer' ? array('5A','4A','3A','2A','1A') : array('5A','4A','3A','2A','1A','Class B','Class C');
    foreach ($order as $class) {
        if (empty($groups[$class])) continue;
        $out .= '<details class="lvay-w-class"><summary><i>›</i>' . esc_html($class) . '</summary><div>';
        uksort($groups[$class], 'strnatcasecmp');
        foreach ($groups[$class] as $district => $rows) {
            usort($rows, function($a,$b){ return strcasecmp($a['school'],$b['school']); });
            $district_label = $district ? $district . '-' . $class : $class;
            $out .= '<details class="lvay-w-district"><summary><i>›</i>' . esc_html($district_label) . '</summary><div>';
            foreach ($rows as $row) {
                $name = (string)$row['school'];
                $out .= '<article class="lvay-w-school" id="' . esc_attr($cfg['short'] . '-' . sanitize_title($name))
                    . '" data-name="' . esc_attr(strtolower($name)) . '" data-label="' . esc_attr($name) . '">'
                    . '<button type="button" aria-expanded="false"><b>' . esc_html($name) . '</b></button><div hidden></div></article>';
            }
            $out .= '</div></details>';
        }
        $out .= '</div></details>';
    }
    return $out . '</div>';
}

function lvay_winter_rankings($sport, $cfg, $season) {
    $data = lvay_winter_get('/api/rankings/winter/' . $sport . '?season=' . $season);
    $rows = (isset($data['season'], $data['rankings']) && (int)$data['season'] === $season) ? $data['rankings'] : array();
    if (!$rows) return lvay_winter_empty($season, 'rankings', $cfg, 'rankings');
    $groups = array();
    foreach ($rows as $row) $groups[$row['division'] ?: ($row['class_'] ?: 'Other')][] = $row;
    uksort($groups, 'strnatcasecmp');
    $out = '<div class="lvay-w-rankings">';
    foreach ($groups as $division => $division_rows) {
        usort($division_rows, function($a,$b){ return ((float)$b['power_rating']) <=> ((float)$a['power_rating']); });
        $out .= '<details><summary><i>›</i>' . esc_html($division) . '</summary><div class="lvay-w-table"><table><thead><tr><th>#</th><th>Team</th><th>Class</th><th>Record</th><th>GP</th><th>PR</th><th>SF</th></tr></thead><tbody>';
        foreach ($division_rows as $i => $row) {
            $record = (int)$row['wins'] . '-' . (int)$row['losses'] . (!empty($row['ties']) ? '-' . (int)$row['ties'] : '');
            $team_url = add_query_arg(array('season'=>$season,'school'=>$row['school']), home_url('/'.$cfg['schedule'].'/')) . '#' . $cfg['short'] . '-' . sanitize_title($row['school']);
            $out .= '<tr><td>' . ($i+1) . '</td><td><a href="' . esc_url($team_url) . '">' . esc_html($row['school']) . '</a></td><td>'
                . esc_html($row['class_']) . '</td><td>' . esc_html($record) . '</td><td>' . (int)$row['games_played'] . '</td><td>'
                . number_format((float)$row['power_rating'],2) . '</td><td>' . number_format((float)($row['strength_factor'] ?? 0),2) . '</td></tr>';
        }
        $out .= '</tbody></table></div></details>';
    }
    return $out . '</div>';
}

function lvay_winter_brackets($cfg, $season) {
    if ($season !== 2026) return lvay_winter_empty($season, 'brackets', $cfg, 'brackets');
    if ($cfg['type'] === 'soccer') {
        $out = '<p class="lvay-w-source">Complete official LHSAA 2026 brackets.</p><div class="lvay-w-brackets">';
        foreach (array('Division I','Division II','Division III','Division IV') as $i => $division) {
            $url = $cfg['bracket_pdf'] . '#page=' . ($i+1) . '&view=FitH';
            $out .= '<details><summary><i>›</i>' . $division . '</summary><div><a target="_blank" rel="noopener" href="' . esc_url($url) . '">Open official completed bracket</a><iframe loading="lazy" src="' . esc_url($url) . '" title="' . esc_attr($cfg['label'].' '.$division) . '"></iframe></div></details>';
        }
        return $out . '</div>';
    }
    $out = '<p class="lvay-w-source">Complete official LHSAA 2026 brackets.</p><div class="lvay-w-brackets">';
    foreach (array('Division I Non-Select'=>array('I',0),'Division II Non-Select'=>array('II',0),'Division III Non-Select'=>array('III',0),'Division IV Non-Select'=>array('IV',0),'Division I Select'=>array('I',1),'Division II Select'=>array('II',1),'Division III Select'=>array('III',1),'Division IV Select'=>array('IV',1),'Class B'=>array('B',null),'Class C'=>array('C',null)) as $label=>$parts) {
        $url = 'https://www.lhsaaonline.org/MainBracket32.aspx?d=' . rawurlencode($parts[0]) . '&s=' . (int)$cfg['bracket_sport'] . '&y=2026';
        if ($parts[1] !== null) $url .= '&select=' . (int)$parts[1];
        $out .= '<details><summary><i>›</i>' . esc_html($label) . '</summary><div><a target="_blank" rel="noopener" href="' . esc_url($url) . '">Open official completed bracket</a><iframe loading="lazy" src="' . esc_url($url) . '" title="' . esc_attr($cfg['label'].' '.$label) . '"></iframe></div></details>';
    }
    return $out . '</div>';
}

function lvay_winter_replace($content) {
    if (!in_the_loop() || !is_main_query()) return $content;
    $context = lvay_winter_context();
    if (!$context) return $content;
    list($sport,$cfg,$view) = $context;
    $season = lvay_winter_season();
    $body = $view === 'schedule' ? lvay_winter_schedule($sport,$cfg,$season) : ($view === 'rankings' ? lvay_winter_rankings($sport,$cfg,$season) : lvay_winter_brackets($cfg,$season));
    $title = $season . ' LHSAA<br>' . $cfg['label'] . ' ' . strtoupper($view === 'rankings' ? 'POWER RANKINGS' : ($view === 'brackets' ? 'PLAYOFF BRACKETS' : 'SCHEDULES'));
    return '<section class="lvay-w-layout" data-sport="' . esc_attr($sport) . '" data-season="' . $season . '"><main><header><h1>' . $title . '</h1></header>' . $body . '</main>' . lvay_winter_nav($cfg,$view,$season) . '</section>';
}
add_filter('the_content','lvay_winter_replace',999);

function lvay_winter_assets() {
    $context = lvay_winter_context();
    if (!$context) return;
    wp_enqueue_style('lvay-fonts','https://fonts.googleapis.com/css2?family=Alfa+Slab+One&family=Teko:wght@400;500;600;700&display=swap',array(),null);
    wp_register_style('lvay-winter-pages',false);
    wp_enqueue_style('lvay-winter-pages');
    wp_add_inline_style('lvay-winter-pages','
    .lvay-w-layout{width:min(96vw,1680px);margin:28px auto;display:grid;grid-template-columns:minmax(0,1fr) 300px;gap:28px;color:#111}
    .lvay-w-layout *{box-sizing:border-box}.lvay-w-layout>main>header h1{font-family:"Alfa Slab One",serif;color:#078d8b;font-size:clamp(34px,4vw,62px);line-height:.95;margin:0 0 22px}
    .lvay-w-archive{background:#050505;color:#fff;padding:24px;height:max-content}.lvay-w-archive h3{font-family:"Alfa Slab One";font-size:25px;text-decoration:underline;margin:0 0 16px;font-weight:400}.lvay-w-archive a{display:block;color:#777;font:600 25px/1.3 Teko;text-decoration:none}.lvay-w-archive a.active{color:#fff}.lvay-w-archive span{display:block;color:#888;font:italic 17px Teko;margin-top:18px}
    .lvay-w-empty{border:2px solid #078d8b;padding:42px;background:#f7f7f7}.lvay-w-empty span{color:#078d8b;font:600 22px Teko}.lvay-w-empty h2{font:400 36px "Alfa Slab One";margin:6px 0}.lvay-w-empty p{font:24px Teko}.lvay-w-empty a{background:#050505;color:#fff;padding:10px 16px;text-decoration:none;font:600 21px Teko}
    .lvay-w-search{position:relative;z-index:30;margin-bottom:18px}.lvay-w-search input{width:100%;border:2px solid #078d8b;padding:13px 15px;font-size:17px}.lvay-w-results{position:absolute;top:100%;left:0;right:0;background:#fff;border:1px solid #078d8b;max-height:360px;overflow:auto;box-shadow:0 10px 25px #0003}.lvay-w-results button{display:block;width:100%;text-align:left;border:0;border-bottom:1px solid #ddd;background:#fff;padding:10px 14px;font:600 20px Teko}.lvay-w-results button:hover{background:#000;color:#fff}
    .lvay-w-layout details{border-bottom:1px solid #ddd}.lvay-w-layout summary{cursor:pointer;list-style:none;font-family:"Alfa Slab One";padding:12px}.lvay-w-layout summary i{display:inline-block;color:#078d8b;margin-right:10px}.lvay-w-layout details[open]>summary i{transform:rotate(90deg)}.lvay-w-class>summary{font-size:25px;border-bottom:2px solid #078d8b}.lvay-w-district>summary,.lvay-w-rankings summary,.lvay-w-brackets summary{font-size:22px}
    .lvay-w-school>button{width:100%;text-align:left;border:0;background:#fff;padding:10px 18px;font:600 21px Teko}.lvay-w-school>button:hover{background:#000;color:#fff}.lvay-w-school>button[aria-expanded=true]{background:#444;color:#fff}.lvay-w-school>div{padding:10px;overflow:auto}
    .lvay-w-table{overflow:auto}.lvay-w-layout table{width:100%;border-collapse:collapse;font:19px Teko;min-width:680px}.lvay-w-layout th{background:#078d8b;color:#fff;text-align:left;padding:8px}.lvay-w-layout td{padding:7px 8px;border-bottom:1px solid #ddd}.lvay-w-layout tr:nth-child(even){background:#f1f1f1}.lvay-w-layout td a{color:#078d8b;font-weight:600}
    .lvay-w-brackets details>div{padding:12px}.lvay-w-brackets a{display:inline-block;background:#078d8b;color:#fff;padding:9px 14px;text-decoration:none;font:600 20px Teko;margin-bottom:10px}.lvay-w-brackets iframe{display:block;width:100%;height:820px;border:1px solid #bbb}.lvay-w-source{font:23px Teko}
    .lvay-w-game.w td:nth-child(4){color:#079447;font-weight:700}.lvay-w-game.l td:nth-child(4){color:#d22;font-weight:700}.lvay-w-game.district{font-weight:600;color:#078d8b}
    @media(max-width:900px){.lvay-w-layout{grid-template-columns:1fr;width:94vw}.lvay-w-archive{order:-1;display:flex;gap:14px;align-items:center;flex-wrap:wrap}.lvay-w-archive h3{width:100%}.lvay-w-brackets iframe{height:620px}}
    @media(max-width:560px){.lvay-w-layout{width:96vw;margin:16px auto}.lvay-w-layout>main>header h1{font-size:34px}.lvay-w-archive{padding:16px}.lvay-w-empty{padding:24px}.lvay-w-brackets iframe{height:520px}}
    ');
    wp_register_script('lvay-winter-pages',false,array(),null,true);
    wp_enqueue_script('lvay-winter-pages');
    wp_add_inline_script('lvay-winter-pages','
    document.addEventListener("DOMContentLoaded",function(){
      var root=document.querySelector(".lvay-w-layout"); if(!root)return;
      var sport=root.dataset.sport,season=root.dataset.season;
      var schools=[].slice.call(root.querySelectorAll(".lvay-w-school"));
      var input=root.querySelector(".lvay-w-search input"),results=root.querySelector(".lvay-w-results");
      function openSchool(card){
        card.closest(".lvay-w-district").open=true; card.closest(".lvay-w-class").open=true;
        var body=card.querySelector("div"),button=card.querySelector("button");
        button.setAttribute("aria-expanded","true"); body.hidden=false;
        if(!body.dataset.loaded){body.innerHTML="<p>Loading schedule...</p>";fetch("https://lvay-scraper.onrender.com/api/schedules/winter/"+sport+"?season="+season+"&school="+encodeURIComponent(card.dataset.label)).then(function(r){return r.json()}).then(function(data){
          var s=(data.schools||[]).find(function(x){return x.school===card.dataset.label})||(data.schools||[])[0];if(!s){body.innerHTML="<p>Schedule unavailable.</p>";return}
          var h="<table><thead><tr><th>Date</th><th>H/A</th><th>Opponent</th><th>W/L</th><th>Score</th><th>Opp Record</th><th>Division</th><th>Power Pts</th></tr></thead><tbody>";
          (s.games||[]).forEach(function(g){var cls="lvay-w-game "+((g.result||"").toLowerCase())+(g.is_district?" district":"");var opp=(g.opp_wins||0)+"-"+(g.opp_losses||0)+(g.opp_ties?"-"+g.opp_ties:"");h+="<tr class=\""+cls+"\"><td>"+(g.game_date||"")+"</td><td>"+(g.home_away||"")+"</td><td>"+(g.opponent||"")+"</td><td>"+(g.result||"")+"</td><td>"+(g.score||"")+"</td><td>"+opp+"</td><td>"+(g.opp_division||"")+"</td><td>"+Number(g.total_pts||0).toFixed(2)+"</td></tr>"});body.innerHTML=h+"</tbody></table>";body.dataset.loaded="1";
        }).catch(function(){body.innerHTML="<p>Schedule temporarily unavailable.</p>"})}
        card.scrollIntoView({behavior:"smooth",block:"center"});
      }
      schools.forEach(function(card){card.querySelector("button").addEventListener("click",function(){var expanded=this.getAttribute("aria-expanded")==="true";if(expanded){this.setAttribute("aria-expanded","false");card.querySelector("div").hidden=true}else openSchool(card)})});
      if(input){input.addEventListener("input",function(){var q=this.value.trim().toLowerCase();if(!q){results.hidden=true;results.innerHTML="";return}var matches=schools.filter(function(c){return c.dataset.name.indexOf(q)!==-1}).slice(0,14);results.innerHTML=matches.map(function(c){return "<button type=\"button\" data-id=\""+c.id+"\">"+c.dataset.label+"</button>"}).join("");results.hidden=!matches.length});results.addEventListener("click",function(e){var b=e.target.closest("button");if(!b)return;results.hidden=true;input.value=document.getElementById(b.dataset.id).dataset.label;openSchool(document.getElementById(b.dataset.id))})}
      var params=new URLSearchParams(location.search),school=params.get("school");if(school){var card=schools.find(function(c){return c.dataset.label===school});if(card)openSchool(card)}
    });');
}
add_action('wp_enqueue_scripts','lvay_winter_assets');

