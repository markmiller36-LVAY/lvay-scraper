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

function lvay_winter_season_label($season) {
    return $season === 2026 ? '2025-2026' : '2026-2027';
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
    $title = lvay_winter_season_label($season) . ' LHSAA<br>' . $cfg['label'] . ' ' . strtoupper($view === 'rankings' ? 'POWER RANKINGS' : ($view === 'brackets' ? 'PLAYOFF BRACKETS' : 'SCHEDULES'));
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
    .entry-title{display:none!important}
    .lvay-w-layout{display:grid;grid-template-columns:minmax(0,1fr) 300px;gap:24px;width:min(1600px,calc(100vw - 48px));max-width:none;margin:0;position:relative;left:50%;transform:translateX(-50%);padding:18px 0 36px;color:#111}
    .lvay-w-layout *{box-sizing:border-box}.lvay-w-layout>main{min-width:0}.lvay-w-layout>main>header h1{font-family:"Alfa Slab One",Rockwell,serif;color:#078b88;font-size:clamp(34px,3vw,46px);line-height:.95;letter-spacing:.2px;margin:0 0 17px;text-transform:uppercase}
    .lvay-w-archive{align-self:start;background:#050505;color:#fff!important;padding:20px 24px 24px;display:grid;grid-template-columns:1fr 1fr;gap:7px 18px;height:max-content}.lvay-w-archive h3{grid-column:1/-1;color:#fff!important;font-family:Teko,Arial,sans-serif!important;font-size:34px!important;font-weight:500!important;line-height:1!important;letter-spacing:.8px!important;margin:0 0 12px;text-decoration:underline;text-underline-offset:5px}.lvay-w-archive a{color:#999!important;font-family:Teko,Arial,sans-serif!important;font-size:25px!important;font-weight:500!important;line-height:1.05!important;text-decoration:none!important}.lvay-w-archive a:hover,.lvay-w-archive a.active{color:#fff!important}.lvay-w-archive span{grid-column:1/-1;color:#aaa!important;font:400 18px/1.15 Teko,Arial,sans-serif!important;margin-top:12px}
    .lvay-w-empty{border:2px solid #078d8b;padding:42px;background:#f7f7f7}.lvay-w-empty span{color:#078d8b;font:600 22px Teko}.lvay-w-empty h2{font:400 36px "Alfa Slab One";margin:6px 0}.lvay-w-empty p{font:24px Teko}.lvay-w-empty a{background:#050505;color:#fff;padding:10px 16px;text-decoration:none;font:600 21px Teko}
    .lvay-w-search{position:relative;z-index:30;margin-bottom:18px}.lvay-w-search input{display:block;width:100%;height:50px;border:2px solid #078b88;border-radius:4px;padding:10px 15px;font-size:18px}.lvay-w-results{position:absolute;z-index:9999;top:100%;left:0;right:0;background:#fff;border:1px solid #078b88;max-height:420px;overflow:auto;box-shadow:0 10px 24px #0003}.lvay-w-results button{display:block;width:100%;text-align:left;border:0;border-bottom:1px solid #ddd;background:#fff;padding:10px 14px;font:500 24px Teko}.lvay-w-results button:hover{background:#000;color:#fff}
    .lvay-w-layout details{border-bottom:1px solid #d9dddd;background:#fff}.lvay-w-layout summary{display:flex;align-items:center;gap:7px;cursor:pointer;list-style:none;font-family:"Alfa Slab One",Rockwell,serif;color:#090909}.lvay-w-layout summary::-webkit-details-marker{display:none}.lvay-w-layout summary i{display:inline-block;color:#5dc7c1;font:normal 32px/.7 Arial;transition:transform .15s}.lvay-w-layout details[open]>summary i{transform:rotate(90deg)}.lvay-w-class>summary{padding:14px 4px 10px;font-size:27px;border-bottom:2px solid #078b88}.lvay-w-district>summary{padding:12px 16px;font-size:20px}.lvay-w-rankings summary,.lvay-w-brackets summary{min-height:48px;padding:8px 16px;font-size:23px}
    .lvay-w-school>button{width:100%;min-height:50px;text-align:left;border:0;border-bottom:1px solid #e7e9e9;background:#fff!important;color:#080808!important;padding:11px 20px;font:500 25px Teko,Arial,sans-serif}.lvay-w-school>button:hover{background:#000!important;color:#fff!important}.lvay-w-school>button[aria-expanded=true]{background:#333!important;color:#fff!important}.lvay-w-school>div{padding:10px;overflow:auto}
    .lvay-w-table{overflow:auto}.lvay-w-layout table{width:100%;border-collapse:collapse;font:20px Teko,Arial,sans-serif;min-width:850px}.lvay-w-layout th{background:#078d8b;color:#fff;text-align:left;padding:8px 10px;font-size:18px}.lvay-w-layout td{padding:7px 10px;border-bottom:1px solid #ddd}.lvay-w-layout tr:nth-child(even){background:#f1f1f1}.lvay-w-layout td a{color:#078d8b;font-weight:600}
    .lvay-w-layout a.lvay-w-opponent{color:inherit!important;font-weight:inherit;text-decoration:none}.lvay-w-layout a.lvay-w-opponent:hover{text-decoration:underline}
    .lvay-w-brackets details>div{padding:12px}.lvay-w-brackets a{display:inline-block;background:#078d8b;color:#fff;padding:9px 14px;text-decoration:none;font:600 20px Teko;margin-bottom:10px}.lvay-w-brackets iframe{display:block;width:100%;height:820px;border:1px solid #bbb}.lvay-w-source{font:23px Teko}
    .lvay-w-game.w td:nth-child(4){color:#079447;font-weight:700}.lvay-w-game.l td:nth-child(4){color:#d22;font-weight:700}.lvay-w-game.district{font-weight:600;color:#078d8b}
    @media(max-width:1200px){.lvay-w-layout{grid-template-columns:1fr}.lvay-w-layout>main{grid-row:1}.lvay-w-archive{grid-row:2;order:2}.lvay-w-brackets iframe{height:620px}}
    @media(max-width:600px){.lvay-w-layout{width:calc(100vw - 24px);padding-top:8px}.lvay-w-layout>main>header h1{font-size:34px}.lvay-w-archive{padding:17px 19px}.lvay-w-class>summary{font-size:23px}.lvay-w-district>summary{font-size:18px}.lvay-w-school>button{font-size:22px}.lvay-w-empty{padding:24px}.lvay-w-brackets iframe{height:520px}}
    ');
    wp_register_script('lvay-winter-pages',false,array(),null,true);
    wp_enqueue_script('lvay-winter-pages');
    wp_add_inline_script('lvay-winter-pages','
    document.addEventListener("DOMContentLoaded",function(){
      var root=document.querySelector(".lvay-w-layout"); if(!root)return;
      var sport=root.dataset.sport,season=root.dataset.season;
      var schools=[].slice.call(root.querySelectorAll(".lvay-w-school"));
      var input=root.querySelector(".lvay-w-search input"),results=root.querySelector(".lvay-w-results");
      function esc(v){return String(v==null?"":v).replace(/[&<>"\']/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","\'":"&#39;"}[c]})}
      function schoolCard(name){var n=String(name||"").toLowerCase();return schools.find(function(c){return c.dataset.label.toLowerCase()===n})}
      function opponentLink(name){
        var card=schoolCard(name);if(!card)return esc(name);
        return "<a class=\"lvay-w-opponent\" href=\"?season="+encodeURIComponent(season)+"&school="+encodeURIComponent(card.dataset.label)+"#"+card.id+"\">"+esc(name)+"</a>";
      }
      function openSchool(card){
        card.closest(".lvay-w-district").open=true; card.closest(".lvay-w-class").open=true;
        var body=card.querySelector("div"),button=card.querySelector("button");
        button.setAttribute("aria-expanded","true"); body.hidden=false;
        if(!body.dataset.loaded){body.innerHTML="<p>Loading schedule...</p>";fetch("https://lvay-scraper.onrender.com/api/schedules/winter/"+sport+"?season="+season+"&school="+encodeURIComponent(card.dataset.label)).then(function(r){return r.json()}).then(function(data){
          var s=(data.schools||[]).find(function(x){return x.school===card.dataset.label})||(data.schools||[])[0];if(!s){body.innerHTML="<p>Schedule unavailable.</p>";return}
          var h="<table><thead><tr><th>Date</th><th>H/A</th><th>Opponent</th><th>W/L</th><th>Score</th><th>Opp Record</th><th>Division</th><th>Power Pts</th></tr></thead><tbody>";
          (s.games||[]).forEach(function(g){var cls="lvay-w-game "+((g.result||"").toLowerCase())+(g.is_district?" district":"");var opp=(g.opp_wins||0)+"-"+(g.opp_losses||0)+(g.opp_ties?"-"+g.opp_ties:"");h+="<tr class=\""+cls+"\"><td>"+esc(g.game_date||"")+"</td><td>"+esc(g.home_away||"")+"</td><td>"+opponentLink(g.opponent||"")+"</td><td>"+esc(g.result||"")+"</td><td>"+esc(g.score||"")+"</td><td>"+esc(opp)+"</td><td>"+esc(g.opp_division||"")+"</td><td>"+Number(g.total_pts||0).toFixed(2)+"</td></tr>"});body.innerHTML=h+"</tbody></table>";body.dataset.loaded="1";
        }).catch(function(){body.innerHTML="<p>Schedule temporarily unavailable.</p>"})}
        card.scrollIntoView({behavior:"smooth",block:"center"});
      }
      schools.forEach(function(card){card.querySelector("button").addEventListener("click",function(){var expanded=this.getAttribute("aria-expanded")==="true";if(expanded){this.setAttribute("aria-expanded","false");card.querySelector("div").hidden=true}else openSchool(card)})});
      if(input){input.addEventListener("input",function(){var q=this.value.trim().toLowerCase();if(!q){results.hidden=true;results.innerHTML="";return}var matches=schools.filter(function(c){return c.dataset.name.indexOf(q)!==-1}).slice(0,14);results.innerHTML=matches.map(function(c){return "<button type=\"button\" data-id=\""+c.id+"\">"+c.dataset.label+"</button>"}).join("");results.hidden=!matches.length});results.addEventListener("click",function(e){var b=e.target.closest("button");if(!b)return;results.hidden=true;input.value=document.getElementById(b.dataset.id).dataset.label;openSchool(document.getElementById(b.dataset.id))})}
      var params=new URLSearchParams(location.search),school=params.get("school");if(school){var card=schoolCard(school);if(card)openSchool(card)}
    });');
}
add_action('wp_enqueue_scripts','lvay_winter_assets');

