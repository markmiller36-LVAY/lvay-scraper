/**
 * LVAY Baseball season pages.
 *
 * Replaces the Baseball Schedules, Power Ratings, and Playoff Brackets page
 * bodies with season-aware, responsive displays matching the football pages.
 * Paste into Code Snippets without an opening PHP tag.
 */

function lvay_bb_season() {
    $season = isset($_GET['season']) ? absint($_GET['season']) : 2026;
    return in_array($season, array(2025, 2026), true) ? $season : 2026;
}

function lvay_bb_get($path) {
    $response = wp_remote_get('https://lvay-scraper.onrender.com' . $path, array('timeout' => 25));
    if (is_wp_error($response)) return array();
    $data = json_decode(wp_remote_retrieve_body($response), true);
    return is_array($data) ? $data : array();
}

function lvay_bb_archive_nav($url, $selected) {
    $out = '<aside class="lvay-bb-archive"><h3>SEASON ARCHIVES</h3>';
    foreach (array(2026, 2025) as $year) {
        $href = $year === 2026 ? $url : add_query_arg('season', $year, $url);
        $out .= '<a' . ($year === $selected ? ' class="active"' : '') . ' href="' . esc_url($href) . '">' . $year . '</a>';
    }
    return $out . '<span>More seasons will be added as they are digitized.</span></aside>';
}

function lvay_bb_empty_card($eyebrow, $title, $text, $archive_url, $archive_label) {
    return '<section class="lvay-bb-empty"><span>' . esc_html($eyebrow) . '</span>'
        . '<h2>' . esc_html($title) . '</h2><p>' . esc_html($text) . '</p>'
        . '<a href="' . esc_url($archive_url) . '">' . esc_html($archive_label) . '</a></section>';
}

function lvay_bb_class_label($class) {
    $class = strtoupper(trim((string) $class));
    if ($class === 'B' || $class === 'CLASS B') return 'Class B';
    if ($class === 'C' || $class === 'CLASS C') return 'Class C';
    return $class;
}

function lvay_bb_schedule_page($season) {
    $url = 'https://louisianavsallyall.com/baseball-schedules/';
    $data = lvay_bb_get('/api/schedules/baseball?season=' . $season . '&summary=1');
    $schools = (
        isset($data['season'], $data['schools'])
        && (int) $data['season'] === $season
        && is_array($data['schools'])
    ) ? $data['schools'] : array();

    $main = '<main class="lvay-bb-main"><header class="lvay-bb-title"><h1>'
        . $season . ' LHSAA<br>BASEBALL SCHEDULES</h1></header>';
    if (!$schools) {
        $main .= lvay_bb_empty_card(
            'SCHEDULES COMING SOON',
            'Baseball schedules are not available yet.',
            'This page is ready and will populate when the season schedules enter the LVAY system.',
            add_query_arg('season', 2025, $url),
            'View the archived 2025 schedules'
        );
    } else {
        $groups = array();
        foreach ($schools as $school) {
            $class = lvay_bb_class_label($school['class_'] ?? '');
            $district = (string) ($school['district'] ?? '');
            $groups[$class][$district][] = $school;
        }
        $order = array('5A', '4A', '3A', '2A', '1A', 'Class B', 'Class C');
        $main .= '<div class="lvay-bb-search-wrap"><label class="screen-reader-text" for="lvay-bb-search">Search for a school</label>'
            . '<input id="lvay-bb-search" type="search" placeholder="Search for a school..." autocomplete="off">'
            . '<div id="lvay-bb-results" role="listbox" hidden></div></div>'
            . '<p id="lvay-bb-status" aria-live="polite"></p><div class="lvay-bb-classes">';
        foreach ($order as $class) {
            if (empty($groups[$class])) continue;
            uksort($groups[$class], 'strnatcasecmp');
            $main .= '<details class="lvay-bb-class"><summary><i>&rsaquo;</i>' . esc_html($class) . '</summary><div>';
            foreach ($groups[$class] as $district => $district_schools) {
                usort($district_schools, function($a, $b) {
                    return strcasecmp($a['school'], $b['school']);
                });
                $district_label = ($class === 'Class B' || $class === 'Class C')
                    ? $class
                    : $district . '-' . $class;
                $main .= '<details class="lvay-bb-district"><summary><i>&rsaquo;</i>'
                    . esc_html($district_label) . '</summary><div>';
                foreach ($district_schools as $school) {
                    $name = (string) $school['school'];
                    $key = sanitize_title($name);
                    $record = (int) ($school['wins'] ?? 0) . '-' . (int) ($school['losses'] ?? 0)
                        . (!empty($school['ties']) ? '-' . (int) $school['ties'] : '');
                    $main .= '<article class="lvay-bb-school" id="bb-school-' . esc_attr($key)
                        . '" data-name="' . esc_attr(strtolower($name)) . '" data-label="'
                        . esc_attr($name) . '" data-district="' . esc_attr($district_label) . '">'
                        . '<button type="button" class="lvay-bb-school-toggle" aria-expanded="false">'
                        . '<b>' . esc_html($name) . '</b><span>' . esc_html($record) . '</span></button><div class="lvay-bb-school-body" hidden></div></article>';
                }
                $main .= '</div></details>';
            }
            $main .= '</div></details>';
        }
        $main .= '</div>';
    }
    $main .= '</main>';
    return '<section class="lvay-bb-layout" data-season="' . esc_attr($season) . '">'
        . $main . lvay_bb_archive_nav($url, $season) . '</section>';
}

function lvay_bb_rankings_table($rankings, $season) {
    $groups = array();
    foreach ($rankings as $row) {
        $groups[$row['division'] ?? 'Unknown'][] = $row;
    }
    $divisions = $season >= 2027
        ? array('Division I', 'Division II', 'Division III', 'Division IV', 'Class B', 'Class C')
        : array(
            'Non-Select Division I', 'Non-Select Division II', 'Non-Select Division III',
            'Non-Select Division IV', 'Select Division I', 'Select Division II',
            'Select Division III', 'Select Division IV', 'Class B', 'Class C'
        );
    $out = '<div class="lvay-bb-rankings">';
    foreach ($divisions as $division) {
        $rows = $groups[$division] ?? array();
        usort($rows, function($a, $b) {
            return ((float) ($b['power_rating'] ?? 0)) <=> ((float) ($a['power_rating'] ?? 0));
        });
        $out .= '<details><summary><i>&rsaquo;</i>' . esc_html($division) . '</summary><div class="lvay-bb-table-scroll">';
        if (!$rows) {
            $out .= '<p>No data available.</p>';
        } else {
            $out .= '<table><thead><tr><th>#</th><th>Team</th><th>Class</th><th>Record</th><th>GP</th><th>PR</th><th>SF</th></tr></thead><tbody>';
            foreach ($rows as $index => $row) {
                $record = (int) ($row['wins'] ?? 0) . '-' . (int) ($row['losses'] ?? 0);
                if (!empty($row['ties'])) $record .= '-' . (int) $row['ties'];
                $schedule = add_query_arg(array('season' => $season, 'school' => $row['school']), 'https://louisianavsallyall.com/baseball-schedules/')
                    . '#bb-school-' . sanitize_title($row['school']);
                $out .= '<tr><td>' . ($index + 1) . '</td><td><a href="' . esc_url($schedule) . '">'
                    . esc_html($row['school']) . '</a></td><td>' . esc_html(lvay_bb_class_label($row['class_'] ?? ''))
                    . '</td><td>' . esc_html($record) . '</td><td>' . (int) ($row['games_played'] ?? 0)
                    . '</td><td>' . number_format((float) ($row['power_rating'] ?? 0), 2)
                    . '</td><td>' . number_format((float) ($row['strength_factor'] ?? 0), 2) . '</td></tr>';
            }
            $out .= '</tbody></table>';
        }
        $out .= '</div></details>';
    }
    return $out . '</div>';
}

function lvay_bb_rankings_page($season) {
    $url = 'https://louisianavsallyall.com/baseball/power-ratings/';
    $data = lvay_bb_get('/api/rankings/baseball?season=' . $season);
    $rows = (
        isset($data['season'], $data['rankings'])
        && (int) $data['season'] === $season
        && is_array($data['rankings'])
    ) ? $data['rankings'] : array();
    $main = '<main class="lvay-bb-main"><header class="lvay-bb-title"><h1>'
        . $season . ' LHSAA<br>BASEBALL POWER RATINGS</h1></header>';
    if ($rows) {
        $main .= lvay_bb_rankings_table($rows, $season);
    } else {
        $main .= lvay_bb_empty_card(
            'PRESEASON',
            'Power ratings begin after games are played.',
            'Ratings will populate automatically when official scores enter the LVAY system.',
            add_query_arg('season', 2025, $url),
            'View the final 2025 ratings'
        );
    }
    return '<section class="lvay-bb-layout">' . $main . '</main>'
        . lvay_bb_archive_nav($url, $season) . '</section>';
}

function lvay_bb_brackets_page($season) {
    $url = 'https://louisianavsallyall.com/baseball-playoff-brackets/';
    $main = '<main class="lvay-bb-main"><header class="lvay-bb-title"><h1>'
        . $season . ' LHSAA<br>BASEBALL PLAYOFF BRACKETS</h1></header>';
    if ($season === 2025) {
        $pdf = 'https://www.lhsaa.org/siteuploads/editorimg/file/Baseball/2024-25/brackets%2025.pdf';
        $divisions = array(
            'Non-Select Division I', 'Non-Select Division II', 'Non-Select Division III',
            'Non-Select Division IV', 'Select Division I', 'Select Division II',
            'Select Division III', 'Select Division IV', 'Class B', 'Class C'
        );
        $main .= '<p class="lvay-bb-source">Complete official LHSAA brackets, including every round and championship result.</p><div class="lvay-bb-brackets">';
        foreach ($divisions as $index => $division) {
            $page = $index + 1;
            $src = $pdf . '#page=' . $page . '&view=FitH';
            $main .= '<details><summary><i>&rsaquo;</i>' . esc_html($division) . '</summary><div>'
                . '<a class="lvay-bb-official" target="_blank" rel="noopener" href="' . esc_url($src) . '">Open official completed bracket</a>'
                . '<iframe loading="lazy" title="' . esc_attr('2025 ' . $division . ' baseball bracket')
                . '" src="' . esc_url($src) . '"></iframe></div></details>';
        }
        $main .= '</div>';
    } else {
        $main .= lvay_bb_empty_card(
            'COMING THIS POSTSEASON',
            'The 2026 playoff brackets will appear here.',
            'The completed 2025 brackets remain preserved in the Season Archives.',
            add_query_arg('season', 2025, $url),
            'View the 2025 playoff brackets'
        );
    }
    return '<section class="lvay-bb-layout">' . $main . '</main>'
        . lvay_bb_archive_nav($url, $season) . '</section>';
}

function lvay_bb_replace_page($content) {
    if (!in_the_loop() || !is_main_query()) return $content;
    $season = lvay_bb_season();
    if (is_page(22706)) return lvay_bb_schedule_page($season);
    if (is_page(22710)) return lvay_bb_rankings_page($season);
    if (is_page(22711)) return lvay_bb_brackets_page($season);
    return $content;
}
add_filter('the_content', 'lvay_bb_replace_page', 999);

function lvay_bb_assets() {
    if (!is_page(array(22706, 22710, 22711))) return;
    wp_enqueue_style('lvay-fonts', 'https://fonts.googleapis.com/css2?family=Alfa+Slab+One&family=Teko:wght@400;500;600;700&display=swap', array(), null);
    wp_register_style('lvay-baseball-pages', false);
    wp_enqueue_style('lvay-baseball-pages');
    $css = <<<'CSS'
.lvay-bb-layout{display:grid;grid-template-columns:minmax(0,1fr) 300px;gap:24px;width:min(1600px,calc(100vw - 48px));max-width:none;margin:0;position:relative;left:50%;transform:translateX(-50%);padding:18px 0 36px}
.lvay-bb-main{min-width:0}.lvay-bb-title h1{margin:0 0 17px;color:#078b88;font-family:"Alfa Slab One",serif;font-size:clamp(32px,3.4vw,52px);line-height:.95;text-transform:uppercase}
.lvay-bb-archive{align-self:start;background:#050505;color:#fff;padding:22px 25px 26px;display:grid;grid-template-columns:1fr 1fr;gap:8px 18px}
.lvay-bb-archive h3{grid-column:1/-1;margin:0 0 9px;color:#fff;font-family:Teko,Arial,sans-serif;font-size:36px;font-weight:500;line-height:1;text-decoration:underline;text-underline-offset:5px}
.lvay-bb-archive a{color:#666!important;font-family:Teko,Arial,sans-serif;font-size:29px;font-weight:500;line-height:1;text-decoration:none!important}.lvay-bb-archive a:hover,.lvay-bb-archive a.active{color:#fff!important}
.lvay-bb-archive span{grid-column:1/-1;margin-top:12px;color:#999;font-family:Teko,Arial,sans-serif;font-size:21px;line-height:1.15}
.lvay-bb-search-wrap{position:relative;z-index:30}.lvay-bb-search-wrap input{display:block;width:100%;height:50px;padding:0 15px;border:2px solid #078b88;border-radius:4px;background:#fff;font-size:17px}
#lvay-bb-results{position:absolute;z-index:9999;top:100%;left:0;right:0;max-height:420px;overflow-y:auto;background:#fff;border:1px solid #078b88;box-shadow:0 10px 24px rgba(0,0,0,.22)}
#lvay-bb-results button{display:flex;width:100%;justify-content:space-between;padding:10px 14px;border:0;border-bottom:1px solid #e1e4e4;background:#fff;color:#111;font-family:Teko,Arial,sans-serif;font-size:24px;text-align:left;cursor:pointer}
#lvay-bb-results button:hover,#lvay-bb-results button[aria-selected=true]{background:#050505;color:#fff}
#lvay-bb-status{min-height:15px;margin:3px 0 8px;color:#666;font-size:13px}
.lvay-bb-class,.lvay-bb-district,.lvay-bb-school,.lvay-bb-rankings details,.lvay-bb-brackets details{border-bottom:1px solid #d9dddd;background:#fff}
.lvay-bb-class>summary,.lvay-bb-district>summary,.lvay-bb-rankings summary,.lvay-bb-brackets summary{display:flex;align-items:center;gap:8px;cursor:pointer;list-style:none;color:#090909;font-family:"Alfa Slab One",serif}
.lvay-bb-class>summary::-webkit-details-marker,.lvay-bb-district>summary::-webkit-details-marker,.lvay-bb-rankings summary::-webkit-details-marker,.lvay-bb-brackets summary::-webkit-details-marker{display:none}
.lvay-bb-class>summary{min-height:68px;padding:10px 15px;font-size:31px;border-bottom:2px solid #078b88}.lvay-bb-district>summary{min-height:48px;padding:8px 30px;font-size:23px}
.lvay-bb-class summary i,.lvay-bb-district summary i,.lvay-bb-rankings summary i,.lvay-bb-brackets summary i{color:#45b8b2;font-family:Arial,sans-serif;font-size:34px;font-style:normal;line-height:.7;transition:transform .15s}
.lvay-bb-class[open]>summary i,.lvay-bb-district[open]>summary i,.lvay-bb-rankings details[open]>summary i,.lvay-bb-brackets details[open]>summary i{transform:rotate(90deg)}
.lvay-bb-school-toggle{display:flex;justify-content:space-between;gap:16px;width:100%;min-height:50px;padding:8px 45px;border:0;border-bottom:1px solid #e7e9e9;background:#fff;color:#080808;font-family:Teko,Arial,sans-serif;font-size:25px;text-align:left;cursor:pointer}
.lvay-bb-school-toggle:hover{background:#050505;color:#fff}.lvay-bb-school-toggle[aria-expanded=true]{background:#333;color:#fff}
.lvay-bb-school-body{padding:0 0 18px}.lvay-bb-meta{display:flex;gap:24px;flex-wrap:wrap;padding:9px 13px;background:#078b88;color:#fff;font-family:Teko,Arial,sans-serif;font-size:22px}
.lvay-bb-table-scroll{max-width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch}.lvay-bb-table-scroll table{width:100%;min-width:850px;border-collapse:collapse;font-size:15px}
.lvay-bb-table-scroll th{padding:9px 10px;background:#078b88;color:#fff;text-align:left}.lvay-bb-table-scroll td{padding:8px 10px;border-bottom:1px solid #e2e5e5}.lvay-bb-table-scroll tr:nth-child(even){background:#f2f3f3}
.lvay-bb-table-scroll tr.district td{color:#078b88;font-weight:700}.lvay-bb-table-scroll td.win{color:#009b35;font-weight:800}.lvay-bb-table-scroll td.loss{color:#df2027;font-weight:800}.lvay-bb-table-scroll a{color:inherit!important;text-decoration:none!important}
.lvay-bb-rankings summary,.lvay-bb-brackets summary{min-height:74px;padding:11px 16px;font-size:clamp(24px,2.25vw,36px)}.lvay-bb-rankings details[open]>summary,.lvay-bb-brackets details[open]>summary{background:#078b88;color:#fff}
.lvay-bb-rankings table{min-width:700px}.lvay-bb-rankings td:nth-child(2) a{color:#078b88!important;font-weight:800}.lvay-bb-rankings p{padding:14px}
.lvay-bb-empty{border-top:8px solid #078b88;background:#f1f5f5;padding:35px 38px;margin-top:18px}.lvay-bb-empty>span{color:#078b88;font-family:Teko,Arial,sans-serif;font-size:25px;font-weight:700;letter-spacing:2px}
.lvay-bb-empty h2{margin:3px 0 10px;color:#080808;font-family:"Alfa Slab One",serif;font-size:clamp(27px,3vw,42px);line-height:1.05}.lvay-bb-empty p{max-width:740px;font-size:17px;line-height:1.5}
.lvay-bb-empty a,.lvay-bb-official{display:inline-block;margin-top:8px;background:#078b88;color:#fff!important;padding:10px 17px;font-weight:800;text-decoration:none!important}
.lvay-bb-source{margin:4px 0 18px;color:#555;font-size:17px}.lvay-bb-brackets details>div{padding:14px 0 24px}.lvay-bb-official{margin:0 0 12px;background:#050505}
.lvay-bb-brackets iframe{display:block;width:100%;height:1150px;border:1px solid #cfd5d5;background:#fff}
@media(max-width:1200px){.lvay-bb-layout{grid-template-columns:1fr}.lvay-bb-archive{grid-row:1}.lvay-bb-main{grid-row:2}}
@media(max-width:700px){.lvay-bb-layout{width:calc(100vw - 24px);padding-top:8px}.lvay-bb-archive{padding:17px 19px}.lvay-bb-class>summary{font-size:26px}.lvay-bb-district>summary{padding-left:18px}.lvay-bb-school-toggle{padding-left:26px}.lvay-bb-empty{padding:25px 20px}.lvay-bb-brackets iframe{height:850px}}
CSS;
    wp_add_inline_style('lvay-baseball-pages', $css);

    if (is_page(22706)) {
        wp_register_script('lvay-baseball-schedules', false, array(), null, true);
        wp_enqueue_script('lvay-baseball-schedules');
        $js = <<<'JS'
(function(){
  const root=document.querySelector('.lvay-bb-layout[data-season]'); if(!root)return;
  const season=root.dataset.season, search=document.getElementById('lvay-bb-search');
  const results=document.getElementById('lvay-bb-results'), status=document.getElementById('lvay-bb-status');
  const schools=Array.from(root.querySelectorAll('.lvay-bb-school'));
  let active=-1;
  function matches(q){q=q.trim().toLowerCase();return q?schools.filter(x=>x.dataset.name.includes(q)).slice(0,12):[]}
  function draw(q){
    const found=matches(q); active=-1; results.innerHTML='';
    found.forEach((school,i)=>{const b=document.createElement('button');b.type='button';b.setAttribute('role','option');b.dataset.target=school.id;b.innerHTML='<span>'+school.dataset.label+'</span><small>'+school.dataset.district+'</small>';b.addEventListener('click',()=>openSchool(school));results.appendChild(b)});
    results.hidden=!found.length; status.textContent=q.trim()?(found.length+' match'+(found.length===1?'':'es')):'';
  }
  function esc(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
  function div(v){const map={'Non-Select Division I':'NSI','Non-Select Division II':'NSII','Non-Select Division III':'NSIII','Non-Select Division IV':'NSIV','Select Division I':'SI','Select Division II':'SII','Select Division III':'SIII','Select Division IV':'SIV'};return map[v]||v||''}
  function date(v){if(!v)return '—';const m=String(v).match(/^(\d{4})-(\d\d)-(\d\d)$/);return m?(+m[2])+'/'+(+m[3])+'/'+m[1]:v}
  function render(data){
    const s=data.schools&&data.schools[0];if(!s)return '<p>Schedule details are not available.</p>';
    const games=s.games||[];let rows='';
    games.forEach((g,i)=>{const r=(g.result||'').toUpperCase(),district=!!g.is_district,opp=g.opponent||'',rec=(g.opp_wins!=null&&g.opp_losses!=null)?' ('+g.opp_wins+'-'+g.opp_losses+(g.opp_ties?'-'+g.opp_ties:'')+')':'';
      const href='?season='+encodeURIComponent(season)+'&school='+encodeURIComponent(opp)+'#bb-school-'+opp.toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/(^-|-$)/g,'');
      rows+='<tr class="'+(district?'district':'')+'"><td>'+(g.week?'Gm '+esc(g.week):'Gm '+(i+1))+(district?' D':'')+'</td><td>'+esc(date(g.game_date))+'</td><td>'+esc(g.home_away||'')+'</td><td><a href="'+href+'">'+esc(opp+rec)+'</a></td><td>'+(district?'D':'')+'</td><td>'+esc(div(g.opp_division))+'</td><td class="'+(r==='W'?'win':r==='L'?'loss':'')+'">'+esc(r)+'</td><td>'+esc(g.score||'')+'</td><td>'+esc(g.total_pts==null?'':Number(g.total_pts).toFixed(2))+'</td></tr>'});
    return '<div class="lvay-bb-meta"><strong>'+esc(s.district+'-'+lvClass(s.class_))+'</strong><span>'+esc(s.division||'')+'</span><span>Overall: '+esc(s.record||((s.wins||0)+'-'+(s.losses||0)))+'</span><span>PR: '+Number(s.power_rating||0).toFixed(2)+'</span></div><div class="lvay-bb-table-scroll"><table><thead><tr><th>Game</th><th>Date</th><th>H/A</th><th>Opponent</th><th>District</th><th>Division</th><th>W/L</th><th>Score</th><th>Power Pts</th></tr></thead><tbody>'+rows+'</tbody></table></div>'
  }
  function lvClass(c){c=String(c||'').toUpperCase();return c==='B'||c==='CLASS B'?'Class B':c==='C'||c==='CLASS C'?'Class C':c}
  async function openSchool(school){
    results.hidden=true; search.value=school.dataset.label;
    school.closest('.lvay-bb-class').open=true;school.closest('.lvay-bb-district').open=true;
    const button=school.querySelector('.lvay-bb-school-toggle'),body=school.querySelector('.lvay-bb-school-body');
    schools.forEach(x=>{if(x!==school){x.querySelector('.lvay-bb-school-toggle').setAttribute('aria-expanded','false');x.querySelector('.lvay-bb-school-body').hidden=true}});
    button.setAttribute('aria-expanded','true');body.hidden=false;
    if(!body.dataset.loaded){body.innerHTML='<p>Loading schedule...</p>';try{const r=await fetch('https://lvay-scraper.onrender.com/api/schedules/baseball?season='+encodeURIComponent(season)+'&school='+encodeURIComponent(school.dataset.label));body.innerHTML=render(await r.json());body.dataset.loaded='1'}catch(e){body.innerHTML='<p>Schedule details are temporarily unavailable.</p>'}}
    school.scrollIntoView({behavior:'smooth',block:'center'});
  }
  if(search){search.addEventListener('input',()=>draw(search.value));search.addEventListener('keydown',e=>{const opts=Array.from(results.querySelectorAll('button'));if(e.key==='ArrowDown'||e.key==='ArrowUp'){e.preventDefault();active=(active+(e.key==='ArrowDown'?1:-1)+opts.length)%opts.length;opts.forEach((x,i)=>x.setAttribute('aria-selected',i===active?'true':'false'))}else if(e.key==='Enter'&&active>=0){e.preventDefault();opts[active].click()}else if(e.key==='Escape'){results.hidden=true}})}
  root.addEventListener('click',e=>{const b=e.target.closest('.lvay-bb-school-toggle');if(b)openSchool(b.closest('.lvay-bb-school'))});
  document.addEventListener('click',e=>{if(search&&!e.target.closest('.lvay-bb-search-wrap'))results.hidden=true});
  const requested=new URLSearchParams(location.search).get('school');if(requested){const target=schools.find(x=>x.dataset.label.toLowerCase()===requested.toLowerCase());if(target)openSchool(target)}
})();
JS;
        wp_add_inline_script('lvay-baseball-schedules', $js);
    }
}
add_action('wp_enqueue_scripts', 'lvay_bb_assets', 50);
