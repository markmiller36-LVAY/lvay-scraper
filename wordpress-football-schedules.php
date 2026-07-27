<?php
/**
 * LVAY season-aware Football schedules.
 *
 * Shortcode:
 *   [lvay_football_schedules]
 *   [lvay_football_schedules season="2025"]
 *
 * The query string takes precedence, so archive links use ?season=2025.
 * Paste into Code Snippets without this opening PHP tag.
 */

function lvay_football_schedule_api_url_v5($path) {
    return 'https://lvay-scraper.onrender.com' . $path;
}

function lvay_football_schedule_fetch_v5($path) {
    $response = wp_remote_get(
        lvay_football_schedule_api_url_v5($path),
        array('timeout' => 25)
    );
    if (is_wp_error($response)) {
        return null;
    }
    $body = json_decode(wp_remote_retrieve_body($response), true);
    return is_array($body) ? $body : null;
}

function lvay_football_schedule_shortcode_v5($atts) {
    $atts = shortcode_atts(array('season' => ''), $atts);
    $requested = isset($_GET['season'])
        ? sanitize_text_field(wp_unslash($_GET['season']))
        : $atts['season'];
    $season = (
        preg_match('/^\d{4}$/', $requested)
        && in_array((int) $requested, array(2022, 2023, 2024, 2025, 2026), true)
    ) ? $requested : '2026';

    $schedule = lvay_football_schedule_fetch_v5(
        '/api/schedules/football?season=' . rawurlencode($season) . '&summary=1'
    );
    $seasons_data = lvay_football_schedule_fetch_v5('/api/seasons/football');
    if (!$schedule || !isset($schedule['schools'])) {
        return '<p class="lvay-schedule-error">Schedules are temporarily unavailable.</p>';
    }

    $schools = $schedule['schools'];
    $classes = array('5A', '4A', '3A', '2A', '1A');
    $available = array();
    if ($seasons_data && !empty($seasons_data['seasons'])) {
        foreach ($seasons_data['seasons'] as $item) {
            $available[(string) $item['season']] = $item;
        }
    }
    if (!isset($available[$season])) {
        $available[$season] = array('season' => $season);
    }

    $page_url = remove_query_arg(array('season', 'school'));
    ob_start();
    ?>
    <section id="lvay-football-schedules"
             class="lvay-football-schedules"
             data-season="<?php echo esc_attr($season); ?>">
        <div class="lvay-schedule-main">
            <header class="lvay-schedule-title">
                <h1><?php echo esc_html($season); ?> LHSAA<br>FOOTBALL SCHEDULES</h1>
                <?php if (($schedule['status'] ?? '') === 'preseason'): ?>
                    <p>Preseason schedules — dates and opponents remain subject to change.</p>
                <?php endif; ?>
            </header>

            <label class="screen-reader-text" for="lvay-school-search">
                Search for a school
            </label>
            <input id="lvay-school-search" type="search"
                   placeholder="Search for a school…"
                   autocomplete="off">
            <p id="lvay-search-status" class="lvay-search-status" aria-live="polite"></p>

            <div class="lvay-class-list">
                <?php foreach ($classes as $class_name):
                    $class_schools = array_values(array_filter(
                        $schools,
                        function($school) use ($class_name) {
                            return strtoupper((string) ($school['class_'] ?? '')) === $class_name;
                        }
                    ));
                    if (!$class_schools) continue;
                    $districts = array();
                    foreach ($class_schools as $school) {
                        $district = (string) ($school['district'] ?? '');
                        $districts[$district][] = $school;
                    }
                    uksort($districts, 'strnatcasecmp');
                    ?>
                    <details class="lvay-class" data-class="<?php echo esc_attr($class_name); ?>">
                        <summary><span class="lvay-caret">›</span><?php echo esc_html($class_name); ?></summary>
                        <div class="lvay-district-list">
                            <?php foreach ($districts as $district => $district_schools): ?>
                                <details class="lvay-district">
                                    <summary>
                                        <span class="lvay-caret">›</span>
                                        <?php echo esc_html($district . '-' . $class_name); ?>
                                    </summary>
                                    <div class="lvay-school-list">
                                        <?php foreach ($district_schools as $school):
                                            $school_name = $school['school'];
                                            $school_key = sanitize_title($school_name);
                                            $record = !empty($school['games_played'])
                                                ? ($school['record'] ?? '')
                                                : '';
                                            ?>
                                            <article class="lvay-school"
                                                id="school-<?php echo esc_attr($school_key); ?>"
                                                data-school="<?php echo esc_attr(strtolower($school_name)); ?>">
                                                <button class="lvay-school-toggle"
                                                        type="button"
                                                        aria-expanded="false">
                                                    <span><?php echo esc_html($school_name); ?></span>
                                                    <?php if ($record): ?>
                                                        <small><?php echo esc_html($record); ?></small>
                                                    <?php endif; ?>
                                                </button>
                                                <div class="lvay-school-body" hidden>
                                                    <template class="lvay-school-template">
                                                    <div class="lvay-school-meta">
                                                        <strong><?php echo esc_html($district . '-' . $class_name); ?></strong>
                                                        <span><?php echo esc_html($school['source_division'] ?? $school['division'] ?? ''); ?></span>
                                                        <?php if ($record): ?>
                                                            <span>Overall: <?php echo esc_html($record); ?></span>
                                                        <?php endif; ?>
                                                        <?php if ($school['power_rating'] !== null): ?>
                                                            <span>PR: <?php echo esc_html(number_format((float) $school['power_rating'], 2)); ?></span>
                                                        <?php endif; ?>
                                                    </div>
                                                    <div class="lvay-table-scroll">
                                                        <table>
                                                            <thead>
                                                                <tr>
                                                                    <th>Week</th>
                                                                    <th>Date</th>
                                                                    <th>H/A</th>
                                                                    <th>Opponent</th>
                                                                    <th>District</th>
                                                                    <th>Division</th>
                                                                    <th>W/L</th>
                                                                    <th>Score</th>
                                                                    <th>Power Pts</th>
                                                                </tr>
                                                            </thead>
                                                            <tbody>
                                                                <?php foreach (($school['games'] ?? array()) as $game):
                                                                    $opponent = $game['opponent'] ?? '';
                                                                    $internal = !empty($game['opponent_internal']);
                                                                    $opponent_url = add_query_arg(
                                                                        array(
                                                                            'season' => $season,
                                                                            'school' => $opponent,
                                                                        ),
                                                                        $page_url
                                                                    ) . '#school-' . sanitize_title($opponent);
                                                                    $opponent_record = '';
                                                                    if (isset($game['opp_wins'], $game['opp_losses'])) {
                                                                        $opponent_record = ' (' . $game['opp_wins'] . '-' . $game['opp_losses'] . ')';
                                                                    }
                                                                    $display_date = $game['game_date'] ?? '';
                                                                    if (preg_match('/^(\d{4})-(\d{2})-(\d{2})$/', $display_date, $date_parts)) {
                                                                        $display_date = ((int) $date_parts[2]) . '/' . ((int) $date_parts[3]) . '/' . $date_parts[1];
                                                                    }
                                                                    ?>
                                                                    <tr class="<?php echo !empty($game['is_district']) ? 'is-district' : 'is-nondistrict'; ?>">
                                                                        <td><?php echo esc_html('Wk' . ($game['week'] ?? '') . (!empty($game['is_district']) ? ' D' : '')); ?></td>
                                                                        <td><?php echo esc_html($display_date ?: '—'); ?></td>
                                                                        <td><?php echo esc_html($game['home_away'] ?? ''); ?></td>
                                                                        <td>
                                                                            <?php if ($internal && $opponent): ?>
                                                                                <a href="<?php echo esc_url($opponent_url); ?>">
                                                                                    <?php echo esc_html($opponent . $opponent_record); ?>
                                                                                </a>
                                                                            <?php else: ?>
                                                                                <?php echo esc_html($opponent . $opponent_record); ?>
                                                                            <?php endif; ?>
                                                                        </td>
                                                                        <td><?php echo !empty($game['is_district']) ? 'D' : ''; ?></td>
                                                                        <td><?php echo esc_html($game['opp_division'] ?? $game['district_class'] ?? ''); ?></td>
                                                                        <td class="result-<?php echo esc_attr(strtolower($game['result'] ?? '')); ?>"><?php echo esc_html($game['result'] ?? ''); ?></td>
                                                                        <td><?php echo esc_html($game['score'] ?? ''); ?></td>
                                                                        <td><?php echo isset($game['total_pts']) ? esc_html(number_format((float) $game['total_pts'], 2)) : ''; ?></td>
                                                                    </tr>
                                                                <?php endforeach; ?>
                                                            </tbody>
                                                        </table>
                                                    </div>
                                                    </template>
                                                </div>
                                            </article>
                                        <?php endforeach; ?>
                                    </div>
                                </details>
                            <?php endforeach; ?>
                        </div>
                    </details>
                <?php endforeach; ?>
            </div>
        </div>

        <aside class="lvay-season-archives" aria-label="Season Archives">
            <h2>SEASON ARCHIVES</h2>
            <div class="lvay-season-grid">
                <?php
                // Only publish seasons whose archive is actually preserved.
                $archive_years = array(2025, 2024, 2023, 2022);
                foreach ($archive_years as $year):
                    $enabled = isset($available[(string) $year]);
                    $selected = (string) $year === $season;
                    if ($enabled):
                        $url = add_query_arg('season', (string) $year, $page_url);
                        ?>
                        <a class="<?php echo $selected ? 'is-current' : ''; ?>"
                           href="<?php echo esc_url($url); ?>">
                            <?php echo esc_html($year); ?>
                        </a>
                    <?php else: ?>
                        <span><?php echo esc_html($year); ?></span>
                    <?php endif;
                endforeach; ?>
            </div>
            <p class="lvay-archive-note">More seasons will be added as they are digitized.</p>
        </aside>
    </section>

    <style>
    .page-id-10379 .elementor-element-55d0a63{display:none!important}
    .lvay-football-schedules{--teal:#078b88;--ink:#080808;display:grid;grid-template-columns:minmax(0,1fr) 340px;gap:20px;max-width:1240px;margin:0 auto;padding:18px 0 36px}
    .lvay-schedule-title h1{margin:0 0 8px;color:var(--teal);font-family:"Alfa Slab One",Rockwell,serif;font-size:36px;line-height:.95;letter-spacing:.2px}
    .lvay-schedule-title p{margin:0 0 12px;color:#666;font-family:"Teko",sans-serif;font-size:18px}
    #lvay-school-search{width:100%;height:38px;margin:5px 0 12px;padding:7px 12px;border:1px solid var(--teal);border-radius:3px;background:#fff}
    .lvay-search-status{min-height:16px;margin:0;color:#777;font-size:12px}
    .lvay-class,.lvay-district{border:0;margin:0}
    .lvay-class>summary,.lvay-district>summary{display:flex;align-items:center;gap:7px;cursor:pointer;list-style:none;font-family:"Alfa Slab One",Rockwell,serif}
    .lvay-class>summary{padding:9px 0 6px;border-bottom:2px solid var(--teal);font-size:18px}
    .lvay-district>summary{padding:8px 14px;border-bottom:1px solid #e4e4e4;font-size:14px}
    summary::-webkit-details-marker{display:none}
    .lvay-caret{display:inline-block;color:#5dc7c1;font-family:Arial,sans-serif;font-size:24px;line-height:.7;transform:rotate(0)}
    details[open]>summary .lvay-caret{transform:rotate(90deg)}
    .lvay-school{border-bottom:1px solid #e9e9e9}
    .lvay-school-toggle{display:flex;width:100%;justify-content:space-between;align-items:center;padding:8px 18px;border:0;background:#fff;color:#111;text-align:left;cursor:pointer;font-family:"Teko",sans-serif;font-size:19px}
    .lvay-school-toggle:hover{background:#edf7f7;color:var(--teal)}
    .lvay-school-toggle small{font-size:15px}
    .lvay-school-body{padding:4px 10px 18px}
    .lvay-school-meta{display:flex;gap:18px;padding:5px 8px;background:var(--teal);color:#fff;font-family:"Teko",sans-serif;font-size:16px}
    .lvay-table-scroll{overflow-x:auto}
    .lvay-school table{width:100%;min-width:720px;border-collapse:collapse;font-family:"Teko",sans-serif;font-size:17px}
    .lvay-school th{padding:4px 7px;background:var(--teal);color:#fff;text-align:left;font-weight:600}
    .lvay-school td{padding:4px 7px;border-bottom:1px solid #e6e6e6}
    .lvay-school tr:nth-child(even) td{background:#f2f4f4}
    .lvay-school a{color:var(--teal);font-weight:600;text-decoration:none}
    .lvay-school tr.is-district td{color:var(--teal);font-weight:600}
    .lvay-school tr.is-district a{color:var(--teal);font-weight:600}
    .lvay-school tr.is-nondistrict td{color:#111;font-weight:400}
    .lvay-school tr.is-nondistrict a{color:#111;font-weight:400}
    .lvay-school td.result-w{color:#00a651!important;font-weight:700!important}
    .lvay-school td.result-l{color:#e53935!important;font-weight:700!important}
    .lvay-season-archives{align-self:start;padding:17px 26px 20px;background:#050505;color:#fff}
    .lvay-season-archives h2{margin:0 0 14px;text-align:center;text-decoration:underline;font-family:"Alfa Slab One",Rockwell,serif;font-size:25px}
    .lvay-season-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:7px 18px}
    .lvay-season-grid a,.lvay-season-grid span{font-family:"Teko",Arial,sans-serif;font-size:27px;font-weight:500;line-height:1.05;text-decoration:none}
    .lvay-season-grid a{color:#999}
    .lvay-season-grid a:hover{color:#fff}
    .lvay-season-grid a.is-current{color:#fff;text-decoration:underline}
    .lvay-season-grid span{color:#666}
    .lvay-archive-note{margin:12px 0 0;color:#999;font:400 20px/1.15 "Teko",Arial,sans-serif}
    @media(max-width:900px){.lvay-football-schedules{grid-template-columns:1fr}.lvay-season-archives{order:2}.lvay-schedule-title h1{font-size:29px}}
    </style>

    <script>
    (function(){
      const root=document.getElementById('lvay-football-schedules');
      if(!root)return;
      const search=root.querySelector('#lvay-school-search');
      const status=root.querySelector('#lvay-search-status');
      const schoolParam=new URLSearchParams(window.location.search).get('school');
      const normalize=v=>(v||'').toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();
      const esc=value=>String(value??'')
        .replaceAll('&','&amp;').replaceAll('<','&lt;')
        .replaceAll('>','&gt;').replaceAll('"','&quot;');
      const formatDate=value=>{
        const match=String(value||'').match(/^(\d{4})-(\d{2})-(\d{2})$/);
        return match ? Number(match[2])+'/'+Number(match[3])+'/'+match[1] : (value||'—');
      };
      const formatDivision=value=>String(value||'')
        .replace(/^Non-Select Division\s+/i,'NS')
        .replace(/^Select Division\s+/i,'S');
      async function renderSchoolBody(body){
        if(body.dataset.loaded==='1')return;
        if(body.dataset.loading==='1')return;
        body.dataset.loading='1';
        const article=body.closest('.lvay-school');
        const school=article.dataset.school;
        body.innerHTML='<p class="lvay-schedule-loading">Loading schedule…</p>';
        try{
          const url='https://lvay-scraper.onrender.com/api/schedules/football?season='
            +encodeURIComponent(root.dataset.season)+'&school='+encodeURIComponent(school);
          const response=await fetch(url);
          const payload=await response.json();
          const data=payload.schools&&payload.schools[0];
          if(!data)throw new Error('Schedule unavailable');
          const record=data.games_played?data.record:'';
          const division=data.source_division||data.division||'';
          let html='<div class="lvay-school-meta"><strong>'
            +esc(data.district+'-'+data.class_)+'</strong><span>'+esc(division)+'</span>';
          if(record)html+='<span>Overall: '+esc(record)+'</span>';
          if(data.power_rating!==null&&data.power_rating!==undefined){
            html+='<span>PR: '+Number(data.power_rating).toFixed(2)+'</span>';
          }
          html+='</div><div class="lvay-table-scroll"><table><thead><tr>'
            +'<th>Week</th><th>Date</th><th>H/A</th><th>Opponent</th>'
            +'<th>District</th><th>Division</th><th>W/L</th><th>Score</th><th>Power Pts</th>'
            +'</tr></thead><tbody>';
          (data.games||[]).forEach(game=>{
            const opponent=game.opponent||'';
            const hasRecord=game.opp_wins!==null&&game.opp_wins!==undefined
              &&game.opp_losses!==null&&game.opp_losses!==undefined;
            const opponentLabel=opponent+(hasRecord?' ('+game.opp_wins+'-'+game.opp_losses+')':'');
            let opponentHtml=esc(opponentLabel);
            if(game.opponent_internal&&opponent){
              const destination=new URL(window.location.href);
              destination.searchParams.set('season',root.dataset.season);
              destination.searchParams.set('school',opponent);
              destination.hash='school-'+normalize(opponent).replaceAll(' ','-');
              opponentHtml='<a href="'+esc(destination.toString())+'">'+esc(opponentLabel)+'</a>';
            }
            const points=game.total_pts===null||game.total_pts===undefined
              ? '' : Number(game.total_pts).toFixed(2);
            const rowClass=game.is_district?'is-district':'is-nondistrict';
            const resultClass='result-'+String(game.result||'').toLowerCase();
            const division=formatDivision(game.opp_division||game.district_class||'');
            const weekLabel=game.phase&&game.phase!=='Regular Season'
              ? game.phase
              : 'Wk'+esc(game.week||'')+(game.is_district?' D':'');
            html+='<tr class="'+rowClass+'"><td>'+esc(weekLabel)+'</td><td>'
              +esc(formatDate(game.game_date))+'</td><td>'+esc(game.home_away||'')
              +'</td><td>'+opponentHtml+'</td><td>'+(game.is_district?'D':'')
              +'</td><td>'+esc(division)+'</td><td class="'+resultClass+'">'
              +esc(game.result||'')+'</td><td>'+esc(game.score||'')
              +'</td><td>'+esc(points)+'</td></tr>';
          });
          body.innerHTML=html+'</tbody></table></div>';
          body.dataset.loaded='1';
        }catch(error){
          body.innerHTML='<p class="lvay-schedule-error">Schedule temporarily unavailable.</p>';
        }finally{
          delete body.dataset.loading;
        }
      }
      async function openSchool(article){
        root.querySelectorAll('.lvay-school-body').forEach(body=>body.hidden=true);
        root.querySelectorAll('.lvay-school-toggle').forEach(button=>button.setAttribute('aria-expanded','false'));
        let parent=article.parentElement;
        while(parent&&parent!==root){if(parent.tagName==='DETAILS')parent.open=true;parent=parent.parentElement}
        const body=article.querySelector('.lvay-school-body');
        const button=article.querySelector('.lvay-school-toggle');
        await renderSchoolBody(body);
        body.hidden=false;button.setAttribute('aria-expanded','true');
        window.setTimeout(()=>article.scrollIntoView({behavior:'smooth',block:'center'}),50);
      }
      root.querySelectorAll('.lvay-school-toggle').forEach(button=>{
        button.addEventListener('click',async()=>{
          const body=button.nextElementSibling;
          if(body.hidden)await renderSchoolBody(body);
          body.hidden=!body.hidden;
          button.setAttribute('aria-expanded',String(!body.hidden));
        });
      });
      root.addEventListener('click',async event=>{
        const link=event.target.closest('.lvay-school td a');
        if(!link)return;
        const destination=new URL(link.href,window.location.href);
        const opponent=destination.searchParams.get('school');
        if(!opponent)return;
        const target=Array.from(root.querySelectorAll('.lvay-school')).find(
          article=>normalize(article.dataset.school)===normalize(opponent)
        );
        if(!target)return;
        event.preventDefault();
        await openSchool(target);
        history.replaceState(null,'',destination.pathname+destination.search+destination.hash);
      });
      search.addEventListener('input',()=>{
        const query=normalize(search.value);
        let matches=0;
        root.querySelectorAll('.lvay-school').forEach(article=>{
          const match=!query||normalize(article.dataset.school).includes(query);
          article.hidden=!match;
          if(match&&query){matches++;let p=article.parentElement;while(p&&p!==root){if(p.tagName==='DETAILS')p.open=true;p=p.parentElement}}
        });
        status.textContent=query?matches+' school'+(matches===1?'':'s')+' found':'';
      });
      if(schoolParam){
        const target=Array.from(root.querySelectorAll('.lvay-school')).find(
          article=>normalize(article.dataset.school)===normalize(schoolParam)
        );
        if(target)openSchool(target);
      }
    })();
    </script>
    <?php
    return ob_get_clean();
}
add_shortcode('lvay_football_schedules', 'lvay_football_schedule_shortcode_v5');
