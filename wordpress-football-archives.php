/**
 * LVAY football season archives.
 *
 * Install as a Code Snippets PHP snippet and activate. This intentionally runs
 * after the existing football-ranking design filter so it can replace the
 * legacy "latest available season" output with an explicitly selected season.
 */

function lvay_archive_selected_season_v2() {
    $season = isset($_GET['season']) ? absint($_GET['season']) : 2026;
    return in_array($season, array(2025, 2026), true) ? $season : 2026;
}

function lvay_archive_nav_v2($page_url, $selected) {
    $years = array(2026, 2025);
    $out = '<aside class="lvay-season-archive"><h3>SEASON ARCHIVES</h3>';
    foreach ($years as $year) {
        $url = $year === 2026 ? $page_url : add_query_arg('season', $year, $page_url);
        $class = $year === $selected ? ' class="active"' : '';
        $out .= '<a' . $class . ' href="' . esc_url($url) . '">' . esc_html($year) . '</a>';
    }
    $out .= '<span class="coming">More seasons will be added as they are digitized.</span></aside>';
    return $out;
}

function lvay_archive_rankings_payload_v2($season) {
    $response = wp_remote_get(
        'https://lvay-scraper.onrender.com/api/rankings/football?season=' . absint($season),
        array('timeout' => 25)
    );
    if (is_wp_error($response)) return array();
    $data = json_decode(wp_remote_retrieve_body($response), true);
    return is_array($data) ? $data : array();
}

function lvay_archive_rankings_table_v2($rankings, $season) {
    $groups = array();
    foreach ($rankings as $school) {
        $division = isset($school['division']) ? $school['division'] : 'Unknown';
        $groups[$division][] = $school;
    }
    foreach ($groups as &$schools) {
        usort($schools, function($a, $b) {
            return ((float) $b['power_rating']) <=> ((float) $a['power_rating']);
        });
    }
    unset($schools);

    $tracks = array(
        array('Non-Select Division I', 'Non-Select Division II', 'Non-Select Division III', 'Non-Select Division IV'),
        array('Select Division I', 'Select Division II', 'Select Division III', 'Select Division IV'),
    );
    $out = '<div class="lvay lvay-cols">';
    foreach ($tracks as $divisions) {
        $out .= '<div>';
        foreach ($divisions as $division) {
            $schools = isset($groups[$division]) ? $groups[$division] : array();
            $out .= '<div class="lvay-acc"><div class="lvay-acc-hdr" onclick="lvayToggle(this)">';
            $out .= '<span class="lvay-arrow">&rsaquo;</span> ' . esc_html($division) . '</div>';
            $out .= '<div class="lvay-acc-body">';
            if (!$schools) {
                $out .= '<p>No data available.</p>';
            } else {
                $out .= '<div class="lvay-stbl-wrap"><table class="lvay-rtbl"><thead><tr>';
                $out .= '<th>#</th><th>Team</th><th>Class</th><th>Record</th><th>GP</th><th>PR</th><th>SF</th>';
                $out .= '</tr></thead><tbody>';
                foreach ($schools as $index => $school) {
                    $ties = isset($school['ties']) ? (int) $school['ties'] : 0;
                    $record = (int) $school['wins'] . '-' . (int) $school['losses'];
                    if ($ties) $record .= '-' . $ties;
                    $schedule_url = add_query_arg('season', $season, 'https://louisianavsallyall.com/schedules/');
                    $schedule_url .= '#' . sanitize_title($school['school']);
                    $out .= '<tr><td>' . ($index + 1) . '</td>';
                    $out .= '<td><a href="' . esc_url($schedule_url) . '">' . esc_html($school['school']) . '</a></td>';
                    $out .= '<td>' . esc_html($school['class_']) . '</td><td>' . esc_html($record) . '</td>';
                    $out .= '<td>' . esc_html($school['games_played']) . '</td>';
                    $out .= '<td>' . number_format((float) $school['power_rating'], 2) . '</td>';
                    $out .= '<td>' . number_format((float) $school['strength_factor'], 2) . '</td></tr>';
                }
                $out .= '</tbody></table></div>';
            }
            $out .= '</div></div>';
        }
        $out .= '</div>';
    }
    return $out . '</div>';
}

function lvay_archive_rankings_output_v2($output, $tag, $attr, $match) {
    if ($tag !== 'lvay_power_rankings') return $output;

    $season = lvay_archive_selected_season_v2();
    $data = lvay_archive_rankings_payload_v2($season);
    $rankings = (
        !empty($data['rankings'])
        && isset($data['season'])
        && (int) $data['season'] === $season
    ) ? $data['rankings'] : array();

    $heading = '<div class="lvay-rankings-heading">' . esc_html($season)
        . ' LHSAA<br>FOOTBALL POWER RANKINGS</div>';
    $main = '<main class="lvay-season-main">' . $heading;

    if ($rankings) {
        $updated = '';
        if (!empty($rankings[0]['calculated_at'])) {
            try {
                $when = new DateTime($rankings[0]['calculated_at'], new DateTimeZone('UTC'));
                $when->setTimezone(wp_timezone());
                $updated = $when->format('n/j/Y g:i A T');
            } catch (Exception $e) {
                $updated = '';
            }
        }
        if ($updated) $main .= '<div class="lvay-updated">Final update: ' . esc_html($updated) . '</div>';
        $main .= lvay_archive_rankings_table_v2($rankings, $season);
    } else {
        $main .= '<section class="lvay-preseason-card"><span>PRESEASON</span>';
        $main .= '<h2>Power rankings begin after games are played.</h2>';
        $main .= '<p>The 2026 page is ready. Rankings will populate automatically when official scores enter the LVAY system.</p>';
        $main .= '<a href="' . esc_url(add_query_arg('season', 2025, 'https://louisianavsallyall.com/power-rankings/')) . '">View the final 2025 rankings</a></section>';
    }
    $main .= '</main>';

    return '<section class="lvay-rankings-design lvay-season-layout">'
        . $main
        . lvay_archive_nav_v2('https://louisianavsallyall.com/power-rankings/', $season)
        . '</section><script>if(typeof lvayToggle!=="function"){function lvayToggle(el){el.classList.toggle("open");el.nextElementSibling.classList.toggle("open");}}</script>';
}
add_filter('do_shortcode_tag', 'lvay_archive_rankings_output_v2', 99, 4);

function lvay_archive_brackets_content_v2($content) {
    if (!is_page(10398) || !in_the_loop() || !is_main_query()) return $content;
    $season = lvay_archive_selected_season_v2();
    $nav = lvay_archive_nav_v2('https://louisianavsallyall.com/playoff-brackets/', $season);

    if ($season === 2025) {
        return '<div class="lvay-season-layout lvay-brackets-layout"><main class="lvay-season-main">'
            . $content . '</main>' . $nav . '</div>';
    }

    $current = '<main class="lvay-season-main"><div class="lvay-brackets-heading">2026 LHSAA<br>FOOTBALL PLAYOFF BRACKETS</div>';
    $current .= '<section class="lvay-preseason-card"><span>COMING THIS POSTSEASON</span>';
    $current .= '<h2>The 2026 playoff brackets will appear here.</h2>';
    $current .= '<p>The completed 2025 brackets remain preserved in the Season Archives.</p>';
    $current .= '<a href="' . esc_url(add_query_arg('season', 2025, 'https://louisianavsallyall.com/playoff-brackets/')) . '">View the 2025 playoff brackets</a></section></main>';
    return '<div class="lvay-season-layout lvay-brackets-layout">' . $current . $nav . '</div>';
}
add_filter('the_content', 'lvay_archive_brackets_content_v2', 999);

function lvay_archive_styles_v2() {
    $css = <<<'CSS'
.lvay-season-layout{display:grid;grid-template-columns:minmax(0,1fr) 260px;gap:24px;width:min(1600px,calc(100vw - 48px));max-width:none;margin:0;position:relative;left:50%;transform:translateX(-50%);padding:18px 0 34px}
.lvay-season-main{min-width:0}
.lvay-rankings-design .lvay-acc-body{display:none!important}
.lvay-rankings-design .lvay-acc-body.open{display:block!important}
.lvay-season-archive{align-self:start;background:#050505;color:#fff;padding:20px 24px 24px;display:grid;grid-template-columns:1fr 1fr;gap:7px 18px}
.lvay-season-archive h3{grid-column:1/-1;margin:0 0 8px;color:#fff;font-family:Teko,Arial,sans-serif;font-size:34px;font-weight:500;line-height:1;letter-spacing:.8px;text-decoration:underline;text-underline-offset:5px}
.lvay-season-archive a{color:#666!important;font-family:Teko,Arial,sans-serif;font-size:27px;font-weight:500;line-height:1.05;text-decoration:none!important}
.lvay-season-archive a:hover,.lvay-season-archive a.active{color:#fff!important}
.lvay-season-archive .coming{grid-column:1/-1;margin-top:12px;color:#999;font-family:Teko,Arial,sans-serif;font-size:20px;font-weight:400;line-height:1.15}
.lvay-rankings-design .lvay-acc-hdr{min-height:82px!important;font-size:clamp(27px,2.25vw,42px)!important}
.lvay-rankings-design .lvay-rtbl{font-size:16px!important}
.lvay-rankings-design .lvay-rtbl th{padding:11px 12px!important;font-size:15px!important}
.lvay-rankings-design .lvay-rtbl td{padding:10px 12px!important}
.lvay-preseason-card{border-top:8px solid #078b88;background:#f1f5f5;padding:34px 38px;margin-top:18px}
.lvay-preseason-card span{color:#078b88;font-family:Teko,Arial,sans-serif;font-size:24px;font-weight:700;letter-spacing:2px}
.lvay-preseason-card h2{margin:3px 0 10px;color:#080808;font-family:"Alfa Slab One",serif;font-size:clamp(27px,3vw,42px);line-height:1.05}
.lvay-preseason-card p{max-width:720px;font-size:17px;line-height:1.5}
.lvay-preseason-card a{display:inline-block;margin-top:8px;background:#078b88;color:#fff!important;padding:10px 17px;font-weight:800;text-decoration:none!important}
.lvay-brackets-heading{color:#078b88;font-family:"Alfa Slab One",serif;font-size:clamp(28px,3vw,42px);line-height:.95}
.lvay-brackets-layout .elementor-element-c302248{padding:0!important}
@media(max-width:1200px){.lvay-season-layout{grid-template-columns:1fr}.lvay-season-archive{grid-row:1}.lvay-season-main{grid-row:2}}
@media(max-width:600px){.lvay-season-layout{padding-top:8px}.lvay-season-archive{grid-template-columns:1fr 1fr;padding:16px 18px}.lvay-preseason-card{padding:24px 20px}}
CSS;
    wp_register_style('lvay-football-archives', false);
    wp_enqueue_style('lvay-football-archives');
    wp_add_inline_style('lvay-football-archives', $css);
}
add_action('wp_enqueue_scripts', 'lvay_archive_styles_v2', 40);
