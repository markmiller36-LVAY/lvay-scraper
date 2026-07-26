/**
 * Adds a clear route from archived football schedules back to the current year.
 */
function lvay_football_schedule_archive_return_link($output, $tag, $attr, $match) {
    if ($tag !== 'lvay_football_schedules') return $output;
    $season = isset($_GET['season']) ? absint($_GET['season']) : 2026;
    if ($season >= 2026) return $output;

    $current_url = remove_query_arg(array('season', 'school'));
    $link = '<a class="lvay-return-current-season" href="' . esc_url($current_url) . '">'
        . '&larr; Return to 2026 Schedules</a>';

    return preg_replace(
        '/(<header class="lvay-schedule-title">)/',
        $link . '$1',
        $output,
        1
    );
}
add_filter('do_shortcode_tag', 'lvay_football_schedule_archive_return_link', 85, 4);

function lvay_football_schedule_archive_return_styles() {
    $css = '.lvay-return-current-season{'
        . 'display:inline-block;margin:0 0 12px;color:#777!important;'
        . 'font-family:Teko,Arial,sans-serif;font-size:22px;font-style:italic;'
        . 'font-weight:400;text-decoration:none!important}'
        . '.lvay-return-current-season:hover{color:#111!important;text-decoration:underline!important}';
    wp_register_style('lvay-football-schedule-return', false);
    wp_enqueue_style('lvay-football-schedule-return');
    wp_add_inline_style('lvay-football-schedule-return', $css);
}
add_action('wp_enqueue_scripts', 'lvay_football_schedule_archive_return_styles', 75);
