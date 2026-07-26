/**
 * Hover and selected states for football schedule school rows.
 */
function lvay_football_schedule_interaction_styles() {
    $css = '.lvay-school-toggle:hover,.lvay-school-toggle:focus-visible{'
        . 'background:#050505!important;color:#fff!important}'
        . '.lvay-school-toggle:hover small,.lvay-school-toggle:focus-visible small{'
        . 'color:#fff!important}'
        . '.lvay-school-toggle[aria-expanded="true"]{'
        . 'background:#333!important;color:#fff!important}'
        . '.lvay-school-toggle[aria-expanded="true"] small{color:#fff!important}';
    wp_register_style('lvay-football-schedule-interactions', false);
    wp_enqueue_style('lvay-football-schedule-interactions');
    wp_add_inline_style('lvay-football-schedule-interactions', $css);
}
add_action('wp_enqueue_scripts', 'lvay_football_schedule_interaction_styles', 60);
