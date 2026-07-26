/**
 * Responsive override for the active LVAY Football Season Archives V2 snippet.
 */
function lvay_archive_responsive_width_patch() {
    $css = '@media(max-width:1200px){'
        . '.lvay-season-layout{grid-template-columns:1fr!important}'
        . '.lvay-season-archive{grid-row:1!important}'
        . '.lvay-season-main{grid-row:2!important}'
        . '}';
    wp_register_style('lvay-football-archives-responsive', false);
    wp_enqueue_style('lvay-football-archives-responsive');
    wp_add_inline_style('lvay-football-archives-responsive', $css);
}
add_action('wp_enqueue_scripts', 'lvay_archive_responsive_width_patch', 50);
