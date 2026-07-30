/**
 * LVAY site icon fallback.
 *
 * The logo is already selected as WordPress's Site Icon, but the active theme
 * does not emit favicon markup. This restores the browser-tab, bookmark, and
 * mobile home-screen icon tags using WordPress's configured Site Icon.
 * Paste into Code Snippets without an opening PHP tag and run everywhere.
 */

function lvay_output_site_icon_fallback() {
    if (function_exists('has_site_icon') && has_site_icon()) {
        $icon_32  = get_site_icon_url(32);
        $icon_192 = get_site_icon_url(192);
        $icon_180 = get_site_icon_url(180);

        if ($icon_32) {
            echo '<link rel="icon" href="' . esc_url($icon_32) . '" sizes="32x32">' . "\n";
        }
        if ($icon_192) {
            echo '<link rel="icon" href="' . esc_url($icon_192) . '" sizes="192x192">' . "\n";
        }
        if ($icon_180) {
            echo '<link rel="apple-touch-icon" href="' . esc_url($icon_180) . '">' . "\n";
        }
        if ($icon_192) {
            echo '<meta name="msapplication-TileImage" content="' . esc_url($icon_192) . '">' . "\n";
        }
    }
}
add_action('wp_head', 'lvay_output_site_icon_fallback', 2);

