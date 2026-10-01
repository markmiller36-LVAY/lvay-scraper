/**
 * LVAY pipeline cache refresh. Install as an active, run-everywhere PHP snippet.
 * Uses existing WordPress application-password authentication; no public purge.
 */
add_action('rest_api_init', function () {
    register_rest_route('lvay/v1', '/refresh-cache', array(
        'methods' => 'POST',
        'permission_callback' => function () {
            return current_user_can('manage_options');
        },
        'callback' => function () {
            if (!is_callable(array('KP_Cache_Purge_Common', 'do_the_actual_purge'))) {
                return new WP_Error('lvay_cache_plugin_unavailable',
                    'The Cache Purger is not available.', array('status' => 503));
            }
            try {
                KP_Cache_Purge_Common::do_the_actual_purge();
            } catch (Throwable $error) {
                return new WP_Error('lvay_cache_refresh_failed',
                    'Cache refresh failed.', array('status' => 503));
            }
            $response = new WP_REST_Response(array('purge_requested' => true), 200);
            $response->header('Cache-Control', 'no-store');
            return $response;
        },
    ));
});
