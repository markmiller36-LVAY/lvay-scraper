<?php
/**
 * LVAY Football Scores - WordPress Display Snippet
 * =================================================
 * Add this to your WordPress theme's functions.php
 * OR install "Code Snippets" plugin and paste it there.
 *
 * Then use shortcode [lvay_scores] on any page/post
 * to display the live scores table.
 *
 * Use [lvay_standings] to show the win/loss standings table.
 */

// ── CONFIG ────────────────────────────────────────────────────────────────────
// Replace with your actual Render.com URL after deployment
define('LVAY_API_URL', 'https://lvay-football-scraper.onrender.com');


// ── SCORES SHORTCODE ─────────────────────────────────────────────────────────
// Usage: [lvay_scores] or [lvay_scores school="Airline"]

function lvay_scores_shortcode($atts) {
    $atts = shortcode_atts(['school' => ''], $atts);

    $url = $atts['school']
        ? LVAY_API_URL . '/api/scores/' . urlencode($atts['school'])
        : LVAY_API_URL . '/api/scores';

    $response = wp_remote_get($url, ['timeout' => 15]);

    if (is_wp_error($response)) {
        return '<p style="color:red;">Unable to load scores right now. Please try again later.</p>';
    }

    $data = json_decode(wp_remote_retrieve_body($response), true);
    if (!$data) {
        return '<p>No data available.</p>';
    }

    ob_start();
    ?>
    <div class="lvay-scores-wrap">
        <p class="lvay-updated">
            Last updated: <?php echo esc_html(
                isset($data['updated_at'])
                    ? date('M j, Y g:i A', strtotime($data['updated_at']))
                    : 'Unknown'
            ); ?>
        </p>

        <?php if (isset($data['games'])): ?>
            <?php // Single school view
                $school = $data;
            ?>
            <h3><?php echo esc_html($school['school']); ?>
                — <?php echo esc_html($school['record']); ?></h3>
            <table class="lvay-table">
                <thead>
                    <tr>
                        <th>Week</th>
                        <th>Date</th>
                        <th>Opponent</th>
                        <th>H/A</th>
                        <th>W/L</th>
                        <th>Score</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($school['games'] as $game): ?>
                    <tr class="<?php echo $game['win_loss'] === 'W' ? 'lvay-win' : ($game['win_loss'] === 'L' ? 'lvay-loss' : ''); ?>">
                        <td><?php echo esc_html($game['week']); ?></td>
                        <td><?php echo esc_html($game['game_date']); ?></td>
                        <td><?php echo esc_html($game['opponent']); ?></td>
                        <td><?php echo esc_html($game['home_away']); ?></td>
                        <td><strong><?php echo esc_html($game['win_loss']); ?></strong></td>
                        <td><?php echo esc_html($game['score']); ?></td>
                    </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>

        <?php elseif (isset($data['schools'])): ?>
            <?php // All schools view - show each school as a section
            foreach ($data['schools'] as $school_name => $school_data): ?>
                <div class="lvay-school-block">
                    <h4><?php echo esc_html($school_name); ?>
                        (<?php echo esc_html($school_data['wins']); ?>-<?php echo esc_html($school_data['losses']); ?>)
                    </h4>
                    <table class="lvay-table">
                        <thead>
                            <tr>
                                <th>Week</th><th>Date</th><th>Opponent</th>
                                <th>H/A</th><th>W/L</th><th>Score</th>
                            </tr>
                        </thead>
                        <tbody>
                        <?php foreach ($school_data['games'] as $game): ?>
                            <tr class="<?php echo $game['win_loss'] === 'W' ? 'lvay-win' : ($game['win_loss'] === 'L' ? 'lvay-loss' : ''); ?>">
                                <td><?php echo esc_html($game['week']); ?></td>
                                <td><?php echo esc_html($game['game_date']); ?></td>
                                <td><?php echo esc_html($game['opponent']); ?></td>
                                <td><?php echo esc_html($game['home_away']); ?></td>
                                <td><strong><?php echo esc_html($game['win_loss']); ?></strong></td>
                                <td><?php echo esc_html($game['score']); ?></td>
                            </tr>
                        <?php endforeach; ?>
                        </tbody>
                    </table>
                </div>
            <?php endforeach; ?>
        <?php endif; ?>
    </div>

    <style>
    .lvay-scores-wrap { font-family: inherit; }
    .lvay-updated { font-size: 12px; color: #888; margin-bottom: 12px; }
    .lvay-table { width: 100%; border-collapse: collapse; margin-bottom: 24px; font-size: 14px; }
    .lvay-table th { background: #1a3a5c; color: #fff; padding: 8px 10px; text-align: left; }
    .lvay-table td { padding: 7px 10px; border-bottom: 1px solid #eee; }
    .lvay-table tr.lvay-win td { background: #f0fff4; }
    .lvay-table tr.lvay-loss td { background: #fff5f5; }
    .lvay-school-block { margin-bottom: 32px; }
    .lvay-school-block h4 { margin-bottom: 8px; font-size: 16px; }
    </style>
    <?php
    return ob_get_clean();
}
add_shortcode('lvay_scores', 'lvay_scores_shortcode');


// ── STANDINGS SHORTCODE ───────────────────────────────────────────────────────
// Usage: [lvay_standings] or [lvay_standings class="5A"] or [lvay_standings district="3"]

function lvay_standings_shortcode($atts) {
    $atts = shortcode_atts(['class' => '', 'district' => ''], $atts);

    $response = wp_remote_get(LVAY_API_URL . '/api/standings', ['timeout' => 15]);

    if (is_wp_error($response)) {
        return '<p style="color:red;">Unable to load standings right now.</p>';
    }

    $data = json_decode(wp_remote_retrieve_body($response), true);
    if (!$data || empty($data['standings'])) {
        return '<p>No standings data available yet.</p>';
    }

    $standings = $data['standings'];

    // Filter by class if specified
    if ($atts['class']) {
        $standings = array_filter($standings, function($s) use ($atts) {
            return strtoupper($s['class_']) === strtoupper($atts['class']);
        });
    }

    // Filter by district if specified
    if ($atts['district']) {
        $standings = array_filter($standings, function($s) use ($atts) {
            return $s['district'] === $atts['district'];
        });
    }

    ob_start();
    ?>
    <div class="lvay-standings-wrap">
        <p class="lvay-updated">
            Season <?php echo esc_html($data['season']); ?> |
            Last updated: <?php echo esc_html(
                isset($data['updated_at'])
                    ? date('M j, Y g:i A', strtotime($data['updated_at']))
                    : 'Unknown'
            ); ?>
        </p>
        <table class="lvay-table">
            <thead>
                <tr>
                    <th>#</th>
                    <th>School</th>
                    <th>Class</th>
                    <th>District</th>
                    <th>W</th>
                    <th>L</th>
                    <th>Games</th>
                </tr>
            </thead>
            <tbody>
                <?php $rank = 1; foreach ($standings as $s): ?>
                <tr>
                    <td><?php echo $rank++; ?></td>
                    <td><?php echo esc_html($s['school']); ?></td>
                    <td><?php echo esc_html($s['class_']); ?></td>
                    <td><?php echo esc_html($s['district']); ?></td>
                    <td><strong><?php echo esc_html($s['wins']); ?></strong></td>
                    <td><?php echo esc_html($s['losses']); ?></td>
                    <td><?php echo esc_html($s['games_played']); ?></td>
                </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>
    <?php
    return ob_get_clean();
}
add_shortcode('lvay_standings', 'lvay_standings_shortcode');
