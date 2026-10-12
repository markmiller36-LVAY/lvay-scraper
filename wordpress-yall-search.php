/**
 * LVAY — Hey Y'all Search (TEST) [lvay_yall_search]
 * Plain-English football history questions, answered from the LVAY archive
 * by https://lvay-scraper.onrender.com/api/yall (templates, no AI).
 * Test page: /yall-test/ (kept out of search engines). Oct 12, 2026.
 *
 * Question log (Oct 12, 2026): every question asked on the page is saved in
 * the WordPress table {prefix}lvay_yall_log. Admins see it at
 * /mission-control/yall-questions/ ([lvay_yall_log]) or as JSON from
 * GET /wp-json/lvay/v1/yall-log (admins only).
 */

function lvay_ys_table() {
	global $wpdb;
	return $wpdb->prefix . 'lvay_yall_log';
}

add_action( 'init', function () {
	if ( get_option( 'lvay_ys_db' ) === '2' ) {
		return;
	}
	global $wpdb;
	require_once ABSPATH . 'wp-admin/includes/upgrade.php';
	$t = lvay_ys_table();
	dbDelta( "CREATE TABLE $t (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		asked_at datetime NOT NULL,
		question varchar(300) NOT NULL,
		status varchar(20) NOT NULL DEFAULT '',
		intent varchar(40) NOT NULL DEFAULT '',
		teams varchar(200) NOT NULL DEFAULT '',
		answer text NULL,
		visitor varchar(12) NOT NULL DEFAULT '',
		who varchar(120) NOT NULL DEFAULT '',
		PRIMARY KEY  (id),
		KEY asked_at (asked_at)
	) " . $wpdb->get_charset_collate() . ';' );
	update_option( 'lvay_ys_db', '2', false );
} );

add_action( 'rest_api_init', function () {
	register_rest_route( 'lvay/v1', '/yall-log', array(
		array(
			'methods'             => 'POST',
			'permission_callback' => '__return_true',
			'callback'            => 'lvay_ys_log_save',
		),
		array(
			'methods'             => 'GET',
			'permission_callback' => function () {
				return current_user_can( 'edit_users' );
			},
			'callback'            => function ( $req ) {
				return rest_ensure_response( lvay_ys_log_rows(
					(int) $req->get_param( 'limit' ),
					(string) $req->get_param( 'only' )
				) );
			},
		),
	) );
} );

function lvay_ys_log_save( $req ) {
	$ip  = isset( $_SERVER['REMOTE_ADDR'] ) ? (string) $_SERVER['REMOTE_ADDR'] : '';
	$who = substr( md5( $ip . wp_salt( 'auth' ) ), 0, 12 );
	$key = 'lvay_ys_rl_' . $who;
	$n   = (int) get_transient( $key );
	if ( $n >= 60 ) {
		return new WP_REST_Response( array( 'saved' => false ), 429 );
	}
	set_transient( $key, $n + 1, HOUR_IN_SECONDS );
	$q = trim( sanitize_text_field( (string) $req->get_param( 'q' ) ) );
	if ( '' === $q ) {
		return array( 'saved' => false );
	}
	global $wpdb;
	$wpdb->insert( lvay_ys_table(), array(
		'asked_at' => current_time( 'mysql' ),
		'question' => mb_substr( $q, 0, 300 ),
		'status'   => mb_substr( sanitize_key( (string) $req->get_param( 'status' ) ), 0, 20 ),
		'intent'   => mb_substr( sanitize_key( (string) $req->get_param( 'intent' ) ), 0, 40 ),
		'teams'    => mb_substr( sanitize_text_field( (string) $req->get_param( 'teams' ) ), 0, 200 ),
		'answer'   => mb_substr( sanitize_text_field( (string) $req->get_param( 'answer' ) ), 0, 600 ),
		'visitor'  => $who,
		'who'      => lvay_ys_who(),
	) );
	return array( 'saved' => true );
}

function lvay_ys_who() {
	$u = wp_get_current_user();
	if ( ! $u || ! $u->ID ) {
		return 'Guest';
	}
	return mb_substr( $u->display_name . ' (@' . $u->user_login . ')', 0, 120 );
}

function lvay_ys_log_rows( $limit = 300, $only = '' ) {
	global $wpdb;
	$limit = $limit > 0 ? min( $limit, 2000 ) : 300;
	$where = 'missed' === $only ? "WHERE status <> 'ok'" : '';
	$t     = lvay_ys_table();
	return $wpdb->get_results( $wpdb->prepare(
		"SELECT id, asked_at, who, question, status, intent, teams, answer, visitor FROM $t $where ORDER BY id DESC LIMIT %d",
		$limit
	), ARRAY_A );
}

add_shortcode( 'lvay_yall_log', function () {
	if ( ! current_user_can( 'edit_users' ) ) {
		return '';
	}
	$only = isset( $_GET['only'] ) && 'missed' === $_GET['only'] ? 'missed' : '';
	$rows = lvay_ys_log_rows( 500, $only );
	$all  = lvay_ys_log_rows( 2000, '' );
	$miss = 0;
	foreach ( $all as $r ) {
		if ( 'ok' !== $r['status'] ) {
			$miss++;
		}
	}
	$base = get_permalink();
	ob_start();
	?>
<div class="lvay-ysl">
  <p class="lvay-ysl-kick"><a href="/mission-control/">&lsaquo; Mission Control</a></p>
  <h1>Search Questions</h1>
  <p class="lvay-ysl-sum"><?php echo (int) count( $all ); ?> questions logged &middot; <?php echo (int) $miss; ?> not answered.
    <a href="<?php echo esc_url( $base ); ?>"<?php echo $only ? '' : ' class="on"'; ?>>All</a>
    <a href="<?php echo esc_url( add_query_arg( 'only', 'missed', $base ) ); ?>"<?php echo $only ? ' class="on"' : ''; ?>>Not answered</a>
  </p>
  <div class="lvay-ysl-wrap"><table>
    <thead><tr><th>When (CT)</th><th>Who</th><th>Question</th><th>Result</th><th>Read as</th><th>Team(s)</th><th>Answer given</th></tr></thead>
    <tbody>
    <?php foreach ( $rows as $r ) : $ok = 'ok' === $r['status']; ?>
      <tr class="<?php echo $ok ? 'ok' : 'miss'; ?>">
        <td><?php echo esc_html( mysql2date( 'M j, g:i a', $r['asked_at'] ) ); ?></td>
        <td><?php echo esc_html( $r['who'] ? $r['who'] : 'Guest' ); ?></td>
        <td><a href="/yall-test/?q=<?php echo rawurlencode( $r['question'] ); ?>"><?php echo esc_html( $r['question'] ); ?></a></td>
        <td><?php echo $ok ? 'Answered' : esc_html( ucwords( str_replace( '_', ' ', $r['status'] ?: 'error' ) ) ); ?></td>
        <td><?php echo esc_html( str_replace( '_', ' ', $r['intent'] ) ); ?></td>
        <td><?php echo esc_html( $r['teams'] ); ?></td>
        <td class="ans"><?php echo esc_html( $r['answer'] ); ?></td>
      </tr>
    <?php endforeach; ?>
    <?php if ( ! $rows ) : ?><tr><td colspan="7">No questions yet.</td></tr><?php endif; ?>
    </tbody>
  </table></div>
</div>
<style>
.lvay-ysl{max-width:1380px;margin:30px auto 60px;padding:0 16px;font-family:'PT Serif',Georgia,serif}
.lvay-ysl-kick a{font-family:'Teko',sans-serif;font-size:18px;color:#008584;text-decoration:none}
.lvay-ysl h1{font-family:'Alfa Slab One',serif;font-size:36px;margin:4px 0 6px;text-transform:uppercase}
.lvay-ysl-sum{font-size:16px;color:#444}
.lvay-ysl-sum a{font-family:'Teko',sans-serif;font-size:17px;margin-left:10px;padding:4px 10px 1px;border:1px solid #cfd8d8;border-radius:16px;text-decoration:none;color:#0b4f4e}
.lvay-ysl-sum a.on{background:#008584;border-color:#008584;color:#fff}
.lvay-ysl-wrap{overflow-x:auto;margin-top:14px}
.lvay-ysl table{width:100%;border-collapse:collapse;font-size:14px;min-width:900px}
.lvay-ysl th{font-family:'Teko',sans-serif;font-size:17px;font-weight:400;text-align:left;background:#111;color:#fff;padding:6px 10px 3px;position:sticky;top:0}
.lvay-ysl td{padding:7px 10px;border-bottom:1px solid #e3e7e7;vertical-align:top}
.lvay-ysl tr.miss td{background:#fffbea}
.lvay-ysl td.ans{color:#555;max-width:420px}
</style>
	<?php
	return ob_get_clean();
} );

add_shortcode( 'lvay_yall_search', 'lvay_ys_render' );

add_filter( 'wp_robots', function ( $robots ) {
	if ( is_page( 'yall-test' ) ) {
		$robots['noindex']  = true;
		$robots['nofollow'] = true;
	}
	return $robots;
} );

function lvay_ys_render() {
	ob_start();
	?>
<div class="lvay-ys" id="lvay-ys" data-nonce="<?php echo esc_attr( wp_create_nonce( 'wp_rest' ) ); ?>">
  <div class="lvay-ys-head">
    <h1>&ldquo;Hey <span>Y&#8217;all</span>&hellip;&rdquo;</h1>
    <p>Ask anything about Louisiana high school football history, 1990 to today, or about the LHSAA rules.</p>
  </div>
  <form class="lvay-ys-form" autocomplete="off">
    <span class="lvay-ys-field"><input type="text" name="q" maxlength="300"
      placeholder="When is the last time West Monroe had 50 points put on them?" aria-label="Ask a football history question"><button type="button" class="lvay-ys-clear" aria-label="Clear the question" title="Clear" hidden>Clear</button></span>
    <button type="submit">ASK</button>
  </form>
  <div class="lvay-ys-try lvay-ys-samples"></div>
  <div class="lvay-ys-out" aria-live="polite"></div>
  <p class="lvay-ys-note">Test version. Answers come straight from LVAY game records. <span class="lvay-ys-cov"></span></p>
</div>
<style>
.lvay-ys{max-width:900px;margin:30px auto 60px;padding:0 16px;font-family:'PT Serif',Georgia,serif;color:#111}
.lvay-ys-head h1{font-family:'Alfa Slab One',serif;font-weight:400;font-size:34px;line-height:1.1;letter-spacing:.3px;margin:0 0 6px;text-transform:uppercase}
.lvay-ys-head h1 span{color:#008584}
.lvay-ys-head p{margin:0 0 18px;font-size:17px;color:#444}
.lvay-ys-form{display:flex;gap:8px}
.lvay-ys-form input{flex:1;min-width:0;font-size:18px;padding:14px 16px;border:2px solid #111;border-radius:6px;font-family:inherit}
.lvay-ys-field{position:relative;flex:1;min-width:0;display:flex}
.lvay-ys-form .lvay-ys-field input{width:100%;padding-right:84px}
.lvay-ys-form .lvay-ys-clear{position:absolute;right:8px;top:50%;transform:translateY(-50%);height:32px;padding:3px 12px 0;border:0;border-radius:16px;background:#e6eded;color:#333;font:400 19px/29px 'Teko',sans-serif;letter-spacing:.8px;text-transform:uppercase;cursor:pointer}
.lvay-ys-form .lvay-ys-clear:hover{background:#cfdcdc;color:#000}
.lvay-ys-form .lvay-ys-clear[hidden]{display:none}
.lvay-ys-form input:focus{outline:none;border-color:#008584;box-shadow:0 0 0 3px rgba(0,133,132,.2)}
.lvay-ys-form button{font-family:'Teko',sans-serif;font-size:26px;letter-spacing:1px;padding:4px 26px 0;background:#008584;color:#fff;border:0;border-radius:6px;cursor:pointer}
.lvay-ys-form button:hover{background:#006b6a}
.lvay-ys-try{margin:12px 0 4px;display:flex;flex-wrap:wrap;gap:6px}
.lvay-ys-chip{font-family:'Teko',sans-serif;font-size:17px;line-height:1;padding:7px 10px 4px;border:1px solid #cfd8d8;background:#f4f8f8;border-radius:20px;cursor:pointer;color:#0b4f4e}
.lvay-ys-chip:hover{background:#e3f0f0;border-color:#008584}
.lvay-ys-try.lvay-ys-samples{display:block;margin:16px 0 4px}
.lvay-ys-samples[hidden]{display:none!important}
.lvay-ys-samples-h{font-family:'Teko',sans-serif;font-size:23px;letter-spacing:.5px;text-transform:uppercase;color:#111;margin:0 0 8px;line-height:1}
.lvay-ys-samples-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px 28px}
.lvay-ys-sgh{font-family:'Teko',sans-serif;font-size:19px;letter-spacing:.5px;text-transform:uppercase;color:#008584;line-height:1.05;margin:0 0 4px;border-bottom:1px solid #d5e3e3;padding-bottom:3px}
.lvay-ys a.lvay-ys-sq{display:block;font-family:'PT Serif',Georgia,serif;font-size:15.5px;line-height:1.35;color:#111;text-decoration:none;padding:3px 0 3px 14px;position:relative}
.lvay-ys a.lvay-ys-sq:before{content:'\201C';position:absolute;left:0;top:2px;color:#008584;font-weight:700}
.lvay-ys a.lvay-ys-sq:hover{color:#008584;text-decoration:underline}
.lvay-ys-tip{font-size:13.5px;color:#5b6b6b;margin:12px 0 0}
.lvay-ys table.lvay-ys-cmp{min-width:0}
.lvay-ys-cmp td.rl{color:#0b4f4e;white-space:nowrap}
@media(max-width:600px){.lvay-ys-samples-grid{grid-template-columns:1fr}}
.lvay-ys-out{margin-top:20px}
.lvay-ys-card{border:1.5px solid #1E7D53;background:#E1F1E8;padding:16px 18px;border-radius:10px}
.lvay-ys-card.miss{border-color:#c9a227;background:#fffbea}
.lvay-ys-ans{font-size:20px;line-height:1.45;margin:0;font-weight:700;color:#111}
.lvay-ys-try[hidden]{display:none}
.lvay-ys-how{font-family:'Teko',sans-serif;font-size:17px;color:#1E7D53;margin:10px 0 0;text-transform:uppercase;letter-spacing:.5px}
.lvay-ys-tablewrap{overflow-x:auto;margin-top:16px}
.lvay-ys table{width:100%;border-collapse:collapse;font-size:15px;min-width:560px}
.lvay-ys th{font-family:'Teko',sans-serif;font-size:17px;text-align:left;background:#111;color:#fff;padding:6px 10px 3px;font-weight:400;letter-spacing:.5px}
.lvay-ys td{padding:6px 10px 3px;border-bottom:1px solid #e3e7e7;font-family:'Teko',sans-serif;font-size:19px;line-height:1.2;letter-spacing:.3px}
.lvay-ys tr:nth-child(even) td{background:#fafbfb}
.lvay-ys .w{color:#0a7a3d;font-weight:700}.lvay-ys .l{color:#b3261e;font-weight:700}
.lvay-ys-note{font-size:13px;color:#777;margin-top:26px}
.lvay-ys-note-box{margin-top:12px;padding:12px 16px;background:#eef7f7;border:1px dashed #008584;border-radius:4px;font-size:15px;line-height:1.5;color:#244}
.lvay-ys-note-box b{font-family:'Teko',sans-serif;font-size:19px;font-weight:400;color:#008584;letter-spacing:.5px;text-transform:uppercase;margin-right:4px}
.lvay-ys-rules{margin-top:14px}
.lvay-ys-rule{border:1px solid #dfe6e6;border-radius:4px;padding:12px 16px;margin-bottom:10px;background:#fff}
.lvay-ys-rule-h{font-family:'Teko',sans-serif;font-size:19px;color:#0b4f4e;text-transform:uppercase;letter-spacing:.4px;display:flex;justify-content:space-between;gap:10px}
.lvay-ys-rule-h span{color:#888;white-space:nowrap}
.lvay-ys-rule p{margin:6px 0 8px;font-size:15px;line-height:1.5;color:#222}
.lvay-ys-rule a{font-family:'Teko',sans-serif;font-size:18px;color:#008584;text-decoration:none}
.lvay-ys-rules-src{font-size:12.5px;color:#777;margin:4px 0 0}
.lvay-ys-loading{font-family:'Teko',sans-serif;font-size:22px;color:#008584}
@media(max-width:767px){.lvay-ys-head h1{font-size:28px}.lvay-ys-form input{font-size:16px;padding:12px}.lvay-ys-form button{font-size:22px;padding:4px 16px 0}.lvay-ys-ans{font-size:18px}}
</style>
<script>
(function(){
  var API='https://lvay-scraper.onrender.com/api/yall?q=';
  var root=document.getElementById('lvay-ys'); if(!root) return;
  var form=root.querySelector('form'), input=form.q, out=root.querySelector('.lvay-ys-out'),
      tryBox=root.querySelector('.lvay-ys-try'), cov=root.querySelector('.lvay-ys-cov');
  var SAMPLES=[
    ['Cross-Decade Comparisons',['1997 Evangel vs 2024 Karr. Who gave up the least amount of points?','2018 Calvary Baptist, 1995 Edna Karr, 2007 Many','Ruston 2018 vs Ruston 2023. Who scored more?']],
    ['How a Team Fared Against a Class or Division',['What is the Many Tigers record vs Select schools?','West Monroe record vs 5A teams in the playoffs']],
    ['Head-to-Head',['Ruston vs West Monroe','Karr vs Curtis']],
    ['Every Team in Louisiana',['Last time a team scored 40+ in 5 straight games','Most points scored in a single game']],
    ['Points & Streaks',['When is the last time West Monroe gave up 50?','Longest winning streak for John Curtis']],
    ['Class & Select History',['How long has Evangel been 5A?','When did Select start?']],
    ['LHSAA Rules in Plain English',['Is there a mercy rule?','Do incoming freshmen have to live in the district?']]];
  function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];});}
  function chips(list,box){box.innerHTML='';list.forEach(function(t){var b=document.createElement('button');b.type='button';b.className='lvay-ys-chip';b.textContent=t;b.onclick=function(){input.value=t;ask();};box.appendChild(b);});}
  function samples(){
    var h='<div class="lvay-ys-samples-h">Sample Questions:</div><div class="lvay-ys-samples-grid">';
    SAMPLES.forEach(function(g){h+='<div class="lvay-ys-sgroup"><div class="lvay-ys-sgh">'+esc(g[0])+'</div>';g[1].forEach(function(q){h+='<a href="#" class="lvay-ys-sq" data-q="'+esc(q)+'">'+esc(q)+'</a>';});h+='</div>';});
    tryBox.innerHTML=h+'</div><p class="lvay-ys-tip"><b>Tip:</b> tap any question to run it, or type your own. Add a year, &ldquo;since 2010,&rdquo; &ldquo;at home,&rdquo; &ldquo;in the playoffs&rdquo; or &ldquo;vs 5A teams&rdquo; to narrow any question.</p>';
    tryBox.querySelectorAll('.lvay-ys-sq').forEach(function(a){a.onclick=function(e){e.preventDefault();input.value=a.getAttribute('data-q');ask();};});
  }
  samples();
  function gtable(t){
    if(!t||!t.rows||!t.rows.length) return '';
    var h='<div class="lvay-ys-tablewrap"><table'+(t.first_col_bold?' class="lvay-ys-cmp"':'')+'><thead><tr>';
    t.cols.forEach(function(c){h+='<th>'+esc(c)+'</th>';});
    h+='</tr></thead><tbody>';
    t.rows.forEach(function(r){h+='<tr>'+r.map(function(v,i){return '<td'+(i===0&&t.first_col_bold?' class="rl"':'')+'>'+esc(v)+'</td>';}).join('')+'</tr>';});
    return h+'</tbody></table></div>';
  }
  function table(rows){
    if(!rows||!rows.length) return '';
    var h='<div class="lvay-ys-tablewrap"><table><thead><tr><th>Date</th><th>Team</th><th>Opponent</th><th>Result</th><th>Where</th><th>Game</th></tr></thead><tbody>';
    rows.forEach(function(r){var c=r.result==='W'?'w':(r.result==='L'?'l':'');
      h+='<tr><td>'+esc(r.date||r.season)+'</td><td>'+esc(r.team)+'</td><td>'+esc(r.opponent)+(r.opp_class?' <small style="color:#888">('+esc(r.opp_class)+')</small>':'')+'</td><td class="'+c+'">'+esc(r.result)+' '+esc(r.score)+'</td><td>'+esc(r.where)+'</td><td>'+esc(r.phase)+'</td></tr>';});
    return h+'</tbody></table></div>';
  }
  function hist(rows){
    if(!rows||!rows.length) return '';
    var h='<div class="lvay-ys-tablewrap"><table><thead><tr><th>Season</th><th>Class</th><th>Playoff bracket</th></tr></thead><tbody>';
    rows.forEach(function(r){h+='<tr><td>'+esc(r.season)+'</td><td><b>'+esc(r['class'])+'</b></td><td>'+esc(r.bracket||'')+'</td></tr>';});
    return h+'</tbody></table></div>';
  }
  function rules(list){
    if(!list||!list.length) return '';
    var h='<div class="lvay-ys-rules">';
    list.forEach(function(r){h+='<div class="lvay-ys-rule"><div class="lvay-ys-rule-h">'+(r.rule?'Bylaw '+esc(r.rule)+' &middot; ':'')+esc(r.section)+(r.title&&r.rule?' &middot; '+esc(r.title):(r.rule?'':' &middot; '+esc(r.title)))+'<span>Page '+esc(r.page)+'</span></div><p>'+esc(r.excerpt)+'</p><a href="'+esc(r.link)+'" target="_blank" rel="noopener">Read it in the official LHSAA Handbook &rsaquo;</a></div>';});
    return h+'<p class="lvay-ys-rules-src">Source: 2026&ndash;2027 LHSAA Handbook. Always check the official handbook for the full rule.</p></div>';
  }
  function show(j){
    if(j.coverage) cov.textContent='Covers: '+j.coverage+'.';
    var miss=j.status!=='ok';
    tryBox.hidden=true;
    var h='<div class="lvay-ys-card'+(miss?' miss':'')+'"><p class="lvay-ys-ans">'+esc(j.answer)+'</p>';
    if(j.interpreted_as) h+='<p class="lvay-ys-how">Searched: '+esc(j.interpreted_as)+'</p>';
    h+='</div>';
    if(j.note) h+='<div class="lvay-ys-note-box">'+esc(j.note).replace(/^(Did you know\?|Please note:)/,'<b>$1</b>')+'</div>';
    out.innerHTML=h+gtable(j.table)+table(j.rows)+hist(j.history)+rules(j.handbook);
    if(j.status==='ambiguous'&&j.choices){var box=document.createElement('div');box.className='lvay-ys-try';out.appendChild(box);
      var q=j.question; chips(j.choices.map(function(name){var base=name.split(' - ')[0];var re=new RegExp(base.replace(/[.*+?^${}()|[\]\\]/g,'\\$&'),'i');return re.test(q)?q.replace(re,name):name+' '+q;}),box);}
    if(miss&&j.examples){var b2=document.createElement('div');b2.className='lvay-ys-try';out.appendChild(b2);chips(j.examples,b2);}
  }
  function logQ(q,j){
    var body=JSON.stringify({q:q,status:j.status||'',intent:j.intent||'',teams:(j.teams||[]).join(', '),answer:j.answer||''});
    try{fetch('/wp-json/lvay/v1/yall-log',{method:'POST',headers:{'Content-Type':'application/json','X-WP-Nonce':root.dataset.nonce||''},keepalive:true,body:body})
      .then(function(r){if(r.status===403){fetch('/wp-json/lvay/v1/yall-log',{method:'POST',headers:{'Content-Type':'application/json'},credentials:'omit',body:body});}});}catch(e){}
  }
  function ask(){
    var q=input.value.trim(); if(!q) return;
    out.innerHTML='<p class="lvay-ys-loading">Digging through the archives&hellip;</p>';
    try{var u=new URL(location.href);u.searchParams.set('q',q);history.replaceState(null,'',u);}catch(e){}
    fetch(API+encodeURIComponent(q)).then(function(r){
      if(r.status===429) throw new Error('slow');
      return r.json();
    }).then(function(j){show(j);logQ(q,j);}).catch(function(e){
      logQ(q,{status:'error',answer:e.message==='slow'?'rate limited':'server not reachable'});
      out.innerHTML='<div class="lvay-ys-card miss"><p class="lvay-ys-ans">'+(e.message==='slow'?'Too many questions too fast. Give it a minute.':'The search server is waking up. Try again in a few seconds.')+'</p></div>';
    });
  }
  form.addEventListener('submit',function(e){e.preventDefault();ask();});
  var clr=root.querySelector('.lvay-ys-clear');
  function syncClear(){clr.hidden=!input.value;}
  input.addEventListener('input',syncClear);
  form.addEventListener('submit',syncClear);
  tryBox.addEventListener('click',function(){setTimeout(syncClear,0);});
  out.addEventListener('click',function(){setTimeout(syncClear,0);});
  clr.addEventListener('click',function(){input.value='';syncClear();input.focus();tryBox.hidden=false;out.innerHTML='';
    try{var u=new URL(location.href);u.searchParams.delete('q');history.replaceState(null,'',u);}catch(e){}});
  try{var start=new URL(location.href).searchParams.get('q');if(start){input.value=start;syncClear();ask();}}catch(e){}
})();
</script>
	<?php
	return ob_get_clean();
}
