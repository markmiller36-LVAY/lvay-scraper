/**
 * LVAY — Y'all Search (TEST) [lvay_yall_search]
 * Plain-English football history questions, answered from the LVAY archive
 * by https://lvay-scraper.onrender.com/api/yall (templates, no AI).
 * Test page: /yall-test/ (kept out of search engines). Oct 12, 2026.
 */

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
<div class="lvay-ys" id="lvay-ys">
  <div class="lvay-ys-head">
    <h1>Ask <span>Y&#8217;all</span></h1>
    <p>Ask anything about Louisiana high school football history, 1990 to today.</p>
  </div>
  <form class="lvay-ys-form" autocomplete="off">
    <input type="text" name="q" maxlength="300"
      placeholder="When is the last time West Monroe had 50 points put on them?" aria-label="Ask a football history question">
    <button type="submit">ASK</button>
  </form>
  <div class="lvay-ys-try"></div>
  <div class="lvay-ys-out" aria-live="polite"></div>
  <p class="lvay-ys-note">Test version. Answers come straight from LVAY game records. <span class="lvay-ys-cov"></span></p>
</div>
<style>
.lvay-ys{max-width:900px;margin:30px auto 60px;padding:0 16px;font-family:'PT Serif',Georgia,serif;color:#111}
.lvay-ys-head h1{font-family:'Alfa Slab One',serif;font-size:42px;line-height:1.05;margin:0 0 6px;text-transform:uppercase}
.lvay-ys-head h1 span{color:#008584}
.lvay-ys-head p{margin:0 0 18px;font-size:17px;color:#444}
.lvay-ys-form{display:flex;gap:8px}
.lvay-ys-form input{flex:1;min-width:0;font-size:18px;padding:14px 16px;border:2px solid #111;border-radius:6px;font-family:inherit}
.lvay-ys-form input:focus{outline:none;border-color:#008584;box-shadow:0 0 0 3px rgba(0,133,132,.2)}
.lvay-ys-form button{font-family:'Teko',sans-serif;font-size:26px;letter-spacing:1px;padding:4px 26px 0;background:#008584;color:#fff;border:0;border-radius:6px;cursor:pointer}
.lvay-ys-form button:hover{background:#006b6a}
.lvay-ys-try{margin:12px 0 4px;display:flex;flex-wrap:wrap;gap:6px}
.lvay-ys-chip{font-family:'Teko',sans-serif;font-size:17px;line-height:1;padding:7px 10px 4px;border:1px solid #cfd8d8;background:#f4f8f8;border-radius:20px;cursor:pointer;color:#0b4f4e}
.lvay-ys-chip:hover{background:#e3f0f0;border-color:#008584}
.lvay-ys-out{margin-top:20px}
.lvay-ys-card{border-left:6px solid #008584;background:#f7f9f9;padding:18px 20px;border-radius:4px}
.lvay-ys-card.miss{border-color:#c9a227;background:#fffbea}
.lvay-ys-ans{font-size:20px;line-height:1.45;margin:0}
.lvay-ys-how{font-family:'Teko',sans-serif;font-size:17px;color:#5b6b6b;margin:10px 0 0;text-transform:uppercase;letter-spacing:.5px}
.lvay-ys-tablewrap{overflow-x:auto;margin-top:16px}
.lvay-ys table{width:100%;border-collapse:collapse;font-size:15px;min-width:560px}
.lvay-ys th{font-family:'Teko',sans-serif;font-size:17px;text-align:left;background:#111;color:#fff;padding:6px 10px 3px;font-weight:400;letter-spacing:.5px}
.lvay-ys td{padding:7px 10px;border-bottom:1px solid #e3e7e7}
.lvay-ys tr:nth-child(even) td{background:#fafbfb}
.lvay-ys .w{color:#0a7a3d;font-weight:700}.lvay-ys .l{color:#b3261e;font-weight:700}
.lvay-ys-note{font-size:13px;color:#777;margin-top:26px}
.lvay-ys-loading{font-family:'Teko',sans-serif;font-size:22px;color:#008584}
@media(max-width:767px){.lvay-ys-head h1{font-size:30px}.lvay-ys-form input{font-size:16px;padding:12px}.lvay-ys-form button{font-size:22px;padding:4px 16px 0}.lvay-ys-ans{font-size:18px}}
</style>
<script>
(function(){
  var API='https://lvay-scraper.onrender.com/api/yall?q=';
  var root=document.getElementById('lvay-ys'); if(!root) return;
  var form=root.querySelector('form'), input=form.q, out=root.querySelector('.lvay-ys-out'),
      tryBox=root.querySelector('.lvay-ys-try'), cov=root.querySelector('.lvay-ys-cov');
  var EX=['When is the last time West Monroe had 50 points put on them?','Ruston vs West Monroe',
    'Longest winning streak for John Curtis','When was Haynesville last undefeated?',
    'Biggest win in Southwood history','When is the last time Acadiana was shut out?'];
  function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];});}
  function chips(list,box){box.innerHTML='';list.forEach(function(t){var b=document.createElement('button');b.type='button';b.className='lvay-ys-chip';b.textContent=t;b.onclick=function(){input.value=t;ask();};box.appendChild(b);});}
  chips(EX,tryBox);
  function table(rows){
    if(!rows||!rows.length) return '';
    var h='<div class="lvay-ys-tablewrap"><table><thead><tr><th>Date</th><th>Team</th><th>Opponent</th><th>Result</th><th>Where</th><th>Game</th></tr></thead><tbody>';
    rows.forEach(function(r){var c=r.result==='W'?'w':(r.result==='L'?'l':'');
      h+='<tr><td>'+esc(r.date||r.season)+'</td><td>'+esc(r.team)+'</td><td>'+esc(r.opponent)+'</td><td class="'+c+'">'+esc(r.result)+' '+esc(r.score)+'</td><td>'+esc(r.where)+'</td><td>'+esc(r.phase)+'</td></tr>';});
    return h+'</tbody></table></div>';
  }
  function show(j){
    if(j.coverage) cov.textContent='Covers: '+j.coverage+'.';
    var miss=j.status!=='ok';
    var h='<div class="lvay-ys-card'+(miss?' miss':'')+'"><p class="lvay-ys-ans">'+esc(j.answer)+'</p>';
    if(j.interpreted_as) h+='<p class="lvay-ys-how">Searched: '+esc(j.interpreted_as)+'</p>';
    h+='</div>';
    out.innerHTML=h+table(j.rows);
    if(j.status==='ambiguous'&&j.choices){var box=document.createElement('div');box.className='lvay-ys-try';out.appendChild(box);
      var q=j.question; chips(j.choices.map(function(name){var base=name.split(' - ')[0];var re=new RegExp(base.replace(/[.*+?^${}()|[\]\\]/g,'\\$&'),'i');return re.test(q)?q.replace(re,name):name+' '+q;}),box);}
    if(miss&&j.examples){var b2=document.createElement('div');b2.className='lvay-ys-try';out.appendChild(b2);chips(j.examples,b2);}
  }
  function ask(){
    var q=input.value.trim(); if(!q) return;
    out.innerHTML='<p class="lvay-ys-loading">Digging through the archives&hellip;</p>';
    try{var u=new URL(location.href);u.searchParams.set('q',q);history.replaceState(null,'',u);}catch(e){}
    fetch(API+encodeURIComponent(q)).then(function(r){
      if(r.status===429) throw new Error('slow');
      return r.json();
    }).then(show).catch(function(e){
      out.innerHTML='<div class="lvay-ys-card miss"><p class="lvay-ys-ans">'+(e.message==='slow'?'Too many questions too fast. Give it a minute.':'The search server is waking up. Try again in a few seconds.')+'</p></div>';
    });
  }
  form.addEventListener('submit',function(e){e.preventDefault();ask();});
  try{var start=new URL(location.href).searchParams.get('q');if(start){input.value=start;ask();}}catch(e){}
})();
</script>
	<?php
	return ob_get_clean();
}
