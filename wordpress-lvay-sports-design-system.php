/**
 * LVAY Sports Design System
 *
 * Football is the canonical presentation. This late-loading stylesheet maps
 * Baseball, Softball, Basketball, and Soccer onto the same visual tokens.
 * Paste into Code Snippets without an opening PHP tag.
 */

function lvay_sports_design_system_pages() {
    return array(
        'schedules', 'power-rankings', 'playoff-brackets',
        'baseball-schedules', 'baseball-power-rankings', 'baseball-playoff-brackets',
        'softball-schedules', 'softball-power-rankings', 'softball-playoff-brackets',
        'boys-basketball-schedules', 'boys-basketball-power-rankings', 'boys-basketball-playoff-brackets',
        'girls-basketball-schedules', 'girls-basketball-power-rankings', 'girls-basketball-playoff-brackets',
        'boys-soccer-schedules', 'boys-soccer-power-rankings', 'boys-soccer-playoff-brackets',
        'girls-soccer-schedules', 'girls-soccer-power-rankings', 'girls-soccer-playoff-brackets',
    );
}

function lvay_sports_design_system() {
    if (!is_page(lvay_sports_design_system_pages())) return;

    wp_enqueue_style(
        'lvay-sports-fonts',
        'https://fonts.googleapis.com/css2?family=Alfa+Slab+One&family=Teko:wght@400;500;600;700&display=swap',
        array(),
        null
    );
    wp_register_style('lvay-sports-design-system', false);
    wp_enqueue_style('lvay-sports-design-system');

    $css = <<<'CSS'
:root{
  --lvay-teal:#078b88;
  --lvay-caret:#5dc7c1;
  --lvay-ink:#080808;
  --lvay-selected:#333;
  --lvay-win:#00a651;
  --lvay-loss:#e53935;
}

/* Shared page frame */
:is(.lvay-football-schedules,.lvay-bb-layout,.lvay-sb-layout,.lvay-w-layout){
  grid-template-columns:minmax(0,1fr) 300px!important;
  gap:24px!important;
  width:min(1600px,calc(100vw - 48px))!important;
  max-width:none!important;
  margin:0!important;
  position:relative!important;
  left:50%!important;
  transform:translateX(-50%)!important;
  padding:18px 0 36px!important;
  color:var(--lvay-ink);
}

/* Page titles: canonical Football style */
:is(.lvay-schedule-title,.lvay-bb-title,.lvay-sb-title,.lvay-w-layout>main>header) h1{
  margin:0 0 17px!important;
  color:var(--lvay-teal)!important;
  font-family:"Alfa Slab One",Rockwell,serif!important;
  font-size:clamp(34px,3vw,46px)!important;
  font-weight:400!important;
  line-height:.95!important;
  letter-spacing:.2px!important;
  text-transform:uppercase!important;
}

/* Archive panels */
:is(.lvay-season-archives,.lvay-bb-archive,.lvay-sb-archive,.lvay-w-archive){
  align-self:start!important;
  padding:20px 24px 24px!important;
  background:#050505!important;
  color:#fff!important;
}
:is(.lvay-season-archives h2,.lvay-bb-archive h3,.lvay-sb-archive h3,.lvay-w-archive h3){
  margin:0 0 12px!important;
  color:#fff!important;
  font-family:Teko,Arial,sans-serif!important;
  font-size:34px!important;
  font-weight:500!important;
  line-height:1!important;
  letter-spacing:.8px!important;
  text-decoration:underline!important;
  text-underline-offset:5px!important;
}
:is(.lvay-season-grid a,.lvay-season-grid span,.lvay-bb-archive a,.lvay-sb-archive a,.lvay-w-archive a){
  font-family:Teko,Arial,sans-serif!important;
  font-size:25px!important;
  font-weight:500!important;
  line-height:1.05!important;
}
:is(.lvay-bb-archive a,.lvay-sb-archive a,.lvay-w-archive a){color:#999!important}
:is(.lvay-bb-archive a.active,.lvay-sb-archive a.active,.lvay-w-archive a.active),
:is(.lvay-bb-archive a:hover,.lvay-sb-archive a:hover,.lvay-w-archive a:hover){color:#fff!important}
:is(.lvay-bb-archive span,.lvay-sb-archive span,.lvay-w-archive span){
  color:#aaa!important;
  font:400 18px/1.15 Teko,Arial,sans-serif!important;
}

/* Search */
:is(#lvay-school-search,#lvay-bb-search,#lvay-sb-search,.lvay-w-search input){
  display:block!important;
  width:100%!important;
  height:50px!important;
  padding:10px 15px!important;
  border:2px solid var(--lvay-teal)!important;
  border-radius:4px!important;
  background:#fff!important;
  color:#111!important;
  font-size:18px!important;
}

/* Class/district accordions */
:is(.lvay-class>summary,.lvay-bb-class>summary,.lvay-sb-class>summary,.lvay-w-class>summary){
  padding:14px 4px 10px!important;
  border-bottom:2px solid var(--lvay-teal)!important;
  color:#090909!important;
  font-family:"Alfa Slab One",Rockwell,serif!important;
  font-size:27px!important;
}
:is(.lvay-district>summary,.lvay-bb-district>summary,.lvay-sb-district>summary,.lvay-w-district>summary){
  padding:12px 16px!important;
  color:#090909!important;
  font-family:"Alfa Slab One",Rockwell,serif!important;
  font-size:20px!important;
}
:is(.lvay-caret,.lvay-bb-class summary i,.lvay-bb-district summary i,.lvay-sb-class summary i,.lvay-sb-district summary i,.lvay-w-class summary i,.lvay-w-district summary i){
  color:var(--lvay-caret)!important;
  font-family:Arial,sans-serif!important;
  font-size:32px!important;
  font-style:normal!important;
  line-height:.7!important;
}

/* School rows */
:is(.lvay-school-toggle,.lvay-bb-school-toggle,.lvay-sb-school-toggle,.lvay-w-school>button){
  width:100%!important;
  min-height:50px!important;
  padding:11px 20px!important;
  border:0!important;
  border-bottom:1px solid #e7e9e9!important;
  background:#fff!important;
  color:#080808!important;
  font-family:Teko,Arial,sans-serif!important;
  font-size:25px!important;
  font-weight:500!important;
  text-align:left!important;
}
:is(.lvay-school-toggle,.lvay-bb-school-toggle,.lvay-sb-school-toggle,.lvay-w-school>button):hover{
  background:#050505!important;
  color:#fff!important;
}
:is(.lvay-school-toggle[aria-expanded=true],.lvay-bb-school-toggle[aria-expanded=true],.lvay-sb-school-toggle[aria-expanded=true],.lvay-w-school>button[aria-expanded=true]){
  background:var(--lvay-selected)!important;
  color:#fff!important;
}

/* Schedule metadata and tables */
:is(.lvay-school-meta,.lvay-bb-meta,.lvay-sb-meta){
  padding:8px 11px!important;
  background:var(--lvay-teal)!important;
  color:#fff!important;
  font-family:Teko,Arial,sans-serif!important;
  font-size:20px!important;
}
:is(.lvay-school table,.lvay-bb-table-scroll table,.lvay-sb-table-scroll table,.lvay-w-layout table){
  width:100%!important;
  min-width:850px!important;
  border-collapse:collapse!important;
  font-family:Teko,Arial,sans-serif!important;
  font-size:20px!important;
}
:is(.lvay-school th,.lvay-bb-table-scroll th,.lvay-sb-table-scroll th,.lvay-w-layout th){
  padding:8px 10px!important;
  background:var(--lvay-teal)!important;
  color:#fff!important;
  font-size:18px!important;
  text-align:left!important;
}
:is(.lvay-school td,.lvay-bb-table-scroll td,.lvay-sb-table-scroll td,.lvay-w-layout td){
  padding:7px 10px!important;
}
:is(.result-w,.lvay-bb-table-scroll td.win,.lvay-sb-table-scroll td.win,.lvay-w-game.w td:nth-child(4)){
  color:var(--lvay-win)!important;
  font-weight:700!important;
}
:is(.result-l,.lvay-bb-table-scroll td.loss,.lvay-sb-table-scroll td.loss,.lvay-w-game.l td:nth-child(4)){
  color:var(--lvay-loss)!important;
  font-weight:700!important;
}
:is(.is-district,.lvay-bb-table-scroll tr.district,.lvay-sb-table-scroll tr.district,.lvay-w-game.district){
  color:var(--lvay-teal)!important;
  font-weight:700!important;
}

@media(max-width:1200px){
  :is(.lvay-football-schedules,.lvay-bb-layout,.lvay-sb-layout,.lvay-w-layout){
    grid-template-columns:1fr!important;
  }
  :is(.lvay-schedule-main,.lvay-bb-main,.lvay-sb-main,.lvay-w-layout>main){
    grid-row:1!important;
  }
  :is(.lvay-season-archives,.lvay-bb-archive,.lvay-sb-archive,.lvay-w-archive){
    grid-row:2!important;
    order:2!important;
  }
}
@media(max-width:600px){
  :is(.lvay-football-schedules,.lvay-bb-layout,.lvay-sb-layout,.lvay-w-layout){
    width:calc(100vw - 24px)!important;
  }
  :is(.lvay-class>summary,.lvay-bb-class>summary,.lvay-sb-class>summary,.lvay-w-class>summary){font-size:23px!important}
  :is(.lvay-district>summary,.lvay-bb-district>summary,.lvay-sb-district>summary,.lvay-w-district>summary){font-size:18px!important}
  :is(.lvay-school-toggle,.lvay-bb-school-toggle,.lvay-sb-school-toggle,.lvay-w-school>button){font-size:22px!important}
}
CSS;

    wp_add_inline_style('lvay-sports-design-system', $css);
}
add_action('wp_enqueue_scripts', 'lvay_sports_design_system', 100);
