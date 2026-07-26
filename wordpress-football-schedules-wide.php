/**
 * Shared LVAY wide-screen presentation for football schedules.
 *
 * Keeps the schedule renderer/data logic separate from presentation so future
 * seasons inherit the same visual system as rankings and playoff brackets.
 */
function lvay_football_schedules_wide_styles() {
    $css = <<<'CSS'
.lvay-football-schedules{
    grid-template-columns:minmax(0,1fr) 300px!important;
    gap:24px!important;
    width:min(1600px,calc(100vw - 48px))!important;
    max-width:none!important;
    margin:0!important;
    position:relative;
    left:50%;
    transform:translateX(-50%);
}
.lvay-schedule-title h1{
    font-size:clamp(34px,3vw,46px)!important;
}
.lvay-schedule-title p{
    font-size:22px!important;
}
#lvay-school-search{
    height:50px!important;
    padding:10px 15px!important;
    font-size:18px!important;
}
.lvay-search-status{
    min-height:20px!important;
    font-size:15px!important;
}
.lvay-class>summary{
    padding:14px 4px 10px!important;
    font-size:27px!important;
}
.lvay-district>summary{
    padding:12px 16px!important;
    font-size:20px!important;
}
.lvay-caret{
    font-size:32px!important;
}
.lvay-school-toggle{
    padding:11px 20px!important;
    font-size:25px!important;
}
.lvay-school-toggle small{
    font-size:19px!important;
}
.lvay-school-meta{
    padding:8px 11px!important;
    font-size:20px!important;
}
.lvay-school table{
    min-width:850px!important;
    font-size:20px!important;
}
.lvay-school th{
    padding:8px 10px!important;
    font-size:18px!important;
}
.lvay-school td{
    padding:7px 10px!important;
}
.lvay-season-archives{
    padding:20px 24px 24px!important;
}
.lvay-season-archives h2{
    margin:0 0 12px!important;
    font-family:Teko,Arial,sans-serif!important;
    font-size:34px!important;
    font-weight:500!important;
    line-height:1!important;
    letter-spacing:.8px!important;
}
.lvay-season-grid{
    gap:7px 18px!important;
}
.lvay-season-grid a,
.lvay-season-grid span{
    font-family:Teko,Arial,sans-serif!important;
    font-size:25px!important;
    font-weight:500!important;
    line-height:1.05!important;
}
.lvay-season-grid span{
    color:#555!important;
}
.lvay-decade-links{
    font:italic 18px Teko,Arial,sans-serif!important;
    font-weight:400!important;
}
@media(max-width:1200px){
    .lvay-football-schedules{
        grid-template-columns:1fr!important;
    }
    .lvay-schedule-main{
        grid-row:1!important;
    }
    .lvay-season-archives{
        grid-row:2!important;
        order:2!important;
    }
}
@media(max-width:600px){
    .lvay-football-schedules{
        width:calc(100vw - 24px)!important;
    }
    .lvay-class>summary{font-size:23px!important}
    .lvay-district>summary{font-size:18px!important}
    .lvay-school-toggle{font-size:22px!important}
}
CSS;
    wp_register_style('lvay-football-schedules-wide', false);
    wp_enqueue_style('lvay-football-schedules-wide');
    wp_add_inline_style('lvay-football-schedules-wide', $css);
}
add_action('wp_enqueue_scripts', 'lvay_football_schedules_wide_styles', 50);
