/**
 * Native, nonblocking school autocomplete for football schedules.
 */
function lvay_football_schedule_native_search_styles_v2() {
    $css = '#lvay-school-search{font-family:Teko,Arial,sans-serif;font-size:21px!important}';
    wp_register_style('lvay-football-schedule-native-search', false);
    wp_enqueue_style('lvay-football-schedule-native-search');
    wp_add_inline_style('lvay-football-schedule-native-search', $css);
}
add_action('wp_enqueue_scripts', 'lvay_football_schedule_native_search_styles_v2', 80);

function lvay_football_schedule_native_search_script_v2() {
    ?>
    <script>
    (function(){
      function initLvayNativeSearch(){
        const root=document.getElementById('lvay-football-schedules');
        if(!root||root.dataset.nativeSearchReady==='1')return;
        root.dataset.nativeSearchReady='1';

        const legacy=root.querySelector('#lvay-school-search');
        const status=root.querySelector('#lvay-search-status');
        if(!legacy)return;
        const search=legacy.cloneNode(true);
        legacy.replaceWith(search);
        search.removeAttribute('aria-controls');
        search.removeAttribute('aria-expanded');
        search.removeAttribute('aria-activedescendant');
        search.setAttribute('autocomplete','off');

        const normalize=value=>(value||'').toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();
        const schools=Array.from(root.querySelectorAll('.lvay-school')).map(article=>{
          const button=article.querySelector('.lvay-school-toggle');
          const name=(button?.querySelector('span')?.textContent||article.dataset.school||'').trim();
          const district=article.closest('.lvay-district')?.querySelector(':scope > summary')?.textContent.trim()||'';
          return {article,name,key:normalize(name),district};
        });

        const datalist=document.createElement('datalist');
        datalist.id='lvay-football-school-options';
        schools.forEach(item=>{
          const option=document.createElement('option');
          option.value=item.name;
          option.label=item.district;
          datalist.appendChild(option);
        });
        root.appendChild(datalist);
        search.setAttribute('list',datalist.id);

        function openSchool(item){
          root.querySelectorAll('.lvay-school-body:not([hidden])').forEach(body=>body.hidden=true);
          root.querySelectorAll('.lvay-school-toggle[aria-expanded="true"]').forEach(button=>button.setAttribute('aria-expanded','false'));
          let parent=item.article.parentElement;
          while(parent&&parent!==root){
            if(parent.tagName==='DETAILS')parent.open=true;
            parent=parent.parentElement;
          }
          const body=item.article.querySelector('.lvay-school-body');
          const button=item.article.querySelector('.lvay-school-toggle');
          if(body.hidden)button.click();
          search.value=item.name;
          if(status)status.textContent='';
          let attempts=0;
          function centerWhenReady(){
            attempts++;
            if(body.dataset.loaded==='1'||attempts>=40){
              item.article.scrollIntoView({behavior:'smooth',block:'center'});
              return;
            }
            window.setTimeout(centerWhenReady,50);
          }
          centerWhenReady();
        }
        function handleValue(){
          const key=normalize(search.value);
          if(!key){if(status)status.textContent='';return}
          const exact=schools.find(item=>item.key===key);
          if(exact){openSchool(exact);return}
          const count=schools.reduce((total,item)=>total+(item.key.includes(key)?1:0),0);
          if(status)status.textContent=count
            ? count+' matching school'+(count===1?'':'s')
            : 'No matching schools';
        }
        search.addEventListener('input',event=>{
          event.stopImmediatePropagation();
          handleValue();
        },true);
        search.addEventListener('change',handleValue);
      }
      if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',initLvayNativeSearch);
      else initLvayNativeSearch();
    })();
    </script>
    <?php
}
add_action('wp_footer', 'lvay_football_schedule_native_search_script_v2', 80);
