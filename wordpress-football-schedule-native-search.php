/**
 * Native, nonblocking school autocomplete for football schedules.
 */
function lvay_football_schedule_native_search_styles_v3() {
    $css = '
    .lvay-search-wrap{position:relative;z-index:30}
    #lvay-school-search{font-family:Teko,Arial,sans-serif;font-size:21px!important}
    .lvay-search-suggestions{position:absolute;top:calc(100% - 12px);left:0;right:0;z-index:9999;max-height:360px;overflow-y:auto;border:1px solid #078b88;border-top:0;background:#fff;box-shadow:0 10px 24px rgba(0,0,0,.2)}
    .lvay-search-suggestions[hidden]{display:none!important}
    .lvay-search-option{display:flex;width:100%;align-items:center;justify-content:space-between;gap:15px;padding:9px 13px;border:0;border-bottom:1px solid #e4e8e8;background:#fff;color:#111;text-align:left;cursor:pointer;font:500 21px/1.05 Teko,Arial,sans-serif}
    .lvay-search-option:hover,.lvay-search-option.is-active{background:#050505;color:#fff}
    .lvay-search-option small{color:#078b88;font-size:16px}
    .lvay-search-option:hover small,.lvay-search-option.is-active small{color:#6dd1cc}
    ';
    wp_register_style('lvay-football-schedule-native-search', false);
    wp_enqueue_style('lvay-football-schedule-native-search');
    wp_add_inline_style('lvay-football-schedule-native-search', $css);
}
add_action('wp_enqueue_scripts', 'lvay_football_schedule_native_search_styles_v3', 80);

function lvay_football_schedule_native_search_script_v3() {
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

        search.removeAttribute('list');
        const wrap=document.createElement('div');
        wrap.className='lvay-search-wrap';
        search.parentNode.insertBefore(wrap,search);
        wrap.appendChild(search);
        const suggestions=document.createElement('div');
        suggestions.className='lvay-search-suggestions';
        suggestions.id='lvay-football-school-options';
        suggestions.hidden=true;
        suggestions.setAttribute('role','listbox');
        wrap.appendChild(suggestions);
        search.setAttribute('aria-controls',suggestions.id);
        search.setAttribute('aria-expanded','false');
        let matches=[];
        let activeIndex=-1;

        function hideSuggestions(){
          suggestions.hidden=true;
          suggestions.innerHTML='';
          search.setAttribute('aria-expanded','false');
          activeIndex=-1;
        }
        function setActive(index){
          const options=suggestions.querySelectorAll('.lvay-search-option');
          if(!options.length)return;
          activeIndex=(index+options.length)%options.length;
          options.forEach((option,i)=>option.classList.toggle('is-active',i===activeIndex));
          options[activeIndex].scrollIntoView({block:'nearest'});
        }
        function renderSuggestions(){
          const key=normalize(search.value);
          if(!key){hideSuggestions();if(status)status.textContent='';return}
          matches=schools.filter(item=>item.key.includes(key)).slice(0,12);
          suggestions.innerHTML='';
          matches.forEach((item,index)=>{
            const option=document.createElement('button');
            option.type='button';
            option.className='lvay-search-option';
            option.setAttribute('role','option');
            const label=document.createElement('span');
            label.textContent=item.name;
            const district=document.createElement('small');
            district.textContent=item.district;
            option.append(label,district);
            option.addEventListener('mousedown',event=>event.preventDefault());
            option.addEventListener('click',()=>openSchool(item));
            suggestions.appendChild(option);
          });
          suggestions.hidden=!matches.length;
          search.setAttribute('aria-expanded',String(!!matches.length));
          activeIndex=-1;
          if(status)status.textContent=matches.length
            ? matches.length+' matching school'+(matches.length===1?'':'s')
            : 'No matching schools';
        }

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
          hideSuggestions();
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
        search.addEventListener('input',event=>{
          event.stopImmediatePropagation();
          renderSuggestions();
        },true);
        search.addEventListener('keydown',event=>{
          if(event.key==='ArrowDown'){event.preventDefault();setActive(activeIndex+1)}
          else if(event.key==='ArrowUp'){event.preventDefault();setActive(activeIndex-1)}
          else if(event.key==='Enter'&&activeIndex>=0){event.preventDefault();openSchool(matches[activeIndex])}
          else if(event.key==='Escape'){hideSuggestions()}
        });
        search.addEventListener('focus',renderSuggestions);
        document.addEventListener('click',event=>{if(!wrap.contains(event.target))hideSuggestions()});
      }
      if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',initLvayNativeSearch);
      else initLvayNativeSearch();
    })();
    </script>
    <?php
}
add_action('wp_footer', 'lvay_football_schedule_native_search_script_v3', 80);
