/**
 * Autocomplete school search for current and archived football schedules.
 */
function lvay_football_schedule_autocomplete_assets() {
    $css = <<<'CSS'
.lvay-schedule-main{position:relative}
#lvay-school-search{margin-bottom:0!important}
#lvay-school-suggestions{
    display:none;
    position:absolute;
    left:0;
    right:0;
    z-index:999;
    max-height:430px;
    overflow-y:auto;
    border:2px solid #078b88;
    border-top:0;
    background:#fff;
    box-shadow:0 10px 24px rgba(0,0,0,.18);
}
#lvay-school-suggestions.is-open{display:block}
.lvay-school-suggestion{
    display:flex;
    width:100%;
    justify-content:space-between;
    align-items:center;
    gap:20px;
    padding:10px 15px;
    border:0;
    border-bottom:1px solid #ddd;
    background:#fff;
    color:#111;
    font-family:Teko,Arial,sans-serif;
    font-size:23px;
    line-height:1.05;
    text-align:left;
    cursor:pointer;
}
.lvay-school-suggestion:hover,
.lvay-school-suggestion.is-active{
    background:#050505;
    color:#fff;
}
.lvay-school-suggestion small{
    color:#777;
    font-size:17px;
    white-space:nowrap;
}
.lvay-school-suggestion:hover small,
.lvay-school-suggestion.is-active small{color:#fff}
CSS;
    wp_register_style('lvay-football-schedule-autocomplete', false);
    wp_enqueue_style('lvay-football-schedule-autocomplete');
    wp_add_inline_style('lvay-football-schedule-autocomplete', $css);
}
add_action('wp_enqueue_scripts', 'lvay_football_schedule_autocomplete_assets', 70);

function lvay_football_schedule_autocomplete_script() {
    ?>
    <script>
    (function(){
      function initLvayAutocomplete(){
        const root=document.getElementById('lvay-football-schedules');
        if(!root||root.dataset.autocompleteReady==='1')return;
        root.dataset.autocompleteReady='1';
        const search=root.querySelector('#lvay-school-search');
        const status=root.querySelector('#lvay-search-status');
        if(!search)return;

        const list=document.createElement('div');
        list.id='lvay-school-suggestions';
        list.setAttribute('role','listbox');
        list.setAttribute('aria-label','Matching schools');
        search.insertAdjacentElement('afterend',list);
        search.setAttribute('aria-controls',list.id);
        search.setAttribute('aria-autocomplete','list');
        search.setAttribute('aria-expanded','false');

        const normalize=value=>(value||'').toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();
        const schools=Array.from(root.querySelectorAll('.lvay-school')).map(article=>{
          const button=article.querySelector('.lvay-school-toggle');
          const district=article.closest('.lvay-district')?.querySelector(':scope > summary')?.textContent.trim()||'';
          return {article,name:(button?.querySelector('span')?.textContent||article.dataset.school||'').trim(),district};
        });
        let activeIndex=-1;
        let visible=[];

        function closeList(){
          list.classList.remove('is-open');
          list.innerHTML='';
          visible=[];
          activeIndex=-1;
          search.setAttribute('aria-expanded','false');
          search.removeAttribute('aria-activedescendant');
        }
        function openSchool(article){
          root.querySelectorAll('.lvay-school-body').forEach(body=>body.hidden=true);
          root.querySelectorAll('.lvay-school-toggle').forEach(button=>button.setAttribute('aria-expanded','false'));
          let parent=article.parentElement;
          while(parent&&parent!==root){
            if(parent.tagName==='DETAILS')parent.open=true;
            parent=parent.parentElement;
          }
          const body=article.querySelector('.lvay-school-body');
          const button=article.querySelector('.lvay-school-toggle');
          body.hidden=false;
          button.setAttribute('aria-expanded','true');
          search.value=button.querySelector('span')?.textContent.trim()||article.dataset.school;
          closeList();
          if(status)status.textContent='';
          setTimeout(()=>article.scrollIntoView({behavior:'smooth',block:'start'}),50);
        }
        function setActive(index){
          const options=Array.from(list.querySelectorAll('.lvay-school-suggestion'));
          options.forEach(option=>option.classList.remove('is-active'));
          if(!options.length){activeIndex=-1;return}
          activeIndex=(index+options.length)%options.length;
          options[activeIndex].classList.add('is-active');
          search.setAttribute('aria-activedescendant',options[activeIndex].id);
          options[activeIndex].scrollIntoView({block:'nearest'});
        }
        function render(query){
          const normalized=normalize(query);
          root.querySelectorAll('.lvay-school').forEach(article=>article.hidden=false);
          if(!normalized){closeList();if(status)status.textContent='';return}
          visible=schools.filter(item=>normalize(item.name).includes(normalized)).slice(0,12);
          list.innerHTML='';
          visible.forEach((item,index)=>{
            const option=document.createElement('button');
            option.type='button';
            option.className='lvay-school-suggestion';
            option.id='lvay-school-suggestion-'+index;
            option.setAttribute('role','option');
            const name=document.createElement('span');
            name.textContent=item.name;
            const district=document.createElement('small');
            district.textContent=item.district;
            option.append(name,district);
            option.addEventListener('mousedown',event=>{
              event.preventDefault();
              openSchool(item.article);
            });
            list.appendChild(option);
          });
          if(status)status.textContent=visible.length
            ? visible.length+' matching school'+(visible.length===1?'':'s')
            : 'No matching schools';
          list.classList.toggle('is-open',visible.length>0);
          search.setAttribute('aria-expanded',visible.length>0?'true':'false');
          activeIndex=-1;
        }

        search.addEventListener('input',event=>{
          event.stopImmediatePropagation();
          render(search.value);
        },true);
        search.addEventListener('keydown',event=>{
          if(event.key==='ArrowDown'){event.preventDefault();setActive(activeIndex+1)}
          else if(event.key==='ArrowUp'){event.preventDefault();setActive(activeIndex-1)}
          else if(event.key==='Enter'&&activeIndex>=0){event.preventDefault();openSchool(visible[activeIndex].article)}
          else if(event.key==='Escape'){closeList()}
        });
        document.addEventListener('click',event=>{
          if(!event.target.closest('#lvay-school-search')&&!event.target.closest('#lvay-school-suggestions'))closeList();
        });
      }
      if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',initLvayAutocomplete);
      else initLvayAutocomplete();
    })();
    </script>
    <?php
}
add_action('wp_footer', 'lvay_football_schedule_autocomplete_script', 70);
