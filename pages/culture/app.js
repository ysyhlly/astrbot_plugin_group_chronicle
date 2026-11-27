import {confirmAction,getSharedScope,msg,setSharedScope,setText} from './series-ui.js';

const $=id=>document.getElementById(id);
const ALL_TABS=['library','review','editions','events','members','progress'];
const states={approved:'已公开',pending:'待审核',disputed:'待纠正',rejected:'已驳回',hidden:'已隐藏',merged:'已合并'};
const eventStates={approved:'已通过',pending:'待审核',rejected:'已驳回',merged:'已合并'};
const actions={approve:'审核通过',edit:'修订',reject:'驳回',hide:'隐藏',restore:'撤销隐藏',correct:'提出纠正',merge:'合并',evidence:'追加证据'};
let epoch=0,detailEpoch=0,page=1,reviewPage=1,selected=null,mergeTarget=null,busy=false,currentTab='library',lastTrigger=null,overviewEnabled=false;
let selectedCultureIds=new Set(),selectedEventIds=new Set(),cachedMembers=[],cachedMembersHidden=0,cachedEditions=[];

const el=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=String(text);if(cls)n.className=cls;return n;};
const date=t=>t?new Date(t*1000).toLocaleString('zh-CN'):'未知';
const shortDate=t=>t?new Date(t*1000).toLocaleDateString('zh-CN'):'未知';
const statusClass=st=>`badge badge--${states[st]?st:'hidden'}`;

function notice(text,error=false){
  const node=$('notice');
  if(!node)return;
  node.textContent=text;
  node.dataset.error=String(error);
}

async function api(path,body){
  const bridge=(window.DynamicsPanelBridge || window.AstrBotPluginPage);
  if(!bridge)throw Error('请从 AstrBot 插件详情打开此页面。');
  const r=await (body?bridge.apiPost(path,body):bridge.apiGet(path));
  if(r.status==='error'||r.ok===false)throw Error(r.message||'请求失败');
  return r.data??r;
}

const request=(op,fields={})=>api('culture',{op,scope:$('scope').value,...fields});

function closeDetail({restoreFocus=false}={}){
  selected=null;
  mergeTarget=null;
  $('merge-preview').hidden=true;
  ++detailEpoch;
  $('detail').hidden=true;
  const backdrop=$('detail-backdrop');
  if(backdrop)backdrop.hidden=true;
  document.body.classList.remove('gc-drawer-open');
  if(restoreFocus&&lastTrigger&&typeof lastTrigger.focus==='function'&&lastTrigger.isConnected){
    try{lastTrigger.focus({preventScroll:true});}catch{/* ignore */}
  }
  lastTrigger=null;
}

function makeEmptyCard(title,subtitle){
  const box=el('div',undefined,'card empty');
  const svgNS='http://www.w3.org/2000/svg';
  const svg=document.createElementNS(svgNS,'svg');
  svg.setAttribute('class','empty-icon');
  svg.setAttribute('width','36');
  svg.setAttribute('height','36');
  svg.setAttribute('viewBox','0 0 24 24');
  svg.setAttribute('fill','none');
  svg.setAttribute('stroke','currentColor');
  svg.setAttribute('stroke-width','1.5');
  svg.setAttribute('aria-hidden','true');
  const p1=document.createElementNS(svgNS,'path');
  p1.setAttribute('d','M4 19.5A2.5 2.5 0 0 1 6.5 17H20');
  const p2=document.createElementNS(svgNS,'path');
  p2.setAttribute('d','M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z');
  svg.append(p1,p2);
  box.append(svg,el('p',title,'empty-title'),el('p',subtitle,'empty-sub'));
  return box;
}

function syncCultureBatchCount(){
  const cnt=$('culture-selected-count');
  if(cnt)cnt.textContent=`已选 ${selectedCultureIds.size} 项`;
  const allCb=$('culture-select-all');
  const cbs=[...document.querySelectorAll('#review-entries .gc-select-cb')];
  if(allCb){
    allCb.checked=cbs.length>0&&cbs.every(cb=>cb.checked);
    allCb.indeterminate=cbs.some(cb=>cb.checked)&&!allCb.checked;
  }
}

function syncEventBatchCount(){
  const cnt=$('events-selected-count');
  if(cnt)cnt.textContent=`已选 ${selectedEventIds.size} 项`;
  const allCb=$('events-select-all');
  const cbs=[...document.querySelectorAll('#events-list .gc-event-cb')];
  if(allCb){
    allCb.checked=cbs.length>0&&cbs.every(cb=>cb.checked);
    allCb.indeterminate=cbs.some(cb=>cb.checked)&&!allCb.checked;
  }
}

function entryCard(e,{selectable=false}={}){
  const box=el('article',undefined,'card entry gc-dict-card');
  box.dataset.status=e.status||'hidden';

  const head=el('div',undefined,'entry-head');
  const titleWrap=el('div',undefined,'gc-entry-title-group');
  if(selectable&&!e.unavailable){
    const wrap=el('label',undefined,'gc-select-wrap');
    const cb=document.createElement('input');
    cb.type='checkbox';
    cb.className='gc-select-cb';
    cb.dataset.entryId=String(e.id);
    cb.setAttribute('aria-label',`选择词条 #${e.id} ${e.name}`);
    cb.checked=selectedCultureIds.has(e.id);
    cb.onchange=()=>{
      if(cb.checked)selectedCultureIds.add(e.id);else selectedCultureIds.delete(e.id);
      syncCultureBatchCount();
    };
    wrap.append(cb);
    titleWrap.append(wrap);
  }
  const title=el('h3',undefined,'entry-title');
  title.append(el('span',`#${e.id}`,'entry-id'),document.createTextNode(' '),el('span',e.name,'entry-name'));
  titleWrap.append(title);
  const badge=el('span',states[e.status]||e.status,statusClass(e.status));
  head.append(titleWrap,badge);
  box.append(head);

  box.append(el('p',e.unavailable?'来源不可用或条目已隐藏，解释暂停展示。':e.meaning,'entry-meaning'));

  if(!e.unavailable){
    if(Array.isArray(e.aliases)&&e.aliases.length){
      const aliases=el('div',undefined,'entry-aliases');
      for(const a of e.aliases.slice(0,4)){
        aliases.append(el('span',a.name,'alias-chip'));
      }
      box.append(aliases);
    }

    const originStrip=el('div',undefined,'entry-origin-strip');
    const row=el('div',undefined,'origin-row');
    if(e.origin?.text){
      row.append(el('span','起源已核对','origin-pill origin-pill--known'));
    }else if(e.proposed_origin?.text){
      row.append(el('span','起源待审核','origin-pill origin-pill--proposed'));
    }else{
      row.append(el('span','起源未知','origin-pill origin-pill--unknown'));
    }
    row.append(el('span',`最早留存：${shortDate(e.first_seen)}`,'origin-date'));
    const srcCount=Array.isArray(e.sources)?e.sources.length:0;
    if(srcCount)row.append(el('span',`${srcCount} 条证据`,'origin-sources'));
    originStrip.append(row);

    if(Array.isArray(e.timeline)&&e.timeline.length){
      const mini=el('ol',undefined,'mini-timeline');
      for(const node of e.timeline.slice(0,2)){
        mini.append(el('li',node.text));
      }
      originStrip.append(mini);
    }
    box.append(originStrip);

    const foot=el('div',undefined,'entry-foot');
    foot.append(el('span',e.usage?`场景：${e.usage}`:`留存：${date(e.first_seen)}`,'entry-usage muted'));
    const btn=el('button','查看沿革与证据','entry-open-btn');
    btn.type='button';
    btn.onclick=()=>showDetail(e.id,btn).catch(err=>notice(err.message,true));
    foot.append(btn);
    box.append(foot);
  }else if(e.status==='hidden'&&e.can_restore){
    const foot=el('div',undefined,'entry-foot');
    foot.append(el('span','隐私隐藏中，支持审计恢复','entry-usage muted'));
    const btn=el('button','撤销隐藏/恢复公开','entry-open-btn');
    btn.type='button';
    btn.onclick=()=>showDetail(e.id,btn).catch(err=>notice(err.message,true));
    foot.append(btn);
    box.append(foot);
  }
  return box;
}

function renderCards(container,items,{selectable=false}={}){
  container.replaceChildren(...items.map(item=>entryCard(item,{selectable})));
  if(!items.length){
    container.append(makeEmptyCard('暂无符合条件的词条','完成群文化整理或调整筛选条件后，词条会以词典卡片形式呈现在这里。'));
  }
}

async function loadList(){
  if(!$('scope').value)return;
  const ticket=++epoch;
  const review=currentTab==='review';
  if(review)selectedCultureIds.clear();
  const d=await request('list',{
    query:review?'':$('query').value.trim(),
    status:review?$('review-status').value:$('status').value,
    page:review?reviewPage:page,
  });
  if(ticket!==epoch)return;
  renderCards($(review?'review-entries':'entries'),d.entries,{selectable:review});
  if(review)syncCultureBatchCount();
  const p=review?reviewPage:page;
  $(review?'review-count':'count').textContent=`共 ${d.total} 条 · 第 ${p} 页`;
  $(review?'review-prev':'prev').disabled=p<=1;
  $(review?'review-next':'next').disabled=p*20>=d.total;
  if(!review)$('page-number').textContent=String(page);
}

function renderEditionComparison(comp){
  const box=$('editions-comparison');
  if(!box)return;
  if(!comp){box.hidden=true;box.replaceChildren();return;}
  box.hidden=false;
  box.replaceChildren();
  const fmtDiff=n=>n>0?`+${n}`:String(n);
  box.append(
    el('h3',`卷册对比：#${comp.left_id} vs #${comp.right_id}`),
    el('p',`字数变化：${fmtDiff(comp.char_diff)} 字 · 事件变化：${fmtDiff(comp.event_diff)} 条 · 消息变化：${fmtDiff(comp.msg_diff)} 条`,'tabular-nums'),
    el('p',`新增事件 ID：${comp.added_event_ids.length?comp.added_event_ids.map(id=>`#${id}`).join('、'):'无'}`,'muted'),
    el('p',`减少事件 ID：${comp.removed_event_ids.length?comp.removed_event_ids.map(id=>`#${id}`).join('、'):'无'}`,'muted'),
  );
}

async function loadEditions({reportId=null,compareId=null}={}){
  if(!$('scope').value)return;
  const ticket=++epoch;
  const params={};
  if(reportId!==null&&compareId!==null){params.report_id=reportId;params.compare_id=compareId;}
  const d=await request('editions',params);
  if(ticket!==epoch)return;
  cachedEditions=Array.isArray(d.editions)?d.editions:[];
  const cnt=$('editions-count');
  if(cnt)cnt.textContent=`共 ${d.total||cachedEditions.length} 卷`;

  const leftSel=$('compare-left'),rightSel=$('compare-right');
  if(leftSel&&rightSel){
    const prevLeft=leftSel.value,prevRight=rightSel.value;
    const makeOpts=()=>cachedEditions.map(ed=>{
      const opt=el('option',`#${ed.report_id} · ${ed.period_label} · ${ed.iso_year}-W${String(ed.iso_week).padStart(2,'0')} v${ed.version}`);
      opt.value=String(ed.report_id);
      return opt;
    });
    leftSel.replaceChildren(...makeOpts());
    rightSel.replaceChildren(...makeOpts());
    if(cachedEditions.length>=1){
      leftSel.value=cachedEditions.some(e=>String(e.report_id)===prevLeft)?prevLeft:String(cachedEditions[0].report_id);
      const fallbackRight=cachedEditions.length>=2?String(cachedEditions[1].report_id):String(cachedEditions[0].report_id);
      rightSel.value=cachedEditions.some(e=>String(e.report_id)===prevRight)?prevRight:fallbackRight;
    }
    const cmpBtn=$('compare-btn');
    if(cmpBtn)cmpBtn.disabled=cachedEditions.length<2;
  }

  renderEditionComparison(d.comparison||null);
  const list=$('editions-list');
  if(!list)return;
  list.replaceChildren();
  if(!cachedEditions.length){
    list.append(makeEmptyCard('暂无群史卷册','发送「群史记」或等待定时编纂后，历期周卷、月卷与年卷会呈现在这里。'));
    return;
  }
  for(const ed of cachedEditions){
    const card=el('article',undefined,'card entry gc-dict-card');
    const head=el('div',undefined,'entry-head');
    const title=el('h3',undefined,'entry-title');
    title.append(
      el('span',`#${ed.report_id}`,'entry-id'),
      document.createTextNode(' '),
      el('span',`${ed.iso_year}-W${String(ed.iso_week).padStart(2,'0')} v${ed.version}`,'entry-name'),
    );
    const badge=el('span',ed.period_label||'周卷','badge badge--approved');
    head.append(title,badge);
    card.append(head);
    card.append(el('p',ed.summary||'（无摘要）','entry-meaning'));
    const meta=el('p',`${shortDate(ed.created_at)} · ${ed.event_count} 条事件 · ${ed.msg_count} 条消息 · ${ed.char_count} 字`,'muted gc-meta-line tabular-nums');
    card.append(meta);
    const details=el('details',undefined,'gc-edition-details');
    details.append(el('summary','展开卷册正文'));
    const pre=el('pre',ed.chronicle_text||'（空）','gc-edition-text');
    details.append(pre);
    card.append(details);
    list.append(card);
  }
}

async function runEventAction(payload,successText){
  if(busy||!$('scope').value)return;
  busy=true;
  try{
    const res=await request('event_action',payload);
    notice(successText||`操作完成（成功 ${res?.succeeded?.length??1} 项）。`);
    await loadEvents();
  }finally{busy=false;}
}

async function loadEvents(){
  if(!$('scope').value)return;
  const ticket=++epoch;
  selectedEventIds.clear();
  const st=$('events-status')?.value??'pending';
  const d=await request('events_list',{status:st||null});
  if(ticket!==epoch)return;
  const counts=d.counts||{};
  const cntEl=$('events-counts');
  if(cntEl){
    cntEl.textContent=`待审 ${counts.pending||0} · 已过 ${counts.approved||0} · 更正 ${counts.corrections||0} · 隐藏成员 ${counts.hidden_members||0}`;
  }

  const list=$('events-list');
  list.replaceChildren();
  const events=Array.isArray(d.events)?d.events:[];
  if(!events.length){
    list.append(makeEmptyCard('当前筛选下暂无大事记录','切换状态筛选或等待群史后台抽取后再查看。'));
  }else{
    for(const ev of events){
      const card=el('article',undefined,'card entry gc-dict-card');
      card.dataset.status=ev.status||'pending';
      const head=el('div',undefined,'entry-head');
      const titleWrap=el('div',undefined,'gc-entry-title-group');
      if(!ev.hidden){
        const wrap=el('label',undefined,'gc-select-wrap');
        const cb=document.createElement('input');
        cb.type='checkbox';
        cb.className='gc-event-cb';
        cb.dataset.eventId=String(ev.id);
        cb.setAttribute('aria-label',`选择事件 #${ev.id}`);
        cb.onchange=()=>{
          if(cb.checked)selectedEventIds.add(ev.id);else selectedEventIds.delete(ev.id);
          syncEventBatchCount();
        };
        wrap.append(cb);
        titleWrap.append(wrap);
      }
      const title=el('h3',undefined,'entry-title');
      title.append(el('span',`#${ev.id}`,'entry-id'),document.createTextNode(' '),el('span',ev.title,'entry-name'));
      titleWrap.append(title);
      const badgeLabel=ev.hidden?'已隐藏':(eventStates[ev.status]||ev.status);
      const badgeCls=ev.hidden?'badge badge--hidden':statusClass(ev.status);
      head.append(titleWrap,el('span',badgeLabel,badgeCls));
      card.append(head);

      card.append(el('p',ev.detail||'','entry-meaning'));
      const parts=(ev.participants||[]).map(p=>`${p.name}(${p.role===3||p.role==='core'?'核心':(p.role===2?'主要':'参与')})`).join('、');
      card.append(el('p',`${shortDate(ev.event_at)} · 重要度 ${ev.importance} · ${ev.kind||'事件'}${parts?` · ${parts}`:''}`,'muted gc-meta-line'));
      if(ev.review_note)card.append(el('p',`审核记录：${ev.review_note}`,'muted gc-meta-line'));

      const foot=el('div',undefined,'entry-foot');
      if(ev.hidden&&ev.can_restore){
        const btn=el('button','撤销隐藏','entry-open-btn');
        btn.type='button';
        btn.onclick=run(()=>runEventAction({action:'restore_event',event_id:ev.id,note:'管理员面板撤销事件隐藏'},`已撤销事件 #${ev.id} 的隐私隐藏。`));
        foot.append(el('span','隐私隐藏保护中','entry-usage muted'),btn);
      }else if(!ev.hidden){
        const mergeDetails=el('details',undefined,'gc-inline-merge');
        mergeDetails.append(el('summary','合并到其他事件'));
        const row=el('div',undefined,'gc-inline-merge-row');
        const targetInput=document.createElement('input');
        targetInput.type='number';
        targetInput.min='1';
        targetInput.placeholder='保留的目标事件 ID';
        const mergeBtn=el('button','确认合并');
        mergeBtn.type='button';
        mergeBtn.onclick=run(async()=>{
          const tid=Number(targetInput.value);
          if(!Number.isInteger(tid)||tid<=0||tid===ev.id)throw Error('请填写另一个有效的目标事件 ID。');
          await runEventAction({action:'merge',event_id:ev.id,target_event_id:tid,note:`管理员面板合并至 #${tid}`},`已将事件 #${ev.id} 合并至 #${tid}。`);
        });
        row.append(targetInput,mergeBtn);
        mergeDetails.append(row);
        foot.append(mergeDetails);
      }
      if(foot.childElementCount)card.append(foot);
      list.append(card);
    }
  }
  syncEventBatchCount();

  const corrBox=$('events-corrections');
  corrBox.replaceChildren();
  const corrs=Array.isArray(d.corrections)?d.corrections:[];
  if(!corrs.length){
    corrBox.append(el('p','当前没有待处理的更正请求。','muted'));
  }else{
    for(const c of corrs){
      const row=el('div',undefined,'gc-subitem');
      const info=el('div',undefined,'gc-subitem-main');
      info.append(
        el('p',`#${c.id} · 事件 #${c.event_id} · 提交人 ${c.reporter}`),
        el('p',c.content,'muted'),
      );
      const btn=el('button','标记已处理','entry-open-btn');
      btn.type='button';
      btn.onclick=run(()=>runEventAction({action:'resolve_correction',correction_id:c.id,note:'管理员面板已核对处理'},`更正请求 #${c.id} 已标记为已处理。`));
      row.append(info,btn);
      corrBox.append(row);
    }
  }

  const hidBox=$('events-hidden-panel');
  hidBox.replaceChildren();
  const hiddenMembers=Array.isArray(d.hidden_members)?d.hidden_members:[];
  const hiddenEventIds=Array.isArray(d.hidden_event_ids)?d.hidden_event_ids:[];
  if(!hiddenMembers.length&&!hiddenEventIds.length){
    hidBox.append(el('p','当前没有已隐藏的成员档案或事件。','muted'));
  }else{
    for(const m of hiddenMembers){
      const row=el('div',undefined,'gc-subitem');
      row.append(el('span',`成员档案：${m.name}（${m.uid}）`,'gc-subitem-main'));
      const btn=el('button','恢复公开','entry-open-btn');
      btn.type='button';
      btn.onclick=run(()=>runEventAction({action:'restore_member',uid:m.uid,note:'管理员面板恢复成员公开档案'},`已恢复成员 ${m.name} 的公开档案。`));
      row.append(btn);
      hidBox.append(row);
    }
    for(const eid of hiddenEventIds){
      const row=el('div',undefined,'gc-subitem');
      row.append(el('span',`隐藏事件：#${eid}`,'gc-subitem-main'));
      const btn=el('button','撤销隐藏','entry-open-btn');
      btn.type='button';
      btn.onclick=run(()=>runEventAction({action:'restore_event',event_id:eid,note:'管理员面板撤销事件隐藏'},`已撤销事件 #${eid} 的隐藏状态。`));
      row.append(btn);
      hidBox.append(row);
    }
  }
}

function renderMembersWall(){
  const list=$('members-list');
  if(!list)return;
  const kw=($('members-filter')?.value||'').trim().toLowerCase();
  const filtered=cachedMembers.filter(m=>{
    if(!kw)return true;
    const hay=[m.name,m.uid,m.grade,...(m.aliases||[])].join(' ').toLowerCase();
    return hay.includes(kw);
  });
  const sumEl=$('members-summary');
  if(sumEl){
    const liezhuan=cachedMembers.filter(m=>m.grade==='列传').length;
    const xiaozhuan=cachedMembers.filter(m=>m.grade==='小传').length;
    const hiddenHint=cachedMembersHidden?` · 已隐私隐藏 ${cachedMembersHidden} 人`:'';
    sumEl.textContent=`共 ${cachedMembers.length} 人（列传 ${liezhuan} · 小传 ${xiaozhuan}）${hiddenHint}`;
  }
  list.replaceChildren();
  if(!filtered.length){
    list.append(makeEmptyCard('暂无匹配的人物档案','完成长期群史建档或清空筛选条件后，人物列传墙会呈现在这里。'));
    return;
  }
  for(const m of filtered){
    const card=el('article',undefined,'card entry gc-dict-card');
    const head=el('div',undefined,'entry-head');
    const title=el('h3',undefined,'entry-title');
    title.append(el('span',m.name,'entry-name'),el('span',m.uid,'entry-id'));
    const gradeCls=m.grade==='列传'?'badge badge--approved':(m.grade==='小传'?'badge badge--pending':'badge badge--hidden');
    head.append(title,el('span',m.grade||'小记',gradeCls));
    card.append(head);

    card.append(el('p',`积分 ${m.points} · 事件 ${m.events} 次（核心大事 ${m.major_core}） · 跨度 ${Math.round(m.span_days||0)} 天`,'muted gc-meta-line tabular-nums'));
    if(Array.isArray(m.aliases)&&m.aliases.length){
      const aliases=el('div',undefined,'entry-aliases');
      for(const a of m.aliases)aliases.append(el('span',a,'alias-chip'));
      card.append(aliases);
    }
    card.append(el('p',`最早现身：${shortDate(m.first_seen)} · 最近留存：${shortDate(m.last_seen)}`,'muted gc-meta-line'));
    if(m.biography_excerpt){
      const details=el('details',undefined,'gc-edition-details');
      details.append(el('summary','查看已缓存人物传摘录'));
      details.append(el('p',m.biography_excerpt,'gc-bio-excerpt'));
      card.append(details);
    }
    list.append(card);
  }
}

async function loadMembers(){
  if(!$('scope').value)return;
  const ticket=++epoch;
  const d=await request('members_wall');
  if(ticket!==epoch)return;
  cachedMembers=Array.isArray(d.members)?d.members:[];
  cachedMembersHidden=Number(d.hidden_count||0);
  renderMembersWall();
}

function renderQuotaGrid(quota){
  if(!quota||typeof quota!=='object')return null;
  const grid=el('div',undefined,'gc-quota-grid');
  const cards=[
    {
      label:'今日总抽取额度',
      value:`已用 ${quota.total_used} / ${quota.daily_limit}`,
      sub:`剩余 ${quota.total_remaining} 次（${quota.day||'今日'}）`,
    },
    {
      label:'群史抽取',
      value:`已用 ${quota.history_used} 次`,
      sub:`今日可用剩余 ${quota.history_remaining}（保底预留 ${quota.history_reserved}）`,
    },
    {
      label:'文化整理',
      value:`已用 ${quota.culture_used} / ${quota.culture_limit}`,
      sub:`今日文化剩余 ${quota.culture_remaining} 次`,
    },
  ];
  for(const c of cards){
    const card=el('div',undefined,'gc-quota-card');
    card.append(
      el('span',c.label,'gc-quota-label'),
      el('strong',c.value,'gc-quota-value tabular-nums'),
      el('span',c.sub,'gc-quota-sub muted tabular-nums'),
    );
    grid.append(card);
  }
  return grid;
}

async function loadProgress(){
  if(!$('scope').value)return;
  const ticket=++epoch;
  const d=await request('progress');
  if(ticket!==epoch)return;
  const box=$('progress-content');
  box.replaceChildren();
  const statusPill=el('p',d.state.paused?'本群已暂停整理':'本群未暂停；按全局开关及允许范围运行',`gc-progress-status${d.state.paused?' is-paused':''}`);
  box.append(statusPill);
  const quotaGrid=renderQuotaGrid(d.quota);
  if(quotaGrid)box.append(quotaGrid);
  const j=d.job;
  if(!j){
    box.append(el('p','尚未开始。请确认全局开关、长期群史范围和记忆来源。','muted'));
  }else{
    const list=el('dl');
    const values=[
      ['已处理消息片段',j.processed],
      ['覆盖开始',date(j.coverage_min)],
      ['覆盖结束',date(j.coverage_max)],
      ['历史回溯',`${j.back_cursor} / ${j.frozen_high}（源消息 ID）`],
      ['新消息游标',j.live_cursor],
      ['更新时间',date(j.updated)],
    ];
    for(const [k,v] of values)list.append(el('dt',k),el('dd',v));
    box.append(list);
    for(const gap of JSON.parse(j.gaps||'[]'))box.append(el('p',gap,'muted'));
  }
  if(d.error||j?.error)box.append(el('p',d.error||j.error,'danger'));
  $('pause').disabled=Boolean(d.state.paused);
  $('resume').disabled=!d.state.paused;
  const runOnceBtn=$('run-once');
  if(runOnceBtn)runOnceBtn.disabled=!overviewEnabled||Boolean(d.state.paused);
}

function clearScopeContent(){
  selectedCultureIds.clear();
  selectedEventIds.clear();
  cachedMembers=[];
  cachedEditions=[];
  for(const id of ['entries','review-entries','count','review-count','page-number','progress-content','editions-list','editions-count','editions-comparison','events-list','events-counts','events-corrections','events-hidden-panel','members-list','members-summary']){
    const n=$(id);
    if(n)n.replaceChildren();
  }
  const cmp=$('editions-comparison');
  if(cmp)cmp.hidden=true;
  for(const id of ['prev','next','review-prev','review-next','pause','resume','run-once','compare-btn']){
    const n=$(id);
    if(n)n.disabled=true;
  }
  syncCultureBatchCount();
  syncEventBatchCount();
}

async function refresh(){
  ++epoch;
  closeDetail();
  if(!$('scope').value){clearScopeContent();return;}
  if(currentTab==='progress')await loadProgress();
  else if(currentTab==='editions')await loadEditions();
  else if(currentTab==='events')await loadEvents();
  else if(currentTab==='members')await loadMembers();
  else await loadList();
}

function addSection(box,title,text){
  const sec=el('div',undefined,'gc-detail-section');
  sec.append(el('h3',title),el('p',text));
  box.append(sec);
}

async function showDetail(id,triggerEl=null){
  const ticket=++detailEpoch,scope=$('scope').value;
  if(triggerEl)lastTrigger=triggerEl;
  const d=await request('detail',{id});
  if(ticket!==detailEpoch||scope!==$('scope').value)return;
  selected=d.entry;
  $('detail').hidden=false;
  const backdrop=$('detail-backdrop');
  if(backdrop)backdrop.hidden=false;
  document.body.classList.add('gc-drawer-open');

  $('detail-title').textContent=`#${selected.id} ${selected.name}`;
  const statusBadge=$('detail-status');
  if(statusBadge){
    if(selected.status){
      statusBadge.textContent=states[selected.status]||selected.status;
      statusBadge.className=statusClass(selected.status);
      statusBadge.hidden=false;
    }else{
      statusBadge.hidden=true;
    }
  }

  const box=$('detail-body');
  box.replaceChildren();
  const canRestore=Boolean(selected.unavailable&&selected.status==='hidden'&&selected.can_restore);
  const restorePanel=$('restore-panel');
  if(restorePanel)restorePanel.hidden=!canRestore;
  $('moderation').hidden=Boolean(selected.unavailable)||selected.status==='merged';
  if(selected.unavailable){
    box.append(el('p',canRestore?'该词条当前处于隐私隐藏状态；为保护隐私暂不展示原文。确认后可在下方撤销隐藏并恢复公开。':'资料已不可用，请刷新词库。'));
    const restoreNote=$('restore-note');
    if(restoreNote)restoreNote.value='';
    const scroll=$('detail-scroll');
    if(scroll)scroll.scrollTop=0;
    $('detail').focus({preventScroll:true});
    return;
  }
  if(selected.merged_into)addSection(box,'合并记录',`已合并至 #${selected.merged_into}，请按目标 ID 查看当前解释。`);
  if(selected.review_note)addSection(box,'待审说明',selected.review_note);
  addSection(box,'当前含义',selected.meaning);
  addSection(box,'使用场景',selected.usage||'未记录');
  addSection(box,'别称',selected.aliases.map(a=>a.name).join('、')||'未记录');
  addSection(box,'起源',selected.origin?.text||'起源未知；最早留存记录不等于真正起源。');
  if(selected.proposed_origin&&!selected.origin)addSection(box,'待审核的起源说法',selected.proposed_origin.text);
  box.append(el('p',`最早留存记录：${date(selected.first_seen)}`,'muted'));

  box.append(el('h3','演变与代表事件'));
  const timeline=el('ol',undefined,'timeline');
  for(const n of selected.timeline){
    const li=el('li');
    li.append(el('p',n.text),el('p',`记载：${date(n.recorded_at)} · 发生：${date(n.event_at)} · 来源 ${n.source_ids.join('、')}`,'muted'));
    timeline.append(li);
  }
  box.append(timeline.childElementCount?timeline:el('p','尚无可核对的演变节点。','muted'));

  box.append(el('h3','原文证据'));
  for(const s of selected.sources){
    const b=el('blockquote',undefined,'evidence');
    b.append(el('p',s.quote),el('p',`${s.name} · ${date(s.ts)} · 来源 #${s.id}`,'muted'));
    box.append(b);
  }

  const history=el('details');
  history.append(el('summary',`修订历史（${d.revisions.length}）`));
  for(const r of d.revisions){
    const section=el('div',undefined,'gc-rev-item');
    section.append(
      el('p',`${actions[r.action]||r.action} · ${date(r.created)} · ${r.actor}`),
      el('p',r.note),
      el('p',r.before.unavailable?'旧版本依据已失效，内容不再展示。':`此前含义：${r.before.meaning}`,'muted'),
    );
    history.append(section);
  }
  box.append(history);

  $('edit-meaning').value=selected.meaning;
  $('edit-usage').value=selected.usage;
  $('edit-origin').value=selected.proposed_origin?.text||'';
  $('action-note').value='';
  $('merge-id').value='';
  const scroll=$('detail-scroll');
  if(scroll)scroll.scrollTop=0;
  $('detail').focus({preventScroll:true});
}

async function moderate(action,extra={}){
  if(busy||!selected)return;
  const note=$('action-note').value.trim();
  if(!note)throw Error('请填写处理理由。');
  busy=true;
  const item=selected,scope=$('scope').value,tab=currentTab;
  let ticket=epoch,detailTicket=detailEpoch;
  const current=()=>scope===$('scope').value&&tab===currentTab&&ticket===epoch&&detailTicket===detailEpoch;
  try{
    await request('action',{id:item.id,revision:item.revision,action,note,...extra});
    if(!current())return;
    const loading=refresh();
    ticket=epoch;
    detailTicket=detailEpoch;
    await loading;
    if(!current())return;
    if(action!=='hide'&&action!=='merge'){
      const detail=showDetail(item.id);
      detailTicket=detailEpoch;
      await detail;
    }
    if(current())notice(`${actions[action]}已保存。`);
  }catch(e){if(current())throw e;}
  finally{busy=false;}
}

function run(fn){return ()=>Promise.resolve().then(fn).catch(e=>notice(e.message,true));}

function switchTab(tabName){
  const target=ALL_TABS.includes(tabName)?tabName:'library';
  currentTab=target;
  for(const tab of ALL_TABS){
    const node=$(tab);
    if(node)node.hidden=tab!==currentTab;
  }
  document.querySelectorAll('[data-tab]').forEach(n=>n.setAttribute('aria-current',n.dataset.tab===currentTab?'page':'false'));
}

document.querySelectorAll('[data-tab]').forEach(b=>b.onclick=run(async()=>{
  switchTab(b.dataset.tab);
  await refresh();
}));

// Quick reason chips fill their bound note input
document.querySelectorAll('.gc-reason-chips .gc-reason-chip').forEach(chip=>{
  chip.onclick=()=>{
    const targetId=chip.closest('.gc-reason-chips')?.dataset.reasonTarget;
    const input=targetId?$(targetId):null;
    if(input){
      input.value=chip.dataset.reason||chip.textContent.trim();
      input.focus();
    }
  };
});

// Culture review batch select & actions
const cultureSelectAll=$('culture-select-all');
if(cultureSelectAll){
  cultureSelectAll.onchange=()=>{
    const checked=cultureSelectAll.checked;
    document.querySelectorAll('#review-entries .gc-select-cb').forEach(cb=>{
      cb.checked=checked;
      const eid=Number(cb.dataset.entryId);
      if(checked)selectedCultureIds.add(eid);else selectedCultureIds.delete(eid);
    });
    syncCultureBatchCount();
  };
}

document.querySelectorAll('[data-culture-batch]').forEach(btn=>{
  btn.onclick=run(async()=>{
    if(busy||!$('scope').value)return;
    const ids=[...selectedCultureIds];
    if(!ids.length)throw Error('请先勾选至少一条待审词条。');
    const note=($('batch-note')?.value||'').trim();
    if(!note)throw Error('请填写或点选批量处理理由。');
    const act=btn.dataset.cultureBatch;
    busy=true;
    try{
      const res=await request('batch_action',{action:act,ids,note});
      const okCount=res?.succeeded?.length||0;
      const failCount=res?.failed?.length||0;
      notice(`批量${actions[act]||act}完成：成功 ${okCount} 条${failCount?`，失败 ${failCount} 条`:''}。`,failCount>0&&okCount===0);
      await loadList();
    }finally{busy=false;}
  });
});

// Editions compare controls
const cmpBtnEl=$('compare-btn');
if(cmpBtnEl){
  cmpBtnEl.onclick=run(async()=>{
    const leftId=Number($('compare-left')?.value);
    const rightId=Number($('compare-right')?.value);
    if(!Number.isInteger(leftId)||!Number.isInteger(rightId)||leftId<=0||rightId<=0)throw Error('请选择两卷用于对比。');
    await loadEditions({reportId:leftId,compareId:rightId});
  });
}
const cmpClearEl=$('compare-clear');
if(cmpClearEl){
  cmpClearEl.onclick=()=>renderEditionComparison(null);
}

// Events filter & batch controls
const eventsStatusEl=$('events-status');
if(eventsStatusEl)eventsStatusEl.onchange=run(loadEvents);

const eventsSelectAll=$('events-select-all');
if(eventsSelectAll){
  eventsSelectAll.onchange=()=>{
    const checked=eventsSelectAll.checked;
    document.querySelectorAll('#events-list .gc-event-cb').forEach(cb=>{
      cb.checked=checked;
      const eid=Number(cb.dataset.eventId);
      if(checked)selectedEventIds.add(eid);else selectedEventIds.delete(eid);
    });
    syncEventBatchCount();
  };
}

document.querySelectorAll('[data-event-batch]').forEach(btn=>{
  btn.onclick=run(async()=>{
    const ids=[...selectedEventIds];
    if(!ids.length)throw Error('请先勾选至少一条大事记录。');
    const note=($('events-batch-note')?.value||'').trim()||`管理员面板批量${btn.textContent.trim()}`;
    const act=btn.dataset.eventBatch;
    await runEventAction({action:act,ids,note},`事件批量${actions[act]||act}已完成（共处理 ${ids.length} 条）。`);
  });
});

// Members wall client-side search filter
const membersFilterEl=$('members-filter');
if(membersFilterEl)membersFilterEl.oninput=()=>renderMembersWall();

$('search').onclick=run(async()=>{page=1;await refresh();});
$('query').onkeydown=e=>{if(e.key==='Enter')$('search').click();};
$('status').onchange=$('search').onclick;
$('review-status').onchange=run(async()=>{reviewPage=1;await refresh();});
$('refresh').onclick=run(refresh);
$('close-detail').onclick=()=>closeDetail({restoreFocus:true});
const backdropEl=$('detail-backdrop');
if(backdropEl)backdropEl.onclick=()=>closeDetail({restoreFocus:true});

document.addEventListener('keydown',e=>{
  if(e.key==='Escape'&&!$('detail').hidden){
    e.preventDefault();
    closeDetail({restoreFocus:true});
  }
});

// Close open info popovers when clicking outside
document.addEventListener('click',e=>{
  document.querySelectorAll('details.gc-info-popover[open]').forEach(pop=>{
    if(!pop.contains(e.target))pop.open=false;
  });
});

const allowedScopes=new Set();
function applyScope(value){
  const scope=allowedScopes.has(value)?value:'';
  $('scope').value=scope;
  page=reviewPage=1;
  clearScopeContent();
}

$('scope').onchange=run(async()=>{
  applyScope($('scope').value);
  setSharedScope($('scope').value,{emit:false});
  await refresh();
});

window.addEventListener('dynamics-scopechange',run(async()=>{
  applyScope(getSharedScope());
  await refresh();
}));

window.addEventListener('dynamics-cmdk-deeplink',run(async ev=>{
  const item=ev?.detail;
  const link=item&&typeof item.deep_link==='object'?item.deep_link:null;
  if(!link)return;
  if(link.scope&&allowedScopes.has(link.scope)&&link.scope!==$('scope').value){
    applyScope(link.scope);
    setSharedScope(link.scope,{emit:false});
  }
  if(link.tab)switchTab(link.tab);
  await refresh();
  const entryId=Number(link.entry_id);
  if(Number.isInteger(entryId)&&entryId>0&&$('scope').value){
    await showDetail(entryId).catch(()=>{/* entry may no longer exist */});
  }
}));

window.DynamicsBeforeNavigate=()=>{
  if(!selected||$('moderation').hidden)return true;
  const dirty=$('edit-meaning').value!==selected.meaning
    ||$('edit-usage').value!==selected.usage
    ||$('edit-origin').value!==(selected.proposed_origin?.text||'')
    ||$('action-note').value.trim()
    ||$('merge-id').value.trim();
  return !dirty||confirmAction(msg('此页有编辑操作。请确认已保存，再打开其他面板。'));
};

for(const [id,delta,review] of [['prev',-1,false],['next',1,false],['review-prev',-1,true],['review-next',1,true]]){
  $(id).onclick=run(async()=>{
    if(review)reviewPage+=delta;else page+=delta;
    await refresh();
  });
}

for(const action of ['pause','resume']){
  $(action).onclick=run(async()=>{
    const scope=$('scope').value,ticket=epoch;
    const current=()=>scope===$('scope').value&&ticket===epoch&&currentTab==='progress';
    try{await request('action',{action,note:action==='pause'?'管理员暂停整理':'管理员恢复整理'});}
    catch(e){if(current())throw e;return;}
    if(!current())return;
    notice(action==='pause'?'本群整理已暂停。':'已恢复本群整理，将按现有调度继续。');
    await loadProgress();
  });
}

const runOnceBtnEl=$('run-once');
if(runOnceBtnEl){
  runOnceBtnEl.onclick=run(async()=>{
    if(busy||!$('scope').value)return;
    const scope=$('scope').value,ticket=epoch;
    const current=()=>scope===$('scope').value&&ticket===epoch&&currentTab==='progress';
    busy=true;
    runOnceBtnEl.disabled=true;
    notice('正在手动整理/重试 1 批，请稍候…');
    try{
      const res=await request('run_once',{batches:1,verification_extra_quota:0});
      if(!current())return;
      const steps=Array.isArray(res?.steps)?res.steps:[];
      const advanced=steps.some(s=>s&&s.advanced);
      const stepErr=steps.find(s=>s&&s.error)?.error||'';
      if(advanced){
        notice(`已完成 1 批整理（今日总额度 ${res.used_before} → ${res.used_after}）。`);
      }else if(stepErr){
        notice(`本批整理未推进：${stepErr}`,true);
      }else{
        notice(`暂无可推进的新批次或今日额度已满（当前已用 ${res.used_after}/${res.restored_limit}）。`);
      }
      await loadProgress();
    }catch(e){if(current())throw e;}
    finally{
      busy=false;
      if(current())runOnceBtnEl.disabled=!overviewEnabled||$('pause').disabled;
    }
  });
}

const restoreBtnEl=$('restore-entry');
if(restoreBtnEl){
  restoreBtnEl.onclick=run(async()=>{
    if(busy||!selected)return;
    const item=selected,scope=$('scope').value,tab=currentTab;
    const note=($('restore-note')?.value||'').trim()||'管理员撤销隐藏恢复公开';
    let ticket=epoch,detailTicket=detailEpoch;
    const current=()=>scope===$('scope').value&&tab===currentTab&&ticket===epoch&&detailTicket===detailEpoch;
    busy=true;
    try{
      await request('action',{id:item.id,revision:item.revision,action:'restore',note});
      if(!current())return;
      const loading=refresh();
      ticket=epoch;
      detailTicket=detailEpoch;
      await loading;
      if(!current())return;
      const detail=showDetail(item.id);
      detailTicket=detailEpoch;
      await detail;
      if(current())notice('撤销隐藏已保存，词条已恢复。');
    }catch(e){if(current())throw e;}
    finally{busy=false;}
  });
}

document.querySelectorAll('[data-action]').forEach(b=>b.onclick=run(async()=>{
  const action=b.dataset.action;
  const patch={meaning:$('edit-meaning').value,usage:$('edit-usage').value,origin_text:$('edit-origin').value};
  await moderate(action,action==='edit'?{patch}:{});
}));

$('merge').onclick=run(async()=>{
  const target=Number($('merge-id').value),item=selected,scope=$('scope').value;
  if(!item||!Number.isInteger(target)||target<1||target===item.id)throw Error('请填写另一个本群词条 ID。');
  const d=await request('detail',{id:target});
  if(item!==selected||scope!==$('scope').value)return;
  if(d.entry.unavailable)throw Error('目标词条证据不可用。');
  mergeTarget={target_id:target,target_revision:d.entry.revision,source:item};
  $('merge-preview-text').textContent=`将“${item.name}”（#${item.id}）合并至“${d.entry.name}”（#${target}），目标将重新待审。请核对二者含义与来源。`;
  $('merge-preview').hidden=false;
});

$('cancel-merge').onclick=()=>{mergeTarget=null;$('merge-preview').hidden=true;};

$('confirm-merge').onclick=run(async()=>{
  if(!mergeTarget||mergeTarget.source!==selected)throw Error('词条已变化，请重新选择合并目标。');
  await moderate('merge',{target_id:mergeTarget.target_id,target_revision:mergeTarget.target_revision});
});

try{
  const bridge=(window.DynamicsPanelBridge || window.AstrBotPluginPage);
  if(bridge?.ready)await bridge.ready();
  const overview=await api('culture_overview');
  overviewEnabled=Boolean(overview.enabled);
  const quotaHint=overview.quota
    ?`今日已用 ${overview.quota.total_used}/${overview.quota.daily_limit}（剩余 ${overview.quota.total_remaining}）`
    :`共用每日 ${overview.daily_limit} 次抽取额度`;
  const connText=`${overview.enabled?'文化整理已启用':'文化整理未启用'} · 记忆${overview.memory} · ${overview.learning?'学习线索已接入':'学习线索可选，未接入'} · ${quotaHint}`;
  $('connection').textContent=connText;
  $('connection').title=connText;
  const placeholder=el('option');
  placeholder.value='';
  setText(placeholder,msg('请选择群聊'));
  $('scope').replaceChildren(placeholder,...overview.groups.map(g=>{
    allowedScopes.add(g.scope);
    const option=el('option',g.name+' · '+g.scope);
    option.value=g.scope;
    return option;
  }));
  if(!overview.groups.length)notice('尚无允许群。先配置长期群史范围，并等待群聊或首次发现。');
  const ctx=bridge?.getContext?.()||{};
  const params=new URLSearchParams(location.search);
  const initial=ctx.scope;
  const supplied=params.has('umo')||typeof initial==='string';
  applyScope(supplied?getSharedScope():overview.groups[0]?.scope);
  const requestedTab=params.get('tab')||ctx.tab;
  if(requestedTab)switchTab(requestedTab);
  await refresh();
  const requestedEntry=Number(params.get('entry_id')||ctx.entry_id);
  if(Number.isInteger(requestedEntry)&&requestedEntry>0&&$('scope').value){
    await showDetail(requestedEntry).catch(()=>{/* deep-link entry may not exist */});
  }
}catch(e){notice(e.message,true);}
