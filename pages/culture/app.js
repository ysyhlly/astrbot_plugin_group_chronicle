import {confirmAction,getSharedScope,msg,setSharedScope,setText} from './series-ui.js';

const $=id=>document.getElementById(id);
const states={approved:'已公开',pending:'待审核',disputed:'待纠正',rejected:'已驳回',hidden:'已隐藏',merged:'已合并'};
const actions={approve:'审核通过',edit:'修订',reject:'驳回',hide:'隐藏',restore:'撤销隐藏',correct:'提出纠正',merge:'合并',evidence:'追加证据'};
let epoch=0,detailEpoch=0,page=1,reviewPage=1,selected=null,mergeTarget=null,busy=false,currentTab='library',lastTrigger=null,overviewEnabled=false;

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

function entryCard(e){
  const box=el('article',undefined,'card entry gc-dict-card');
  box.dataset.status=e.status||'hidden';

  const head=el('div',undefined,'entry-head');
  const title=el('h3',undefined,'entry-title');
  title.append(el('span',`#${e.id}`,'entry-id'),document.createTextNode(' '),el('span',e.name,'entry-name'));
  const badge=el('span',states[e.status]||e.status,statusClass(e.status));
  head.append(title,badge);
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

function renderCards(container,items){
  container.replaceChildren(...items.map(entryCard));
  if(!items.length){
    container.append(makeEmptyCard('暂无符合条件的词条','完成群文化整理或调整筛选条件后，词条会以词典卡片形式呈现在这里。'));
  }
}

async function loadList(){
  if(!$('scope').value)return;
  const ticket=++epoch;
  const review=currentTab==='review';
  const d=await request('list',{
    query:review?'':$('query').value.trim(),
    status:review?$('review-status').value:$('status').value,
    page:review?reviewPage:page,
  });
  if(ticket!==epoch)return;
  renderCards($(review?'review-entries':'entries'),d.entries);
  const p=review?reviewPage:page;
  $(review?'review-count':'count').textContent=`共 ${d.total} 条 · 第 ${p} 页`;
  $(review?'review-prev':'prev').disabled=p<=1;
  $(review?'review-next':'next').disabled=p*20>=d.total;
  if(!review)$('page-number').textContent=String(page);
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
  for(const id of ['entries','review-entries','count','review-count','page-number','progress-content'])$(id).replaceChildren();
  for(const id of ['prev','next','review-prev','review-next','pause','resume','run-once']){
    const n=$(id);
    if(n)n.disabled=true;
  }
}

async function refresh(){
  ++epoch;
  closeDetail();
  if(!$('scope').value){clearScopeContent();return;}
  if(currentTab==='progress')await loadProgress();else await loadList();
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
  const target=['library','review','progress'].includes(tabName)?tabName:'library';
  currentTab=target;
  for(const tab of ['library','review','progress'])$(tab).hidden=tab!==currentTab;
  document.querySelectorAll('[data-tab]').forEach(n=>n.setAttribute('aria-current',n.dataset.tab===currentTab?'page':'false'));
}

document.querySelectorAll('[data-tab]').forEach(b=>b.onclick=run(async()=>{
  switchTab(b.dataset.tab);
  await refresh();
}));

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
