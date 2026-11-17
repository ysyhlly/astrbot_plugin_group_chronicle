import {confirmAction,getSharedScope,msg,setSharedScope,setText} from './series-ui.js';

const $=id=>document.getElementById(id);
const states={approved:'已公开',pending:'待审核',disputed:'待纠正',rejected:'已驳回',hidden:'已隐藏',merged:'已合并'};
const actions={approve:'审核通过',edit:'修订',reject:'驳回',hide:'隐藏',correct:'提出纠正',merge:'合并',evidence:'追加证据'};
let epoch=0,detailEpoch=0,page=1,reviewPage=1,selected=null,mergeTarget=null,busy=false,currentTab='library';
const el=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=String(text);if(cls)n.className=cls;return n;};
const date=t=>t?new Date(t*1000).toLocaleString('zh-CN'):'未知';
function notice(text,error=false){$('notice').textContent=text;$('notice').dataset.error=String(error);}
async function api(path,body){const bridge=window.AstrBotPluginPage;if(!bridge)throw Error('请从 AstrBot 插件详情打开此页面。');const r=await (body?bridge.apiPost(path,body):bridge.apiGet(path));if(r.status==='error'||r.ok===false)throw Error(r.message||'请求失败');return r.data??r;}
const request=(op,fields={})=>api('culture',{op,scope:$('scope').value,...fields});
function closeDetail(){selected=null;mergeTarget=null;$('merge-preview').hidden=true;++detailEpoch;$('detail').hidden=true;}
function entryCard(e){const box=el('article',undefined,'card entry');box.append(el('span',states[e.status]||e.status,'badge'),el('h3',`#${e.id} ${e.name}`));box.append(el('p',e.unavailable?'来源不可用或条目已隐藏，解释暂停展示。':e.meaning));if(!e.unavailable){box.append(el('p',`最早留存：${date(e.first_seen)}`,'muted'));const b=el('button','查看沿革与证据');b.onclick=()=>showDetail(e.id).catch(err=>notice(err.message,true));box.append(b);}return box;}
function renderCards(container,items){container.replaceChildren(...items.map(entryCard));if(!items.length)container.append(el('div','暂无符合条件的词条。完成整理后会出现在这里。','card empty'));}
async function loadList(){if(!$('scope').value)return;const ticket=++epoch;const review=currentTab==='review';const d=await request('list',{query:review?'':$('query').value.trim(),status:review?$('review-status').value:$('status').value,page:review?reviewPage:page});if(ticket!==epoch)return;renderCards($(review?'review-entries':'entries'),d.entries);const p=review?reviewPage:page;$(review?'review-count':'count').textContent=`共 ${d.total} 条 · 第 ${p} 页`;$(review?'review-prev':'prev').disabled=p<=1;$(review?'review-next':'next').disabled=p*20>=d.total;if(!review)$('page-number').textContent=String(page);}
async function loadProgress(){if(!$('scope').value)return;const ticket=++epoch;const d=await request('progress');if(ticket!==epoch)return;const box=$('progress-content');box.replaceChildren();box.append(el('p',d.state.paused?'本群已暂停整理':'本群未暂停；按全局开关及允许范围运行'));const j=d.job;if(!j){box.append(el('p','尚未开始。请确认全局开关、长期群史范围和记忆来源。','muted'));}else{const list=el('dl');const values=[['已处理消息片段',j.processed],['覆盖开始',date(j.coverage_min)],['覆盖结束',date(j.coverage_max)],['历史回溯',`${j.back_cursor} / ${j.frozen_high}（源消息 ID）`],['新消息游标',j.live_cursor],['更新时间',date(j.updated)]];for(const [k,v] of values)list.append(el('dt',k),el('dd',v));box.append(list);for(const gap of JSON.parse(j.gaps||'[]'))box.append(el('p',gap,'muted'));}if(d.error||j?.error)box.append(el('p',d.error||j.error,'danger'));$('pause').disabled=Boolean(d.state.paused);$('resume').disabled=!d.state.paused;}
function clearScopeContent(){
 for(const id of ['entries','review-entries','count','review-count','page-number','progress-content'])$(id).replaceChildren();
 for(const id of ['prev','next','review-prev','review-next','pause','resume'])$(id).disabled=true;
}
async function refresh(){
 ++epoch;
 closeDetail();
 if(!$('scope').value){clearScopeContent();return;}
 if(currentTab==='progress')await loadProgress();else await loadList();
}
function addSection(box,title,text){box.append(el('h3',title),el('p',text));}
async function showDetail(id){const ticket=++detailEpoch,scope=$('scope').value;const d=await request('detail',{id});if(ticket!==detailEpoch||scope!==$('scope').value)return;selected=d.entry;$('detail').hidden=false;$('detail-title').textContent=`#${selected.id} ${selected.name}`;const box=$('detail-body');box.replaceChildren();$('moderation').hidden=Boolean(selected.unavailable)||selected.status==='merged';if(selected.unavailable){box.append(el('p','资料已不可用，请刷新词库。'));return;}if(selected.merged_into)addSection(box,'合并记录',`已合并至 #${selected.merged_into}，请按目标 ID 查看当前解释。`);if(selected.review_note)addSection(box,'待审说明',selected.review_note);addSection(box,'当前含义',selected.meaning);addSection(box,'使用场景',selected.usage||'未记录');addSection(box,'别称',selected.aliases.map(a=>a.name).join('、')||'未记录');addSection(box,'起源',selected.origin?.text||'起源未知；最早留存记录不等于真正起源。');if(selected.proposed_origin&&!selected.origin)addSection(box,'待审核的起源说法',selected.proposed_origin.text);box.append(el('p',`最早留存记录：${date(selected.first_seen)}`,'muted'));box.append(el('h3','演变与代表事件'));const timeline=el('ol',undefined,'timeline');for(const n of selected.timeline){const li=el('li');li.append(el('p',n.text),el('p',`记载：${date(n.recorded_at)} · 发生：${date(n.event_at)} · 来源 ${n.source_ids.join('、')}`,'muted'));timeline.append(li);}box.append(timeline.childElementCount?timeline:el('p','尚无可核对的演变节点。','muted'));box.append(el('h3','原文证据'));for(const s of selected.sources){const b=el('blockquote',undefined,'evidence');b.append(el('p',s.quote),el('p',`${s.name} · ${date(s.ts)} · 来源 #${s.id}`,'muted'));box.append(b);}const history=el('details');history.append(el('summary',`修订历史（${d.revisions.length}）`));for(const r of d.revisions){const section=el('div');section.append(el('p',`${actions[r.action]||r.action} · ${date(r.created)} · ${r.actor}`),el('p',r.note),el('p',r.before.unavailable?'旧版本依据已失效，内容不再展示。':`此前含义：${r.before.meaning}`,'muted'));history.append(section);}box.append(history);$('edit-meaning').value=selected.meaning;$('edit-usage').value=selected.usage;$('edit-origin').value=selected.proposed_origin?.text||'';$('action-note').value='';$('merge-id').value='';$('detail').scrollIntoView({behavior:'smooth',block:'start'});$('detail').focus({preventScroll:true});}
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
document.querySelectorAll('[data-tab]').forEach(b=>b.onclick=run(async()=>{currentTab=b.dataset.tab;for(const tab of ['library','review','progress'])$(tab).hidden=tab!==currentTab;document.querySelectorAll('[data-tab]').forEach(n=>n.setAttribute('aria-current',n===b?'page':'false'));await refresh();}));
$('search').onclick=run(async()=>{page=1;await refresh();});$('query').onkeydown=e=>{if(e.key==='Enter')$('search').click();};$('status').onchange=$('search').onclick;$('review-status').onchange=run(async()=>{reviewPage=1;await refresh();});$('refresh').onclick=run(refresh);$('close-detail').onclick=closeDetail;
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
window.DynamicsBeforeNavigate=()=>{
 if(!selected||$('moderation').hidden)return true;
 const dirty=$('edit-meaning').value!==selected.meaning
  ||$('edit-usage').value!==selected.usage
  ||$('edit-origin').value!==(selected.proposed_origin?.text||'')
  ||$('action-note').value.trim()
  ||$('merge-id').value.trim();
 return !dirty||confirmAction(msg('此页有编辑操作。请确认已保存，再打开其他面板。'));
};
for(const [id,delta,review] of [['prev',-1,false],['next',1,false],['review-prev',-1,true],['review-next',1,true]])$(id).onclick=run(async()=>{if(review)reviewPage+=delta;else page+=delta;await refresh();});
for(const action of ['pause','resume'])$(action).onclick=run(async()=>{
 const scope=$('scope').value,ticket=epoch;
 const current=()=>scope===$('scope').value&&ticket===epoch&&currentTab==='progress';
 try{await request('action',{action,note:action==='pause'?'管理员暂停整理':'管理员恢复整理'});}
 catch(e){if(current())throw e;return;}
 if(!current())return;
 notice(action==='pause'?'本群整理已暂停。':'已恢复本群整理，将按现有调度继续。');
 await loadProgress();
});
document.querySelectorAll('[data-action]').forEach(b=>b.onclick=run(async()=>{const action=b.dataset.action;const patch={meaning:$('edit-meaning').value,usage:$('edit-usage').value,origin_text:$('edit-origin').value};await moderate(action,action==='edit'?{patch}:{});}));
$('merge').onclick=run(async()=>{const target=Number($('merge-id').value),item=selected,scope=$('scope').value;if(!item||!Number.isInteger(target)||target<1||target===item.id)throw Error('请填写另一个本群词条 ID。');const d=await request('detail',{id:target});if(item!==selected||scope!==$('scope').value)return;if(d.entry.unavailable)throw Error('目标词条证据不可用。');mergeTarget={target_id:target,target_revision:d.entry.revision,source:item};$('merge-preview-text').textContent=`将“${item.name}”（#${item.id}）合并至“${d.entry.name}”（#${target}），目标将重新待审。请核对二者含义与来源。`;$('merge-preview').hidden=false;});
$('cancel-merge').onclick=()=>{mergeTarget=null;$('merge-preview').hidden=true;};
$('confirm-merge').onclick=run(async()=>{if(!mergeTarget||mergeTarget.source!==selected)throw Error('词条已变化，请重新选择合并目标。');await moderate('merge',{target_id:mergeTarget.target_id,target_revision:mergeTarget.target_revision});});
try{
 if(window.AstrBotPluginPage?.ready)await window.AstrBotPluginPage.ready();
 const overview=await api('culture_overview');
 $('connection').textContent=`${overview.enabled?'文化整理已启用':'文化整理未启用'} · 记忆${overview.memory} · ${overview.learning?'学习线索已接入':'学习线索可选，未接入'} · 共用每日 ${overview.daily_limit} 次抽取额度`;
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
 const initial=window.AstrBotPluginPage?.getContext?.()?.scope;
 const supplied=new URLSearchParams(location.search).has('umo')||typeof initial==='string';
 applyScope(supplied?getSharedScope():overview.groups[0]?.scope);
 await refresh();
}catch(e){notice(e.message,true);}
