import {store} from '../stores/research.js';
import {runs} from '../api/research.js';
import {esc,fmt} from '../utils/format.js';
import {table} from '../components/common/table.js';
import {status} from '../components/common/status.js';
import {runList} from '../components/common/run-list.js';
import {download} from '../utils/download.js';
const active=r=>['queued','running'].includes(r.status);
export async function render(id){
 if(!id)return `<h1>计算结果与解释</h1><p>选择一次运行查看冻结输入、算法结果与业务解释。</p>${runList()}`;
 const r=await runs.get(id);store.runId=r.id;
 const gate=r.gate||{view:r.status==='completed',approve:r.status==='completed',download:r.status==='completed',stage:r.status,reason:r.status_detail};
 const writable=localStorage.getItem('research-role')!=='viewer';
 const stage=`<section class="method-note stage ${esc(gate.stage)}"><strong>${esc(r.gate?.reason||r.status_detail||'')}</strong><p>${esc(stageNote(r, gate))}</p>${r.attempt>1?`<small>此研究已被接取 ${r.attempt} 次；迟到进程的结果不会覆盖已保存结果。</small>`:''}${r.integrity==='restored'?`<small>留档异常已由数据库原件恢复；已批准研究不重算。</small>`:''}</section>`;
 const actions=(gate.download?`<button id="export">下载核算 CSV</button>`:`<button disabled title="${esc(gate.reason||'')}">下载暂不可用</button>`)
  +(!viewer()&&['failed','queued'].includes(r.status)?`<button id="retry" class="secondary">沿用编号重试</button>`:'')
  +(gate.approve?`<a class="button secondary" href="#review/${r.id}">打开业务复核 →</a>`:`<button disabled title="${esc(gate.reason||'')}">复核暂不可用</button>`);
 const body=!gate.view?`<div class="error">${esc(gate.reason||'结果暂不可查看')}。已关闭查看、复核与下载；系统只恢复原件，不会重算已批准研究。</div>`:(r.result?`
  `<section class="result-intro"><strong>${esc(r.result.interpretation)}</strong><p>${esc(r.result.caveat)}</p></section><h2>核心数值</h2>${table(['业务指标','计算值'],Object.entries(r.result.metrics).map(([k,v])=>[k,fmt(v)]))}<h2>${esc(r.result.table_title||'计算明细')}</h2>${table(r.result.columns,r.result.rows)}${r.result.chart?`<h2>${esc(r.result.chart.title)}</h2><div class="chart">${r.result.chart.points.map(([x,y])=>`<div class="chart-row"><span>${esc(fmt(x))}</span><i style="width:${Math.max(1,Math.min(100,Number(y)*100))}%"></i><b>${esc(fmt(y))}</b></div>`).join('')}</div>`:''}`:'<div class="error">服务端未返回结果内容，请稍后刷新。</div>');
 return `<div class="eyebrow">03 / RESULTS ARCHIVE · RUN ${r.id}</div><h1>${esc(r.name)} ${status(r.status)}</h1><div class="checksum">INPUT SHA256 ${r.checksum}</div><p>快照 ${r.snapshot.length} 条 · ${esc(r.created_at)} · ${r.review?'负责人已复核':'尚未复核'}</p>${stage}${r.status==='failed'?`<div class="error">计算失败：${esc(r.error)}。请到来源页修复数据，或沿用原编号重试。</div>`:body}${gate.view||active(r)||r.status==='failed'?`<div class="toolbar">${actions}</div>`:''}<details><summary>本次冻结的参数与源记录</summary><pre>${esc(JSON.stringify({parameters:r.parameters,snapshot:r.snapshot},null,2))}</pre></details>`;
}
function viewer(){return localStorage.getItem('research-role')==='viewer';}
function stageNote(r,gate){if(gate&&gate.stage==='integrity')return '数据库结果与留档无法互相印证，入口已关闭；系统只尝试恢复原件，不重算已批准研究。查看、批准与下载在恢复并校验一致后自动重开。';if(r.status==='queued')return '登记与通知之间中断也可恢复：后台会反复补发通知，接取成功后只计算一次。';if(r.status==='running')return '活跃计算持续发送心跳；失联超过租约后自动由其他进程接管，迟归进程无权写入。';if(r.status==='failed')return '可沿用原编号重试；已保存结果不会重复计算，重试会产生新的接取编号。';if(r.integrity==='restored')return '留档曾被替换或缺失，已由数据库原件恢复并重新校验，入口已重新开放。';return '数据库结果与留档文件逐字节一致后才开放查看、批准与下载。';}
export function bind(refresh){const button=document.getElementById('export');if(button)button.onclick=async()=>{try{download(await runs.export(store.runId),'research-'+store.runId+'.csv');}catch(e){document.getElementById('notice').textContent=e.message;}};const retry=document.getElementById('retry');if(retry)retry.onclick=async()=>{retry.disabled=true;try{await runs.retry(store.runId);await refresh();}catch(e){document.getElementById('notice').textContent=e.message;retry.disabled=false;}};}
