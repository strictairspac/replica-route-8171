import {table} from '../components/common/table.js';import {esc} from '../utils/html.js';import {session} from '../hooks/session.js';
const sample=[
 {at_ms:200,kind:'query_start',query_id:'Q1-old-report',duration_ms:3600,version:'old',session:'lab-A'},
 {at_ms:1000,kind:'ddl_change',table_name:'orders'},
 {at_ms:1500,kind:'replica_apply',node:'south-replica',position:106},
 {at_ms:2000,kind:'replica_apply',node:'south-replica',position:106},
];
const KIND_HELP=[
 ['query_start','长查询起止：query_id、duration_ms、version=old/new；新版查询在变更应用前自动走别站或拒读'],
 ['ddl_change','锁表变更：table_name；主库位点自动+1，副本必须按序回放到该位点才能拿表锁'],
 ['primary_write','主库并发写：无额外字段，主库提交位点+1'],
 ['replica_apply','副本回放：node、position；目标副本到达变更位点即进入表锁等待，禁止跳过该位点'],
 ['replay_interrupt / replay_resume','回放中断与恢复：node；中断冻结位点与宽限，恢复重试不重复取消/推进'],
];
export function lockScenariosPage(items){return `<div class="split"><section><div class="section-top"><h2>锁表变更时序</h2><span>${items.length} SCENARIOS / DDL LOCK REPLAY</span></div>${table(['场景 / 说明','事件数','操作'],items.map(r=>[`<strong>${esc(r.code)}</strong><small>${esc(r.title)}</small><small>${esc(r.note)}</small>`,`${r.events.length} 个`,`<button class="small edit-lock-scenario" data-id="${r.id}" ${session.user.role==='viewer'?'disabled':''}>编辑</button>`]))}<div class="protocol"><h3>事件字段速查</h3>${KIND_HELP.map(([k,d])=>`<p><b>${esc(k)}</b><br>${esc(d)}</p>`).join('')}<p>事件按 at_ms 非递减排列，同一时刻按输入先后，最多 100 条。主库持续并发写，副本卡住时绝不跳过变更位点。</p></div></section><section class="form-panel"><span class="eyebrow">DDL LOCK SCENARIO</span><h2 id="lock-scenario-heading">编排锁表演练时序</h2><form id="lock-scenario-form"><input type="hidden" name="id"><label>场景编号<input name="code" placeholder="LK-1121-A" required></label><label>工作场景<input name="title" value="副本长查询与表锁变更" required></label><label>时序事件JSON<textarea name="events" rows="14" required>${esc(JSON.stringify(sample,null,2))}</textarea></label><label>备注<input name="note" value="锁表与长查询回放 · 冻结时序"></label><button class="primary" ${session.user.role==='viewer'?'disabled':''}>保存锁表场景 +</button></form><p class="help">冻结输入后由后台 Python 逐事件计算；取消数、回放等待时长、新旧查询去向均输出确定性证据。</p></section></div>`;}
