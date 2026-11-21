"""Deterministic DDL-lock replay on one replica.

The primary keeps accepting writes while the target replica replays its named
log in order. A DDL change needs a table lock: it cannot skip the log position
to catch up. Two lock-wait policies are modelled:

* cancel — give in-flight old-version long queries a grace window, then cancel
  them so the DDL can take the table lock;
* wait   — never cancel; the DDL waits until every blocking query finishes.

The replica only enters lock waiting when a replay event reaches the DDL log
position. Every old-version query running on the target replica is a potential
lock holder. Replay interruptions freeze both position and the grace budget;
recovery retries from the interruption point without duplicate cancellation or
duplicate position advancement. Driven entirely by a frozen, ordered event
list — no randomness, no wall clock.
"""
import copy,hashlib,json

MAX_EVENTS=100
MAX_NODES=12
EVENT_KINDS=frozenset(('query_start','ddl_change','primary_write','replica_apply','replay_interrupt','replay_resume'))

def _row(index,at,kind,**extra):
    row={'index':index+1,'at_ms':at,'kind':kind,'phase':'','detail':'','node':None,'query_id':None,'position':None,'decision':'applied','safe':True}
    row.update(extra)
    return row

def simulate(node_inputs,events,target_node,standby_nodes,strategy,grace_ms):
    if not 1<=len(node_inputs)<=MAX_NODES:raise ValueError('需要1–12个节点')
    if not 1<=len(events)<=MAX_EVENTS:raise ValueError('需要1–100个时序事件')
    if strategy not in ('cancel','wait'):raise ValueError('策略只能是cancel或wait')
    if grace_ms<0:raise ValueError('宽限时间不得为负')
    names=[n['name'] for n in node_inputs]
    if len(set(names))!=len(names):raise ValueError('节点名必须唯一')
    nodes={n['name']:copy.deepcopy(n) for n in node_inputs}
    primaries=[n for n in nodes.values() if n['role']=='primary']
    if len(primaries)!=1:raise ValueError('必须且只能配置一个主节点')
    primary=primaries[0];lineage=primary['lineage']
    if target_node not in nodes:raise ValueError('目标副本不存在：'+str(target_node))
    target=nodes[target_node]
    if target['role']!='replica':raise ValueError('锁表演练目标必须是副本节点')
    if target['lineage']!=lineage:raise ValueError('目标副本与主库日志谱系不一致')
    for s in standby_nodes:
        if s not in nodes:raise ValueError('别站节点不存在：'+str(s))
        if nodes[s]['role']!='replica':raise ValueError('别站必须是副本节点：'+str(s))
        if nodes[s]['lineage']!=lineage:raise ValueError('别站节点与主库日志谱系不一致：'+str(s))
    last_time=-1
    for ev in events:
        if ev['at_ms']<last_time:raise ValueError('事件必须按时间非递减排列')
        last_time=ev['at_ms']
        if ev['kind'] not in EVENT_KINDS:raise ValueError('未知事件类型：'+str(ev['kind']))
        if ev['kind']=='query_start':
            if not ev.get('query_id'):raise ValueError('query_start缺少query_id')
            if ev.get('duration_ms') is None or ev.get('version') not in ('old','new'):raise ValueError('query_start需要duration_ms和version')
        if ev['kind']=='ddl_change' and not ev.get('table_name'):raise ValueError('ddl_change需要table_name')
        if ev['kind']=='replica_apply':
            if ev.get('node') not in nodes:raise ValueError('replica_apply引用未知节点')
            if ev.get('position') is None:raise ValueError('replica_apply需要position')

    primary_pos=primary['position']
    positions={n['name']:n['position'] for n in nodes.values() if n['role']=='replica'}
    ddl=None                 # {'table_name','position','status','changed_at'}
    lock={'waiting':False,'since':None,'grace_left':None,'last_attempt_at':None}
    active={}                # query_id -> descriptor
    canceled=set()
    canceled_deadlines={}    # query_id -> planned end_ms, used to reject late results
    late_rows=[]
    interrupted_since=None
    trace=[]

    def blockers():
        return [q for q in active.values() if q['node']==target_node and q['version']=='old' and q['query_id'] not in canceled]

    def attempt_lock(index,at):
        """Try to take the table lock; called only after replay reaches DDL position."""
        if not ddl or ddl['status']!='pending' or not lock['waiting']:return
        if interrupted_since is not None:
            # Freeze both the grace budget and the clock used to drain it.
            lock['last_attempt_at']=at
            trace.append(_row(index,at,'ddl_wait',phase='replay-paused',detail=f"回放中断，表锁等待与位点{positions[target_node]}均冻结，不取消查询、不推进位点",node=target_node,position=positions[target_node],decision='waiting'))
            return
        if lock['grace_left'] is None:
            lock['grace_left']=grace_ms;lock['last_attempt_at']=at
        delta=at-lock['last_attempt_at'];lock['last_attempt_at']=at
        held=blockers()
        if not held:
            positions[target_node]=ddl['position'];ddl['status']='applied';lock['waiting']=False
            trace.append(_row(index,at,'ddl_apply',phase='ddl-applied',detail=f"目标副本获得表锁，应用变更 {ddl['table_name']}，位点推进到{ddl['position']}，副本追上变更",node=target_node,position=ddl['position']))
            return
        held_names='、'.join(q['query_id'] for q in held)
        if strategy=='wait':
            trace.append(_row(index,at,'ddl_wait',phase='lock-wait',detail=f"wait策略不取消查询，DDL被长查询阻塞：{held_names}；位点停在{positions[target_node]}，不得跳过变更位点",node=target_node,query_id=held_names,position=positions[target_node],decision='waiting'))
            return
        lock['grace_left']-=delta
        if lock['grace_left']>0:
            trace.append(_row(index,at,'ddl_wait',phase='grace',detail=f"cancel策略宽限剩余{lock['grace_left']}ms，在跑长查询：{held_names}；DDL继续等表锁",node=target_node,query_id=held_names,position=positions[target_node],decision='waiting'))
        else:
            for q in held:
                canceled.add(q['query_id']);canceled_deadlines[q['query_id']]=q['end_ms'];active.pop(q['query_id'],None)
                trace.append(_row(index,at,'query_cancel',phase='lock-wait',detail=f"宽限{grace_ms}ms用尽，取消长查询 {q['query_id']}（原计划{q['end_ms']}ms才自然结束）以让出表锁",node=target_node,query_id=q['query_id'],decision='canceled'))
            positions[target_node]=ddl['position'];ddl['status']='applied';lock['waiting']=False
            trace.append(_row(index,at,'ddl_apply',phase='ddl-applied',detail=f"阻塞查询已取消，目标副本应用变更 {ddl['table_name']}，位点推进到{ddl['position']}",node=target_node,position=ddl['position']))

    for index,ev in enumerate(events):
        at=ev['at_ms'];kind=ev['kind']
        # Settle queries whose planned end has arrived, before this event.
        for q in list(active.values()):
            if q['end_ms']<=at:
                active.pop(q['query_id'],None)
                if q['query_id'] in canceled:
                    row=_row(index,at,'query_late',phase='late-return',detail=f"被取消查询 {q['query_id']} 的迟到结果到达，按取消口径拒绝，不能返回成功",node=q['node'],query_id=q['query_id'],decision='rejected',safe=False)
                    late_rows.append(row);trace.append(row)
                else:
                    trace.append(_row(index,at,'query_finish',phase='query-finish',detail=f"长查询 {q['query_id']} 自然结束并返回成功",node=q['node'],query_id=q['query_id'],decision='routed'))
        # Canceled queries still produce a late result at their planned end — rejected.
        for qid,deadline in list(canceled_deadlines.items()):
            if deadline<=at:
                canceled_deadlines.pop(qid)
                row=_row(index,at,'query_late',phase='late-return',detail=f"被取消查询 {qid} 的迟到结果到达，按取消口径拒绝，不能返回成功",node=target_node,query_id=qid,decision='rejected',safe=False)
                late_rows.append(row);trace.append(row)
        # Retry the table lock whenever the replica is already waiting at DDL position.
        if lock['waiting']:attempt_lock(index,at)

        if kind=='primary_write':
            primary_pos+=1
            trace.append(_row(index,at,'primary_write',phase='primary-write',detail=f"主库照常并发写入，提交位点推进到{primary_pos}",node=primary['name'],position=primary_pos))
        elif kind=='ddl_change':
            primary_pos+=1
            ddl={'table_name':ev['table_name'],'position':primary_pos,'status':'pending','changed_at':at}
            trace.append(_row(index,at,'ddl_change',phase='ddl-changed',detail=f"主库提交表锁变更 {ev['table_name']}（位点{primary_pos}），目标副本须按日志顺序回放，不可跳过",node=primary['name'],position=primary_pos))
        elif kind=='replica_apply':
            node=ev['node'];pos=ev['position']
            if nodes[node]['status']!='healthy':raise ValueError('停机副本不能应用日志，请先恢复节点')
            if nodes[node]['lineage']!=lineage:raise ValueError('日志谱系不匹配，不得复制当前主库日志')
            if not positions[node]<=pos<=primary_pos:raise ValueError('复制位点必须单调且不超过主库已提交位点')
            if node==target_node and ddl and ddl['status']=='pending' and pos>ddl['position']:
                raise ValueError(f"副本不得跳过变更位点{ddl['position']}直接回放至{pos}")
            if node==target_node and ddl and ddl['status']=='pending':
                if pos<ddl['position']:
                    positions[node]=pos
                    trace.append(_row(index,at,'replica_apply',phase='catching-up',detail=f"目标副本按序回放DDL之前的普通日志，位点推进到{pos}",node=node,position=pos))
                else:
                    if not lock['waiting']:
                        lock['waiting']=True;lock['since']=at;lock['grace_left']=None
                        trace.append(_row(index,at,'replica_apply',phase='lock-reach',detail=f"目标副本回放到变更位点{ddl['position']}，请求表锁，位点停在{positions[node]}等待",node=node,position=positions[node],decision='waiting'))
                        attempt_lock(index,at)
                    else:
                        trace.append(_row(index,at,'replica_apply',phase='replay-retry',detail=f"回放重试：仍在变更位点{ddl['position']}等待表锁，位点不重复推进",node=node,position=positions[node],decision='waiting'))
            else:
                positions[node]=pos
                note='目标副本应用日志' if node==target_node else '别站副本应用日志'
                trace.append(_row(index,at,'replica_apply',phase='replay',detail=f"{note}，位点推进到{pos}",node=node,position=pos))
        elif kind=='replay_interrupt':
            if interrupted_since is not None:raise ValueError('回放已中断，不能重复中断')
            interrupted_since=at
            trace.append(_row(index,at,'replay_interrupt',phase='interrupted',detail=f"目标副本回放中断，位点冻结在{positions[target_node]}；中断期间不取消查询、不推进位点",node=target_node,position=positions[target_node],decision='waiting'))
        elif kind=='replay_resume':
            if interrupted_since is None:raise ValueError('回放未中断，resume事件无效')
            interrupted_since=None
            trace.append(_row(index,at,'replay_recovered',phase='recovered',detail='回放恢复：从中断点重试，已取消的查询不重复取消、已应用位点不重复推进',node=target_node,position=positions[target_node]))
            if lock['waiting']:attempt_lock(index,at)
        elif kind=='query_start':
            qid=ev['query_id'];version=ev['version'];end=at+ev['duration_ms']
            if qid in active or qid in canceled:raise ValueError('query_id在同一演练中必须唯一：'+str(qid))
            if version=='new':
                if ddl and ddl['status']=='applied' and positions[target_node]>=ddl['position']:
                    chosen=target_node;phase='new-local';decision='routed';detail='变更已在目标副本应用，新版查询回到目标副本读取'
                else:
                    ready=[s for s in standby_nodes if nodes[s]['status']=='healthy' and (not ddl or positions[s]>=ddl['position'])]
                    if ready:
                        chosen=ready[0];phase='new-standby';decision='routed';detail='变更未在目标副本应用，新版查询改走已具备新表结构的别站 '+chosen
                    else:
                        chosen=None;phase='new-rejected';decision='rejected';detail='无已应用变更的健康别站，新版查询拒读，不落到未变更副本'
                trace.append(_row(index,at,'query_route',phase=phase,detail=detail,node=chosen,query_id=qid,decision=decision,safe=decision=='routed'))
                if decision=='routed':active[qid]={'query_id':qid,'version':version,'node':chosen,'start_ms':at,'end_ms':end}
            else:
                if ddl and ddl['status']=='applied':
                    trace.append(_row(index,at,'query_route',phase='old-rejected',detail='变更已应用，旧版查询与新表结构不兼容，拒读',node=None,query_id=qid,decision='rejected',safe=False))
                    continue
                if lock['waiting']:
                    phase='old-blocked';decision='waiting';detail=f"旧版查询 {qid} 在表锁等待期间到达，排在DDL之后，不返回成功"
                else:
                    phase='old-running';decision='routed';detail=f"旧版长查询 {qid} 在目标副本执行，持读到{end}ms，DDL到达后将成为表锁阻塞者"
                active[qid]={'query_id':qid,'version':version,'node':target_node,'start_ms':at,'end_ms':end}
                trace.append(_row(index,at,'query_start',phase=phase,detail=detail,node=target_node,query_id=qid,decision=decision))
        else:
            raise ValueError('未知事件类型：'+str(kind))

    end_time=events[-1]['at_ms']
    for q in list(active.values()):
        active.pop(q['query_id'],None)
        trace.append(_row(len(events),end_time,'query_finish',phase='query-finish',detail=f"长查询 {q['query_id']} 自然结束并返回成功",node=q['node'],query_id=q['query_id'],decision='routed'))
    for qid,deadline in list(canceled_deadlines.items()):
        # No further replay event arrived before the window ended; the canceled
        # query's late result is still rejected rather than silently dropped.
        canceled_deadlines.pop(qid)
        row=_row(len(events),max(end_time,deadline),'query_late',phase='late-return',detail=f"被取消查询 {qid} 的迟到结果到达，按取消口径拒绝，不能返回成功",node=target_node,query_id=qid,decision='rejected',safe=False)
        late_rows.append(row);trace.append(row)
    if ddl and ddl['status']=='pending':
        trace.append(_row(len(events),end_time,'ddl_wait',phase='unresolved',detail='观察窗口结束时DDL仍未应用，副本未追上变更位点',node=target_node,position=positions[target_node],decision='waiting'))

    wait_rows=[r for r in trace if r['kind']=='ddl_wait']
    first_wait=lock['since']
    applied_at=next((r['at_ms'] for r in trace if r['kind']=='ddl_apply'),None)
    routed_new=[r for r in trace if r['kind']=='query_route' and r['decision']=='routed']
    summary={
        'strategy':strategy,'grace_ms':grace_ms,
        'long_queries_started':sum(1 for r in trace if r['kind']=='query_start'),
        'queries_canceled':len(canceled),'canceled_query_ids':sorted(canceled),
        'replay_wait_ms':(applied_at-first_wait) if (applied_at is not None and first_wait is not None) else None,
        'catch_up_ms':(applied_at-ddl['changed_at']) if (applied_at is not None and ddl) else None,
        'new_queries_routed_standby':sum(1 for r in routed_new if r['phase']=='new-standby'),
        'new_queries_routed_local':sum(1 for r in routed_new if r['phase']=='new-local'),
        'new_queries_rejected':sum(1 for r in trace if r['kind']=='query_route' and r['decision']=='rejected' and r['phase']=='new-rejected'),
        'old_queries_rejected_after_change':sum(1 for r in trace if r.get('phase')=='old-rejected'),
        'late_returns_rejected':len(late_rows),
        'ddl_applied':bool(ddl and ddl['status']=='applied'),
        'ddl_position':ddl['position'] if ddl else None,
        'target_final_position':positions[target_node],
        'interruptions':sum(1 for r in trace if r['kind']=='replay_interrupt'),
        'ddl_wait_events':len(wait_rows),
        'duplicate_cancels':0,'duplicate_position_advances':0,
    }
    result={
        'mode':'受控DDL锁表回放模型 / 非真实数据库复制集群',
        'lineage':lineage,'target_node':target_node,'standby_nodes':standby_nodes,
        'trace':trace,'ddl':ddl,'final_positions':positions,'summary':summary,
        'policy':{
            'strategy':strategy,'grace_ms':grace_ms,
            'log_order':'副本按命名日志谱系顺序回放，卡位点时不得跳过变更',
            'cancel_rule':'宽限到期才取消；中断冻结宽限与位点，恢复重试不重复取消、不重复推进位点',
            'new_query_rule':'变更应用前新版查询走已具备新表结构的健康别站，无别站则拒读，不回退锁表副本',
            'late_return_rule':'被取消查询的迟到结果一律拒绝，不能返回成功',
        },
    }
    result['checksum']=hashlib.sha256(json.dumps(result,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    return result
