import traceback,json
from pathlib import Path
from src.config.database import Session
from src.models.entities import Trial,LockDrill
from src.services.trials import queue
from src.services.lock_drills import LOCK_QUEUE
from src.services.routing import simulate
from src.services.lockplay import simulate as simulate_lock
QUEUES=('replicaroute:queue',LOCK_QUEUE)
def run_route(ident):
    with Session() as db:
        obj=db.get(Trial,ident);obj.status='running';db.commit()
        snapshot=obj.input_snapshot;delay=obj.max_delay_ms;sticky=obj.sticky_ms
    result=simulate(snapshot['nodes'],snapshot['request_set']['events'],delay,sticky)
    with Session() as db:
        obj=db.get(Trial,ident);obj.result=result;obj.status='completed';db.commit()
    Path(f'/artifacts/route-{ident}.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
def run_lock(ident):
    with Session() as db:
        obj=db.get(LockDrill,ident);obj.status='running';db.commit()
        snapshot=obj.input_snapshot
        target=obj.target_node;standby=obj.standby_nodes;strategy=obj.strategy;grace=obj.grace_ms
    result=simulate_lock(snapshot['nodes'],snapshot['scenario']['events'],target,standby,strategy,grace)
    with Session() as db:
        obj=db.get(LockDrill,ident);obj.result=result;obj.status='completed';db.commit()
    Path(f'/artifacts/lock-drill-{ident}.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
HANDLERS={'replicaroute:queue':(Trial,run_route),'replicalock:queue':(LockDrill,run_lock)}
while True:
    Path('/tmp/worker-heartbeat').touch()
    item=None
    for name in QUEUES:
        item=queue.blpop(name,timeout=2)
        if item:break
    if not item:continue
    name,payload=item;ident=int(payload);model,handler=HANDLERS[name]
    try:
        handler(ident)
    except Exception as e:
        traceback.print_exc()
        with Session() as db:
            obj=db.get(model,ident)
            if obj:obj.status='failed';obj.error=str(e);db.commit()
