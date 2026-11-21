from sqlalchemy import select
from redis import Redis
from fastapi import HTTPException
from src.models.entities import Node,LockScenario,LockDrill
from src.config.settings import REDIS_URL
from src.constants.enums import RUN_STATES
from src.utils.serialization import record
from .common import save
from .lockplay import simulate

queue=Redis.from_url(REDIS_URL,decode_responses=True)
LOCK_QUEUE='replicalock:queue'

def listing(db):return [record(x) for x in db.scalars(select(LockDrill).order_by(LockDrill.id.desc()))]

def detail(db,ident):
    obj=db.get(LockDrill,ident)
    if not obj:raise HTTPException(404,'锁表演练不存在')
    return record(obj)

def create(db,data,user):
    scenario=db.get(LockScenario,data.scenario_id)
    if not scenario:raise HTTPException(404,'锁表场景不存在')
    nodes=[record(x) for x in db.scalars(select(Node).order_by(Node.id))]
    node_names={n['name'] for n in nodes}
    if data.target_node not in node_names:raise HTTPException(422,'目标副本不在节点台账中')
    missing=[s for s in data.standby_nodes if s not in node_names]
    if missing:raise HTTPException(422,'别站节点不在节点台账中：'+'、'.join(missing))
    try:
        simulate(nodes,scenario.events,data.target_node,data.standby_nodes,data.strategy,data.grace_ms)
    except ValueError as e:
        raise HTTPException(422,str(e))
    snapshot={'nodes':nodes,'scenario':record(scenario)}
    obj=LockDrill(**data.model_dump(),input_snapshot=snapshot,status=RUN_STATES[0])
    result=save(db,obj,user,'锁表演练入队 '+data.name)
    try:
        queue.rpush(LOCK_QUEUE,str(obj.id))
    except Exception:
        obj.status='failed';obj.error='队列暂时不可用，请重新创建';db.commit()
        raise HTTPException(503,obj.error)
    return result
