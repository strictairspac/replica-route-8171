from sqlalchemy import select
from fastapi import HTTPException
from src.models.entities import LockScenario
from src.utils.serialization import record
from .common import save

def listing(db):return [record(x) for x in db.scalars(select(LockScenario).order_by(LockScenario.id))]

def create(db,data,user):
    return save(db,LockScenario(**data.model_dump()),user,'创建锁表演练场景 '+data.code)

def update(db,ident,data,user):
    obj=db.get(LockScenario,ident)
    if not obj:raise HTTPException(404,'锁表场景不存在')
    for k,v in data.model_dump().items():setattr(obj,k,v)
    return save(db,obj,user,'更新锁表演练场景 '+data.code)
