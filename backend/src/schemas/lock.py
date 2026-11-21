from typing import Literal
from pydantic import BaseModel,Field,model_validator

LOCK_KINDS=('query_start','ddl_change','primary_write','replica_apply','replay_interrupt','replay_resume')
class LockEvent(BaseModel):
    at_ms:int=Field(ge=0,le=86400000)
    kind:Literal['query_start','ddl_change','primary_write','replica_apply','replay_interrupt','replay_resume']
    query_id:str|None=Field(default=None,max_length=60)
    duration_ms:int|None=Field(default=None,ge=1,le=86400000)
    version:Literal['old','new']|None=None
    session:str=Field(default='default',min_length=1,max_length=60)
    node:str|None=Field(default=None,max_length=80)
    table_name:str|None=Field(default=None,max_length=80)
    position:int|None=Field(default=None,ge=0,le=1000000000)
    @model_validator(mode='after')
    def valid_event(self):
        if self.kind=='query_start':
            if not self.query_id or self.duration_ms is None or self.version is None:
                raise ValueError('query_start需要query_id、duration_ms和version')
        if self.kind=='ddl_change' and not self.table_name:
            raise ValueError('ddl_change需要table_name')
        if self.kind=='replica_apply':
            if not self.node or self.position is None:
                raise ValueError('replica_apply需要node和position')
        return self

class LockScenarioInput(BaseModel):
    code:str=Field(min_length=2,max_length=80)
    title:str=Field(min_length=2,max_length=160)
    events:list[LockEvent]=Field(min_length=1,max_length=100)
    note:str=Field(default='',max_length=300)
    @model_validator(mode='after')
    def chronological(self):
        if any(a.at_ms>b.at_ms for a,b in zip(self.events,self.events[1:])):
            raise ValueError('事件必须按at_ms非递减排列')
        return self

class LockDrillInput(BaseModel):
    name:str=Field(min_length=2,max_length=120)
    scenario_id:int=Field(gt=0)
    target_node:str=Field(min_length=1,max_length=80)
    standby_nodes:list[str]=Field(default=list,max_length=12)
    strategy:Literal['cancel','wait']
    grace_ms:int=Field(default=0,ge=0,le=3600000)
    @model_validator(mode='after')
    def unique_nodes(self):
        if len(set(self.standby_nodes))!=len(self.standby_nodes):
            raise ValueError('别站节点不得重复')
        if self.target_node in self.standby_nodes:
            raise ValueError('目标副本不能同时作为别站')
        return self
