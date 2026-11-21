from sqlalchemy import select,func
from src.config.database import Session,Base,engine
from src.models.entities import User,Node,RequestSet,Trial,LockScenario,LockDrill
from src.utils.security import password_hash

def seed():
    Base.metadata.create_all(engine)
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email=role+'@qiyuan.test'
            if not db.scalar(select(User).where(User.email==email)):db.add(User(email=email,password=password_hash('Demo2026!'),role=role))
        for name,role,pos,delay,note in [('harbor-primary','primary',105,0,'栖原主站 · 写入航道'),('north-replica','replica',104,150,'北站副本 · 延迟偏高'),('south-replica','replica',105,20,'南站副本 · 常用读航道'),('east-standby','replica',105,40,'东站备援 · 新版查询别站')]:
            if not db.scalar(select(Node).where(Node.name==name)):db.add(Node(name=name,role=role,status='healthy',lineage='qiyuan-log-01',position=pos,delay_ms=delay,note=note))
        mixed=[{'at_ms':0,'kind':'write','session':'lab-A'},{'at_ms':100,'kind':'read','session':'lab-A'},{'at_ms':600,'kind':'replicate','node':'south-replica','position':106,'delay_ms':20},{'at_ms':700,'kind':'read','session':'lab-A'},{'at_ms':800,'kind':'node_down','node':'south-replica'},{'at_ms':900,'kind':'read','session':'lab-A'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-A'}]
        cases=[('QY-1117-A','交接班写后读与双节点故障',mixed),('QY-1117-B','延迟超标回退主库',[{'at_ms':0,'kind':'node_down','node':'south-replica'},{'at_ms':10,'kind':'read','session':'visitor'}]),('QY-1117-C','双副本不可读时拒绝',[{'at_ms':0,'kind':'write','session':'lab-B'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-B'}])]
        for code,title,events in cases:
            if not db.scalar(select(RequestSet).where(RequestSet.code==code)):db.add(RequestSet(code=code,title=title,events=events,note='栖原站点值班交接 · 09:15 请求轨迹'))
        lock_cases=[
            ('LK-1121-A','取消长查询让副本追上变更',[
                {'at_ms':200,'kind':'query_start','query_id':'Q1-old-report','duration_ms':3600,'version':'old','session':'lab-A'},
                {'at_ms':700,'kind':'query_start','query_id':'Q2-old-export','duration_ms':2600,'version':'old','session':'lab-B'},
                {'at_ms':1000,'kind':'ddl_change','table_name':'orders'},
                {'at_ms':1100,'kind':'primary_write'},
                {'at_ms':1200,'kind':'replica_apply','node':'east-standby','position':106},
                {'at_ms':1300,'kind':'query_start','query_id':'Q3-new-dashboard','duration_ms':900,'version':'new','session':'lab-C'},
                {'at_ms':1500,'kind':'replica_apply','node':'south-replica','position':106},
                {'at_ms':1800,'kind':'replica_apply','node':'south-replica','position':106},
                {'at_ms':2000,'kind':'replica_apply','node':'south-replica','position':106},
                {'at_ms':4600,'kind':'query_start','query_id':'Q4-new-after-apply','duration_ms':600,'version':'new','session':'lab-E'},
            ]),
            ('LK-1121-B','wait策略：旧查询跑完才应用变更',[
                {'at_ms':200,'kind':'query_start','query_id':'Q1-old-report','duration_ms':3600,'version':'old','session':'lab-A'},
                {'at_ms':700,'kind':'query_start','query_id':'Q2-old-export','duration_ms':2600,'version':'old','session':'lab-B'},
                {'at_ms':1000,'kind':'ddl_change','table_name':'orders'},
                {'at_ms':1500,'kind':'replica_apply','node':'south-replica','position':106},
                {'at_ms':3300,'kind':'replica_apply','node':'south-replica','position':106},
                {'at_ms':3800,'kind':'replica_apply','node':'south-replica','position':106},
                {'at_ms':4200,'kind':'query_start','query_id':'Q3-old-late','duration_ms':500,'version':'old','session':'lab-D'},
            ]),
            ('LK-1121-C','回放中断恢复：重试不重复取消或推进位点',[
                {'at_ms':200,'kind':'query_start','query_id':'Q1-old-report','duration_ms':4000,'version':'old','session':'lab-A'},
                {'at_ms':1000,'kind':'ddl_change','table_name':'orders'},
                {'at_ms':1400,'kind':'replica_apply','node':'south-replica','position':106},
                {'at_ms':1600,'kind':'replay_interrupt','node':'south-replica'},
                {'at_ms':2000,'kind':'replay_resume','node':'south-replica'},
                {'at_ms':2200,'kind':'replica_apply','node':'south-replica','position':106},
                {'at_ms':2600,'kind':'replica_apply','node':'south-replica','position':106},
            ]),
        ]
        for code,title,events in lock_cases:
            if not db.scalar(select(LockScenario).where(LockScenario.code==code)):db.add(LockScenario(code=code,title=title,events=events,note='副本长查询与表锁变更回放 · 冻结时序'))
        db.commit();print({cls.__tablename__:db.scalar(select(func.count()).select_from(cls)) for cls in (User,Node,RequestSet,Trial,LockScenario,LockDrill)})
if __name__=='__main__':seed()
