# 站点读流量路由演练

栖原站点运维组（11人）在交接班前，用节点位点与会话时序复核读写路由，明确何时读副本、何时回主库、何时必须拒绝读取。

## 场景与模块

原先值班员在配置表和聊天记录中核对副本延迟，以“延迟很低”推断写后读安全。痛点：低延迟不等于已应用会话写位点、故障时回退策略不清、决策无逐请求证据。上游输入为人工确认的节点状态和请求轨迹，本站承接路由规则演练与证据留档，下游为站点值班与变更复核人员。

三个核心实体：复制节点 Node（主副角色/在线状态/日志谱系/位点/延迟），会话请求集 RequestSet（带时序的读写、复制应用、故障恢复事件），路由试验 Trial（冻结输入、策略参数与结果）。4业务页+登录页，另有管理员审计。

第二个演练域为副本 DDL 锁表回放：锁表场景 LockScenario（长查询起止、DDL 变更、主库并发写、副本回放、回放中断/恢复的冻结时序）与锁表演练 LockDrill（目标副本、新版查询别站、cancel/wait 策略与宽限、确定性结果）。对应 3 个业务页：锁表时序、锁表执行、锁表证据。

**这是受控复制路由模型，不连接真实主从集群。** MySQL真实保存节点、请求、任务及证据；Python真实逐事件计算策略。日志位点采用命名谱系中的单调整数，未实现完整GTID集合/真实复制协议。

## 快速启动

```sh
cp .env.example .env
docker compose up -d --build
docker compose exec -T backend python -m scripts.seed_demo
```

前端 http://localhost:18247 ，后端 http://localhost:19247/docs ，MySQL localhost:17747。

|角色|账号|密码|权限|
|---|---|---|---|
|admin|admin@qiyuan.test|Demo2026!|全部业务写入、运行与审计|
|operator|operator@qiyuan.test|Demo2026!|节点/请求编辑、运行、导出|
|viewer|viewer@qiyuan.test|Demo2026!|只读和导出，写API403|

## 一天的工作剧本

09:00 operator登录；09:05查看harbor-primary位点105、north-replica位点104/150ms、south-replica位点105/20ms；09:10打开 `QY-1117-A` 请求集；09:15确认八事件时序；09:20以100ms延迟门槛和500ms粘滞执行 `shift-20261117`；09:25检查：写入主库位点106、100ms读仍走主库、600ms南站追平106、700ms读走南站、800ms南站故障、900ms回主库、1000ms主库故障、1100ms拒绝读取；09:30导出route-1.json；09:40 viewer核验拒绝理由；10:00 admin看审计。

另有 `QY-1117-B` 高延迟副本回退主库、`QY-1117-C` 主库故障且副本未追平的拒绝案例。所有试验使用冻结快照，不会把模型故障写回节点台账。

## 真实计算规则

输入上限12节点、100事件；时间0–86400000ms，事件按at_ms非递减，时间相同按输入顺序执行。必须有且仅一个主节点，同谱系副本初始位点不能超过主库。事件字段：

```json
[
  {"at_ms":0,"kind":"write","session":"lab-A"},
  {"at_ms":100,"kind":"read","session":"lab-A"},
  {"at_ms":600,"kind":"replicate","node":"south-replica","position":106,"delay_ms":20},
  {"at_ms":700,"kind":"read","session":"lab-A"}
]
```

- write：主库在线才成功；主库位点+1，记录该会话required_position和last_write_ms。失败写入不推进会话位点。
- read：若距最近成功写入 `< sticky_ms`，必须主库在线、同谱系且位点满足；否则拒绝。到达边界即结束粘滞。
- 粘滞期外：副本必须在线、同谱系、position≥会话required_position、delay_ms≤max_delay_ms，四条件同时成立。多个候选按延迟升序、位点降序、名称字典序选择。无副本达标则回主库；主库也失效则拒绝。
- replicate：只允许在线同谱系副本；位点不能回退、不能超过主库已提交位点。node_down/node_up修改模型状态。未知节点或非法复制在入队前422。
- 每个会话独立维护位点；从未成功写入的会话required=0，允许最终一致副本读取，不承诺全局强一致。低延迟绝不覆盖落后位点。

输出每事件目标/拒绝、会话所需位点、实际位点、各副本四项资格、最终节点、会话状态、汇总和SHA256。相同输入产生相同摘要；不依赖随机数或外部网络。拒绝不安全读是正常业务结果，不算任务失败。快照与结果保存在MySQL，Redis只传递任务ID。

## 锁表回放计算规则

锁表场景事件按 at_ms 非递减、同时刻按输入顺序执行，最多100条；主库持续并发写（`primary_write`/`ddl_change` 各让主库提交位点+1）。事件类型：

```json
[
  {"at_ms":200,"kind":"query_start","query_id":"Q1-old","duration_ms":3600,"version":"old","session":"lab-A"},
  {"at_ms":1000,"kind":"ddl_change","table_name":"orders"},
  {"at_ms":1500,"kind":"replica_apply","node":"south-replica","position":106},
  {"at_ms":1600,"kind":"replay_interrupt","node":"south-replica"},
  {"at_ms":2000,"kind":"replay_resume","node":"south-replica"}
]
```

- 副本只在 `replica_apply` 回放到 DDL 变更位点时才请求表锁；位点必须单调，禁止跳过变更位点直接回放更后位置（入队前422）。
- 目标副本上所有在跑的旧版（version=old）长查询都是表锁阻塞者。`cancel` 策略给 `grace_ms` 宽限，到期仍持锁就一次性取消这些查询并应用 DDL；`wait` 策略不取消，DDL 等到阻塞查询自然结束。
- 中断（`replay_interrupt`）冻结位点和宽限时钟，期间不取消、不推进；恢复（`replay_resume`）从中断点重试，已取消查询不重复取消、已应用位点不重复推进。重复中断或无中断恢复在422拒绝。
- 新版（version=new）查询：变更未在目标副本应用前，改走已具备新表结构的健康别站，无别站则拒读，绝不回退到锁表副本；变更应用后才回到目标副本。
- 变更应用后到达的旧版查询按结构不兼容拒读；被取消查询在计划结束点产生的迟到结果一律 `rejected`，不能返回成功。
- 汇总输出取消查询数与 ID、回放等待表锁毫秒、主库变更到副本追上毫秒、新旧查询各去向计数、中断次数，以及恒为 0 的重复取消/重复推进哨兵字段和 SHA256。相同输入产生相同摘要。

## 工程说明

|服务|技术|职责|
|---|---|---|
|frontend|原生HTML/CSS/JS + Nginx|路由4业务页+锁表3业务页、编辑表单、共享组件、/api代理|
|backend|Python3.11/FastAPI/SQLAlchemy/PyJWT|校验、权限、审计、快照与导出|
|worker|Python3.11|路由时序算法与DDL锁表回放，消费两个Redis队列|
|db|MySQL8.0|真实持久化业务和JSON证据|
|redis|Redis7|持久队列|

各实体贯穿 `backend/src/{models,schemas,services,routes}` 和 `frontend/src/{api,stores,pages}`；其余强制目录含controllers/middlewares/config/constants/utils、components/common/hooks/types/router。三组件table/badge/frame共用；hooks/session/feedback和utils/forms/html共用；request_id/errors双中间件。

共享枚举角色admin/operator/viewer与任务queued/running/completed/failed位于 `backend/src/constants/enums.py` / `frontend/src/constants/enums.js`，security/trials、frame/badge消费。节点角色primary/replica和状态healthy/down由node schema及前端表单定义，事件类型由request_set schema及路由算法定义。

环境变量见.env.example：COMPOSE_PROJECT_NAME、三端口、DB_PASSWORD、JWT_SECRET、SEED_DEMO。默认密码仅本地演示、JWT8小时；seed自然键仅补缺失，不覆盖修改。schema由metadata.create_all初始建表，无在线升级引擎。5服务均健康检查，backend/worker依赖健康db/redis，frontend依赖backend；数据库、Redis、worker产物使用命名卷。

本地开发：Python3.11安装backend/requirements.txt、配置DATABASE_URL/REDIS_URL，在backend运行 `uvicorn src.main:app --reload` 和 `python -m src.worker`，前端经Nginx同源代理。停止清空：`docker compose down -v --remove-orphans`。

限制：单worker串行消费 `replicaroute:queue` 与 `replicalock:queue`、无生产任务重试/崩溃续跑；不模拟网络随机抖动、自动选主或完整GTID集合。Google Fonts离线回退，业务不依赖字体网络。

## License

MIT。源码外 `../../test-artifacts/gb-4108/` 含真实测试、截图与结果。
