# 砚田包装试封比较席

两种封口工艺独立合格/失败计数→固定方案比较→复试决策；双侧Fisher按概率不大于观测表求和。原生 HTML/CSS/JavaScript 多模块工作席，所有计算由 Python 完成。

## 模拟业务
砚田研究协作组，12名分析师与业务人员，日常使用 `seal-fisher-202611-1.csv` 手工抄表。当前三个痛点：观测与口径不可追溯、重算易出错、结果与复核割裂。上游提供独立合成业务观测；本应用接管数据登记、方法执行和输入冻结；下游负责人形成复测/预算方案，观察员获取复核后的CSV。

核心业务实体：SealTrial / TrialCount / ExactRun / TrialDecision。源码 model/schema/service/controller/route 与前端 api/store/pages 完成链路，不依赖浏览器本地假数据。

## 功能与方法
- 试封方案、工艺计数录入/编辑/删除；重名记录与非法数值明确拒绝。
- Fisher精确检验、Wilson单比例区间与率差。两组试件独立，组别预先固定。双侧 p 值是固定边际下概率不大于观察表的所有表之和。
- 每次运行冻结参数、观测和SHA256。Redis任务由独立worker计算，写DB与共享产物卷；输入修改不会改写历史结果。
- 多计算进程并发接取：行级租约+心跳，失联后自动接管，接取编号即围栏令牌，迟归进程无权改写；已保存结果不重算，重试沿用原编号。
- 排队登记与Redis通知之间用事务外盒（outbox）衔接：中断后自动补发，重复通知会被接取条件吸收。
- 已完成运行在查看、复核、下载前逐字节核对数据库结果与留档JSON；缺失、损坏、串档时关闭入口并由数据库原件恢复，页面显示当前阶段与原因。已批准研究不重算。
- 负责人复核不可覆盖；审计记录登录、数据改动、排队、复核与导出；240请求/分钟限流、全局可恢复错误。

## 快速启动
```bash
cp .env.example .env
docker compose up -d --build
docker compose exec -T backend python -m scripts.seed_demo
```
访问 http://localhost:18241 ，API http://localhost:19241/docs 。数据库映射 17741。无需Node构建。可以从中文和带空格目录启动。清理 `docker compose down -v --remove-orphans` 会删除本题演示数据卷。

|角色|账号|密码|权限与典型动作|
|---|---|---|---|
|admin 负责人|admin@demo.local|Research2026!|全部读写、批准复核、下载|
|analyst 分析师|analyst@demo.local|Research2026!|数据修改、创建任务、读取结果及下载；不能复核|
|viewer 观察员|viewer@demo.local|Research2026!|只读；仅可导出已复核结果|

## 一天的工作剧本
1. 09:00 admin登录，查“砚田·薄膜一轮”。
2. 09:10 analyst核对 `seal-fisher-202611-1.csv` 的seed记录。
3. 09:15 来源录入页检查 `工艺组 (A/B)` 等业务字段，必要时编辑。
4. 09:20 分析执行页保留默认参数 `{"alpha": 0.05}`，命名本轮研究。
5. 09:21 冻结输入并排队，worker实算，结果页自动刷新。
6. 09:25 阅读核心数值、方法边界、明细与SHA256；发现失败先修数据重跑。
7. 09:35 admin打开业务复核，写明限制与下一步动作，归档后不可覆盖。
8. 09:40 viewer读取复核意见并下载CSV，在操作留痕页核对下载事件。

seed提供3次试验×2个工艺组，共6条汇总计数，代表600份独立试件。每次运行每组只允许一条预聚合记录；修正计数应编辑原记录，重复组会明确失败。

## 技术与目录
|部分|实现|
|---|---|
|前端|原生ES modules、HTML/CSS、Nginx1.27，无前端框架|
|API|Python3.11、FastAPI0.115、SQLAlchemy2、JWT、PBKDF2密码|
|计算|NumPy2.2、SciPy1.15，固定seed42|
|数据库|MySQL8|
|任务|Redis7持久队列 + 数据库租约/围栏/outbox，Python独立worker（可多副本、进程内并发），心跳healthcheck|

`frontend/src/`分api、stores、types、components/common、hooks、pages、router、utils、constants；`backend/src/`分config、models、schemas、services、controllers、routes、middlewares、utils、constants；`backend/scripts/seed_demo.py`自然键插入、重复不覆盖修改；`backend/migrations/README.md`说明schema版本。

环境变量 `.env.example`：COMPOSE_PROJECT_NAME、FRONTEND_PORT、BACKEND_PORT、DB_PORT、DB_PASSWORD、JWT_SECRET、SEED_DEMO。开发时在本地venv安装backend/requirements.txt，设置DATABASE_URL、REDIS_URL、JWT_SECRET再运行uvicorn；前端开发经Nginx代理 `/api/` 到不带前缀的API。

共享枚举：角色admin/analyst/viewer和队列queued/running/completed/failed；后端`src/constants/enums.py`、认证与worker使用相同值；前端`src/types/enums.js`、状态与角色界面使用相同值。Google Fonts：Noto Serif SC + IBM Plex Mono；断网使用serif/monospace回退。

## 算法输入、失败态与复现
输入为选定批次全部观测和参数，输出核心数值、明细、解释；参数经后端严格白名单和范围验证。两组试件独立，组别预先固定。双侧 p 值是固定边际下概率不大于观察表的所有表之和。。空批次拒绝；不完整字段/非有限数/非法计数拒绝；常量相关列与不足样本使任务failed并保留错误。固定seed42、输入checksum相同可复现。独立oracle：2×2固定边际对scipy fisher_exact；零格与计数非法拒绝。

已知边界：小型本地研究应用，使用合成数据，不含企业SSO与外发邮件；结果由人工复核，不自动执行业务决策。队列不假设分布式事务：登记先落库（含outbox），Redis只作投递提示；重复或丢失通知由接取条件、补发与租约收敛。计算进程可并发（`WORKER_CONCURRENCY`、`--scale worker=N`），单机单worker仍完全可用。留档校验以数据库登记结果哈希为准，运行结束后被改写的数据库行会被关闭入口而非静默通过。以 `backend/tests/` 的实际结果记录验证（见下）。

MIT License。

## 并发、恢复与留档校验（实现说明）

**排队登记 → 通知**：`runs.create` 先在同一事务内写入运行行与 `outbox(run_id, attempt)`，提交后才尽力 `RPUSH`。若提交后、推送前中断（Redis 不可达或进程退出），`outbox.sent_at` 仍为空，worker/请求路径的 `relay()` 会补发。重复推送不产生重复计算：接取是一次条件 `UPDATE`。

**活跃计算与失联接管**：接取把 `status=running`、`attempt+1`、`owner_id`、`lease_expires_at` 一并写入，条件是 `queued` 或租约已过期。计算期间每 `lease/3` 秒心跳续租。只有 `(owner_id, attempt)` 仍匹配的行才能写失败或完成；被接管的旧进程在 `complete` 的 `WHERE` 上落空，其临时留档文件被删除，因此**迟归者无权改写**。

**已完成结果的校验**：worker 先写规范 JSON 留档（含 `run_id`、`input_sha256`、`result`）并把 `artifact_sha256`、`result_sha256` 与运行行一起条件提交。之后每次查看/复核/下载前，`artifacts.check_and_repair` 重算数据库结果哈希与规范留档字节：
- 一致 → 开放三个入口；
- 文件缺失、损坏、属于其他运行（串档）或哈希不符 → **关闭**查看/批准/下载，由数据库原件原子重写留档并重新校验，状态记为 `restored`，原因写入 `integrity_reason` 并在页面阶段条展示；
- 数据库结果哈希本身不符或写档失败 → 保持关闭（`bad`），页面显示原因；恢复能力恢复后自动重开。
已批准（有 review）的运行只恢复文件，**从不重算**；对 `completed` 运行调用重试返回 409。

**再次退出仍可收敛**：`relay`、`claim`、`outbox` 唯一约束均为幂等；恢复过程中再次中断只会增加 `attempt`，下一次补发继续推进到完成。

**验证**：`cd backend && python -m pytest tests -q`（本地 SQLite + 内存队列，覆盖 24 个用例）：
- `test_retry_and_notification.py`：通知丢失后补发、重复通知、失败后沿用原编号重试、已完成不重算；
- `test_leases_and_fencing.py`：租约内不被抢占、失联接管、迟归结果被丢弃、连续两次恢复、补发幂等；
- `test_archive_integrity.py`：缺失/损坏/串档恢复、数据库结果被改则关闭、写档失败保持关闭、已批准不重算；
- `test_interleavings.py`：通知丢失→写档失败→恢复的交错、已批准运行留档被替换、旧计算迟归；
- `test_concurrent_workers.py`：8 个 worker 并发消费 12 次运行、无重复计算；真实 `serve()` 循环在通知丢失后自动补发完成；
- `test_api_gates.py`：HTTP 层入口开合与阶段原因。
