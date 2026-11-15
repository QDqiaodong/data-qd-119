# 链路观测采集 v1

为运营观测者把分页 HTTP 观测源的一代（generation）链路状态事件采进独立 SQLite
采集库，并对指定版本仓库做链路解析与来源绑定。它回答“这一代观测到了哪些链路的
哪些 up/down 事件、该代来自哪个端点/版本/快照、最近一次完整代是什么”，**不是**
实时监控告警、指标聚合、链路建模或长时序分析平台。

## 输入

* `--db`：只读打开的 SQLite 版本仓库（与对账相同的只读 URI 方式，绝不写入，
  不在其旁边创建 WAL 边车文件）。
* `--revision`：不可变版本号；链路集合与端点取自该版快照，读入时重算快照
  SHA256 与版本记录比对。
* `--endpoint`：分页观测源的 `http://` 或 `https://` URL。
* `--collect`：SQLite 采集库（不存在则创建；存在暂存代则续采）。

## 源协议

* 首页：`GET <endpoint>`，不带任何参数。
* 续页：`GET <endpoint>?snapshot=<代号>&cursor=<游标>`，代号为首页响应的
  `snapshot`，游标为上一页响应的 `next`。
* 响应是 JSON 对象，**恰好**三个字段（严格 JSON：重复键、非有限数值拒绝；
  单页原始字节 ≤ 8 MB）：

| 字段 | 含义 |
| --- | --- |
| `snapshot` | 非空字符串（≤4096 字符），本代代号；同一代每页必须恒定 |
| `next` | `null` 表示末页；否则为下一页的非空游标字符串（≤4096 字符） |
| `events` | 事件数组，每页 ≤ 1000 条 |

* 事件恰好四个字段：`id`、`link_id`、`time`、`state`。
  - `id`/`link_id` 沿用网络契约标识规则（非空、≤128 字符、无控制字符、
    首尾无空白）；
  - `time` 必须带显式时区（`Z`、`+hh:mm`、`-hh:mm`，可选 1..9 位小数秒），
    统一换算为 UTC，规范文本为 `YYYY-MM-DDThh:mm:ss[.fraction]Z`；
  - `state` 仅 `up` 或 `down`；
  - `link_id` 必须存在于 `--revision` 指定版本的快照链路中。
* HTTP 非 2xx 即失败；其中 **410 Gone** 有特殊语义（见下）。

## 代语义

* **代号恒定**：首页的 `snapshot` 绑定本代；后续页代号不同即拒绝（已提交
  检查点保留）。
* **按规范值去重**：同一 `id` 重复出现且规范化内容（链路、UTC 时刻、状态）
  完全相同，只计一次并计入 `duplicates`；同一 `id` 内容不同即**冲突**，
  整页拒绝（同页内与跨页同样判定）。
* **游标环拒绝**：用于请求本代某页的游标再次出现（即服务端把采集带回
  已走过的位置）即拒绝。

## 事务边界与发布

* **逐页一个事务**：每页的事件、原始响应字节 SHA256、下一游标在同一个
  `BEGIN IMMEDIATE` … `COMMIT` 内落库（WAL + `synchronous=FULL`）。
  提交间断杀，重跑从最后一个已提交页的游标续采；提交中断杀，该页回滚，
  重跑重新抓取该页。恢复后的采集与一次性整采内容完全一致（测试逐行比对
  事件与页哈希）。
* **末页原子发布**：只有收到 `next=null` 的末页，才在**同一个事务**内把
  本代标记为 `published`。查询（`latest`/`events`）永远只返回**最近完整代**
  及其来源绑定（端点、revision、仓库快照 ID、源代号），采集中途读到的仍是
  上一个完整代。
* **失败保留**：校验失败、网络失败、游标环、代号变化等都保留已提交检查点
  与全部已发布旧代；修正后同参数重跑即续采。
* **HTTP 410 作废**：源对暂存代返回 410 时，暂存代的全部行在一个事务内
  删除（已发布旧代不受影响），退出码 4；下次运行从首页重新采集。
* **绑定**：暂存代绑定端点、revision 与仓库快照 ID；以不同参数续采一律
  拒绝。确需换参时用 `--restart` 显式丢弃暂存代（不影响已发布代）。

## 采集库结构

| 表 | 内容 |
| --- | --- |
| `meta` | `schema_version` |
| `runs` | 每代一行：端点、revision、仓库快照 ID、源代号、状态（`collecting`/`published`）、页数、收到/重复事件计数、起止时间；部分唯一索引保证至多一个暂存代 |
| `pages` | 每页一行：页序号、请求游标、原始页 SHA256、下一游标、事件数 |
| `events` | 事件明细：`(run_id, id)` 主键，链路、UTC 时刻（纪元秒+纳秒+规范文本）、状态 |

## 查询

* `latest`：输出最近完整代的单行 JSON（来源绑定与计数），无完整代时输出
  `null`。
* `events`：把最近完整代的事件按 `(UTC 时刻, id)` 顺序以 NDJSON（每行一个
  紧凑 JSON 事件）流式输出，不在内存积存。

## 资源边界

* 仅标准库；内存中至多保留一页（≤1000 事件、≤8 MB 原始字节），事件逐页
  流入 SQLite，去重用有界临时表上的集合 SQL 完成。
* 100 万事件（1000 页 × 1000）实测新增峰值内存约 31 MiB（上限 64 MiB，
  真实 CLI 子进程 `ru_maxrss` 测量），全程约 12 秒。

## CLI（`supply-flow-observe`，或 `python -m supply_flow.observe_cli`）

```sh
# 演示源（docs 示例，也可换任何符合源协议的端点）
python3 fixtures/observe_server.py 8765 &
PYTHONPATH=src python3 -m supply_flow.repo_cli init \
  --db run/net.db --source fixtures/reconcile_network.json
PYTHONPATH=src python3 -m supply_flow.observe_cli collect \
  --db run/net.db --revision 0 \
  --endpoint http://127.0.0.1:8765/feed --collect run/obs.db
PYTHONPATH=src python3 -m supply_flow.observe_cli latest --collect run/obs.db
PYTHONPATH=src python3 -m supply_flow.observe_cli events --collect run/obs.db
```

`collect` 成功时打印一行 JSON 摘要（含 `collect`、`run_id`、来源绑定、
`pages`、`received`、`events`、`duplicates`、`resumed`）。退出码：`0` 成功；
`2` 输入/校验/IO/状态错误；`4` HTTP 410 作废（暂存代已删除，重跑即重采）。

## Python API

```python
from supply_flow import observe, latest_generation, iter_events

summary = observe("run/net.db", 0, "http://127.0.0.1:8765/feed", "run/obs.db")
latest = latest_generation("run/obs.db")     # 无完整代时为 None
for event in iter_events("run/obs.db"):      # {"id","link_id","time","state"}
    ...
```

`observe(..., restart=True)` 等价于 CLI 的 `--restart`。异常：`ObserveError`
（含校验、绑定、游标环、冲突、网络失败），`InvalidatedError`（410，其子类）。

## 测试覆盖

`tests/test_observe.py` 用本地 HTTP 服务与真实进程覆盖：多页整采与来源绑定、
跨时区规范值去重、同页/跨页冲突、游标环、代号变化、提交前后 SIGTERM 断杀
（恢复与整采逐行等价）、HTTP 410 作废与重采、参数变更拒绝续采、采集中读取
最近完整代、百万事件峰值内存（子进程 `ru_maxrss`）。
