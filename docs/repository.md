# SQLite 版本仓库

在原有严格 JSON 入口与 `analyze` 分配能力之上，`supply_flow.repo` 提供单文件
SQLite 版本仓库：每次提交保存**不可变完整快照**与父版本，头指针（head）单调
前移。仅使用 Python 标准库（`sqlite3`、`json`、`hashlib`），无第三方依赖。

## 数据模型

| 表 | 作用 |
| --- | --- |
| `meta` | 单行头指针 `head_revision`、`schema_version`、init 源文件 SHA256 |
| `snapshots` | 内容寻址：`snapshot_id`（规范化内容 SHA256）→ 完整网络 JSON |
| `revisions` | 递增版本号、`parent_revision`、绑定的 `snapshot_id`、`batch_id`、请求哈希 |
| `batches` | `batch_id` 唯一登记：请求规范化哈希与产出版本，用于重放/冲突判定 |

版本 0 由 `init` 从现有严格 JSON 导入（`batch_id='__init__'`，
`parent_revision=-1`）。每个成功批次令版本号 +1；内容相同的版本共享同一
snapshot 行（快照去重），但版本号仍然递增，父链完整保留。

## 事务边界

* 所有写操作（`init`、`apply`）在**单连接**的一个 `BEGIN IMMEDIATE` …
  `COMMIT` 内完成全部写入：revision 行、snapshot 行、batch 登记与 head 更新
  同时可见或全部不可见。
* 连接以 `isolation_level=None` 打开，事务完全显式管理；任何异常都走
  `ROLLBACK`，不会残留打开的写事务。
* `PRAGMA journal_mode=WAL` + `synchronous=FULL` + `busy_timeout=30000`：
  读者不阻塞写者、写者不阻塞读者；`COMMIT` 前进程被 SIGTERM 杀死或断电时，
  SQLite 回滚日志保证下一个连接打开时自动恢复，head 与历史均无半写状态。
* 乐观并发控制以当前 head 为准：两个进程从同一 head 提交不同批次，先提交者
  成功，后提交者在同一写事务内读到新 head，`base_revision` 不匹配，整批回滚
  并抛 `ConflictError`（CLI 退出码 3）。不存在两个赢家。
* 校验（`analyze` 最终网络校验）在事务内、写库前完成；校验失败即回滚，
  不会产生 revision/batch 残留行。

## 内容规范化与快照 ID

快照 ID 是完整网络对象 `{nodes, links, supplies, demands}` 规范化字节的
SHA256。规范化规则：

1. 对象键按固定顺序输出，序列化使用
   `json.dumps(..., sort_keys=True, ensure_ascii=False, separators=(",",":"),
   allow_nan=False)`，UTF-8 编码；
2. 所有数组先按稳定键排序：nodes 按节点 id、links 按 link id、
   supplies/demands 按 node；行对象使用固定键顺序；
3. 因此输入数组任意重排不改变快照 ID；任一业务字段改变才改变 ID。

批次重放判定对**请求体**做同类规范化（仅八个 upsert/delete 列表参与，
`base_revision` 与 `batch_id` 不参与）：标量列表排序、link 行按 id 排序、
supply/demand 行按 node 排序后哈希。重放判定先于 base 版本检查。

## 批次语义与校验顺序

`apply` 接收 `base_revision`、`batch_id` 及八个可选列表：
`upsert_nodes`/`delete_nodes`、`upsert_links`/`delete_links`、
`upsert_supplies`/`delete_supplies`、`upsert_demands`/`delete_demands`。

* 同一批次内**先删后建**：同一 id 同时出现在 delete 与 upsert 列表表示原子
  替换；中间状态从不对外可见。单个列表内 id 重复直接拒绝。
* 整批先应用到内存副本，**只对最终网络**执行 `analyze` 严格校验：允许同批
  先引用后建节点（link/supply/demand 可引用批次后段才创建的节点）；最终仍
  悬空的引用拒绝整批。
* 删除**不级联**：删节点不会自动删其链路或供需行，残留引用导致最终校验失败。
* 删不存在的 id、自环、`__` 前缀保留名、越界整数（沿用 0..10^9 契约）等
  在写库前拒绝。

事务内判定顺序固定为：

1. `batch_id` 重放/冲突（最高优先级，base 过期也先判重放）；
2. 乐观并发：`base_revision` 必须等于当前 head；
3. 整批应用与最终网络校验；
4. 版本递增、不可变快照写入、head 更新并提交。

### 重放与冲突

* 相同 `batch_id` + 相同规范化请求：无论 base 是否过期，返回原版本，响应带
  `"replayed": true`，不产生新版本；
* 相同 `batch_id` + 不同请求体：抛 `ConflictError`，即使 base 已过期也优先
  于并发冲突报告；
* 新 `batch_id` 但 base 不等于 head：抛 `ConflictError`。

## 按版本分析

`analyze_revision(revision, allocation='max-flow'|'fair')` 读取该 revision 绑定
的**不可变快照**，校验内容哈希与 revision 行上的 `snapshot_id` 一致后再调用
原有 `analyze`。结果绑定快照 ID 而非移动中的 head：head 后续前移不改变任何
历史版本的分析结果。两种分配模式与原 JSON 入口完全一致。

`export(revision)` 输出可直接被原 `supply-flow` CLI 与 `analyze()` 读取的
裸网络 JSON。

## CLI（`supply-flow-repo`，或 `python -m supply_flow.repo_cli`）

```sh
supply-flow-repo init    --db run/net.db --source fixtures/input.json
supply-flow-repo apply   --db run/net.db --request batch.json
supply-flow-repo export  --db run/net.db --revision 0 --output run/rev0.json
supply-flow-repo analyze --db run/net.db --revision 0 [--allocation fair] [--output run/a.json]
supply-flow-repo head    --db run/net.db
supply-flow-repo history --db run/net.db
```

`batch.json` 示例：

```json
{
  "base_revision": 0,
  "batch_id": "2026-11-add-north",
  "upsert_nodes": ["north"],
  "upsert_links": [{"id": "LN", "from": "pump", "to": "north", "capacity": 3}],
  "upsert_demands": [{"node": "north", "amount": 3}],
  "delete_links": [],
  "delete_nodes": [],
  "delete_supplies": [],
  "delete_demands": []
}
```

退出码：`0` 成功；`2` 输入/校验/IO 错误；`3` 并发或 batch_id 冲突。
请求文件沿用严格 JSON 契约（8 MB 上限、重复键拒绝、NaN/Infinity 拒绝）。
`analyze --output` 与旧 CLI 一样走临时文件 + 原子替换，发布失败保留旧报告，
输出路径拒绝符号链接且不能与数据库同路径。

## Python API

```python
from supply_flow import (
    Repository, init_repository, apply_batch_file,
    export_revision, analyze_at_revision, read_head, read_history,
    snapshot_id, RepoError, ConflictError,
)

with Repository("run/net.db") as repo:
    repo.init("fixtures/input.json")
    info = repo.apply({"base_revision": 0, "batch_id": "b1",
                       "upsert_nodes": ["north"]})
    network = repo.export(info["revision"])
    report = repo.analyze_revision(info["revision"], allocation="fair")
```

模块级便捷函数（`init_repository` 等）每次调用开关一个连接，适合短生命周期
脚本；长生命周期读者应持有一个 `Repository` 实例——WAL 下该连接的读快照在
head 提交期间保持有效，可持续读取历史版本。

## 测试覆盖

`tests/test_repo.py` 与 `tests/test_repo_cli.py` 包含真实多进程用例
（`multiprocessing` spawn 独立连接，非线程/模拟）：

* 同批交换节点/边的原子批与同 id 删后重建；
* 双进程从同一 head 竞争提交，多轮重复，始终只有一个赢家；
* 提交窗口内 SIGTERM 断杀，head/历史/snapshot/batch 无半写，库自动恢复且
  后续提交成功（另有 8 次连续断杀压力脚本验证）；
* 跨进程幂等重放（过期 base 仍返回原版本）；
* head 更新期间长连接持续读取历史快照；
* 导出文件经原 `supply-flow` JSON 入口分析，发布失败保留旧报告。
