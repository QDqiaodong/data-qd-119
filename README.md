# data-qd-119 · 供需网络容量分配

Integer maximum-flow allocation with verifiable minimum cut。业务输入、计算和当前边界详见 [数据契约](docs/contract.md)；运输交接对账见 [对账契约](docs/reconciliation.md)。

## 安装与演示

Python >=3.11，运行和测试仅用标准库；先确认 `python3 --version`。
每仓建立独立环境，构建工具锁版本。首次安装无公共缓存时可能联网；业务运行不访问网络。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m pip check
.venv/bin/supply-flow --source fixtures/input.json --output run/report.json
.venv/bin/python -m unittest discover -s tests -v
```

本地已有wheel时，安装构建工具可加 `--no-index --find-links <公共wheel缓存目录>`。
修改源码后重新安装。本仓不共享其他仓环境，不依赖Docker、外部账号或持续服务。

输入JSON最多8MB，拒绝重复键、非有限数值和未定义字段；源字节SHA256随结果保留。
输出是可独立读取的JSON；成功后原子替换，输入/计算失败退出2并保留旧报告。
输入和输出不得相同，输出符号链接拒绝。单操作者批处理，不承诺并发发布或断电恢复。
同输入重跑不累加。测试使用临时目录，运行产物在忽略的run/。

src/supply_flow/model.py 为业务模型，相关算法模块独立组织；common.py 提供受限JSON及报告发布，
cli.py 衔接数据链路。fixtures 为明确自制样例，代码与样例采用MIT许可。

## 两种分配模式

`--allocation max-flow`（缺省）保留原逻辑：只最大化总服务量。
`--allocation fair` 在同一整数容量边界内按三级词典序目标分配：

1. 先最大化总服务量（与缺省模式的最优总量相同）；
2. 再最大化所有正需求节点的最小满足率 served/amount（精确分数，零需求不参与，
   无正需求时比率定义为 1）；
3. 仍并列时，按需求节点名升序把服务向量词典序最大化。

实现仅用带上下界的整数可行流（超级源/汇环流归约）与同一个 Edmonds-Karp，加精确整数二分；
不逐单位枚举容量，不引入第三方优化器，100 节点 / 500 边 / 10^9 容量上限内可处理。
公平报告额外含 `allocation_mode` 与约分后的 `min_service_ratio.numerator/denominator`，
原流量、守恒校验与最小割证书字段全部保留；同输入确定，数组重排不影响结果。

```sh
# 缺省模式：总服务 4，a=4、b=0（b 被饿死）
.venv/bin/supply-flow --source fixtures/fair_demo.json --output run/plain.json
# 公平模式：总服务仍为 4，但 a=2、b=2，最小满足率精确为 1/2
.venv/bin/supply-flow --source fixtures/fair_demo.json --output run/fair.json --allocation fair
```

Python API 同样可用：

```python
import json
from supply_flow import analyze

data = json.load(open('fixtures/fair_demo.json'))
plain = analyze(data)                       # 等同 allocation='max-flow'
fair = analyze(data, allocation='fair')
assert plain['total_served'] == fair['total_served'] == 4
assert fair['demand_served'] == {'a': 2, 'b': 2}
assert fair['min_service_ratio'] == {'numerator': 1, 'denominator': 2}
```

原始样例（fixtures/input.json）供给10、需求9，L1 瓶颈下最大服务7：
max-flow 为 east=4、west=3；fair 重排为 east=3、west=4，最小满足率 3/4，
总量与最小割容量 7 均不变，证明公平目标改变分配但不损失最大总量。
network.py实现独立反向弧的整数增广与带下界可行流，model.py建立供需网络并核验容量/守恒/割证书。

## SQLite 版本仓库

`supply_flow.repo` 在同一标准库约束下提供单文件 SQLite 版本仓库，设计与事务
边界详见 [版本仓库文档](docs/repository.md)。要点：

- `init` 导入现有严格 JSON 为不可变版本 0；`apply` 接收 `base_revision`、
  `batch_id` 与节点/链路/供需的 upsert/delete 列表，整批应用后只校验最终
  网络：允许同批先引用后建节点，拒绝最终悬空引用，删除不级联。
- 版本号递增，每版保存不可变完整快照与父版本；快照 ID 为规范化内容 SHA256，
  数组重排不改 ID，同内容快照去重但版本仍递增。
- 当前 head 作乐观并发控制：两个进程从同一 head 提交不同批次仅一个成功；
  全部写入在单个 `BEGIN IMMEDIATE` 事务内，提交前断杀或失败均无半写。
- 相同 `batch_id` 与规范化请求重放返回原版本（base 过期也先判重放）；
  同 ID 不同请求报冲突。
- 历史版本可 `export` 为原 analyze 可读 JSON；按版本分析绑定该快照 ID 而非
  变动中的 head，支持 max-flow 与 fair 两种分配。

```sh
PYTHONPATH=src python3 -m supply_flow.repo_cli init   --db run/net.db --source fixtures/input.json
PYTHONPATH=src python3 -m supply_flow.repo_cli apply  --db run/net.db --request batch.json
PYTHONPATH=src python3 -m supply_flow.repo_cli export --db run/net.db --revision 0 --output run/rev0.json
PYTHONPATH=src python3 -m supply_flow.repo_cli analyze --db run/net.db --revision 0 --allocation fair
```

安装后控制台脚本为 `supply-flow-repo`。退出码：0 成功、2 输入/校验错误、
3 并发或 batch_id 冲突。真实多进程测试（竞争提交、断杀、幂等重放、head
更新期间读取历史）见 tests/test_repo.py 与 tests/test_repo_cli.py。

## 运输交接对账

`supply_flow.reconcile` 在同一个只读版本仓库上追加对账能力：读取
`--revision` 指定的不可变快照取链路端点，把发运与签收两个
`id,link_id,time,quantity` CSV 合并成独立 SQLite 对账库，不写源仓库。
完整规则见 [对账契约](docs/reconciliation.md)。要点：

- 时间必须带显式时区并统一为 UTC（小数秒保留到纳秒），任意行序与任意时区
  混合都得到同一结果。
- 每张表按 `id` 去重：内容相同的重复只计一次，内容冲突报文件名、冲突行号与
  首次出现行号，且不发布任何新结果。
- 每条链路按 `(UTC 时刻, id)` 处理，同刻先发运后签收；签收按 FIFO 消耗此前
  未尽发运、允许拆分；无处可抵的签收量记孤儿（`early_arrival`/`over_receipt`），
  永不借用未来发运。
- 输出来自结果库自身的匹配明细、未签余额、孤儿量和节点收发汇总，并绑定
  `revision`、`snapshot_id`（读入时重算校验）与两源文件 SHA256。
- 200 万行输入新增峰值内存 ≤ 64 MiB；中间态与明细全部落临时磁盘，签收按
  发运段推进，不逐单位匹配、不重扫全部发运。输出先写临时文件，校验通过后
  原子替换，失败保留旧结果。

```sh
PYTHONPATH=src python3 -m supply_flow.repo_cli init --db run/net.db \
  --source fixtures/reconcile_network.json
PYTHONPATH=src python3 -m supply_flow.reconcile_cli \
  --db run/net.db --revision 0 \
  --shipments fixtures/reconcile_shipments.csv \
  --receipts fixtures/reconcile_receipts.csv --output run/reconcile.db
python3 -c "import sqlite3;print(sqlite3.connect('run/reconcile.db').execute(
  'SELECT revision,snapshot_id,matched_quantity,orphan_quantity FROM reconciliation').fetchall())"
```

样例中 depot→hub 的发运 15、签收 15 全部匹配，hub→port 的早到签收 1 记孤儿
且不借用其后的发运，port→yard 的签收 4 抵 2 后余 2 记超收孤儿；未签余额为
hub→port 的 6。安装后控制台脚本为 `supply-flow-reconcile`；退出码 0 成功、
2 输入/校验/IO 错误。真实 CLI 测试覆盖跨时区同刻、拆收、早到、重复冲突与
百万级积压，峰值内存由子进程 `ru_maxrss` 实测，见 tests/test_reconcile.py。
