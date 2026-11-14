# data-qd-119 · 供需网络容量分配

Integer maximum-flow allocation with verifiable minimum cut。业务输入、计算和当前边界详见 [数据契约](docs/contract.md)。

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
