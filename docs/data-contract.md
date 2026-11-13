# 分块快照来源协议

position为来源连续日志位置，每项是完整事务或已确认marker。窗口(low,high)不包含marker本身，按提交位置过滤触及键。每事务变化按列表顺序，事务整体可见，源主键不可修改；upsert值为完整string，delete无value。扫描是非一致整块观测，但仅在low/high实际标记之间完成；每个键最多一行且落半开区间。

本材料L1=1/H1=3，tx2同时改键1、删键2并更新另一块键4；c1两条旧扫描行都应消除，tx4高位更新仍保留。c2在L2=5/H2=6间扫到new-d；不因为tx2属于另一块窗口就丢其镜像修改。来源结尾固定position6供逐行对照。缺位置或缺high保持未完成，不用最大位置代替连续进度。

块重扫需新low/high及新scan身份；旧候选不可重发。来源适配器可信地安排写marker和扫描，客户端不得直接改源表构造成功。

## 镜像侧实现（warehouse_data）

- `protocol.py`：记录形状校验、事务稳定摘要（位置+txid+全部变更）与协议异常。
- `source.py`：`FrozenSource` 按 sha256 核对冻结扫描资产；`LiveSource` 在同一单调日志上原子提交 marker/事务，扫描为调用瞬间的非一致观测。
- `state.py`：每次状态变更整包原子写（临时文件+`os.replace`+fsync）。保存流世代、流持久进度、每块窗口 low/high、扫描资产摘要、被消重键、持有事务与逐键来源。
- `engine.py`：分块窗口状态机。先写 low 并吃到该标记，再扫描落盘，再写 high 并吃到该标记；(low,high) 内触及本块键的已提交变更（含删除）使扫描行失效，未触及行仅在镜像无权威行时补入。变化键一律以事务流实际新状态为准，不比较扫描返回顺序。

### 必须保持的不变量

1. **提交顺序应用**：事务按 position 单调应用。前一事务仍在等待窗口时，后到事务不得越过它先应用，否则旧事务放行会覆盖更晚状态（见 `_release_held` 的 FIFO 闸门）。
2. **跨块事务整体可见**：一个事务改多个块时，在所有相关块窗口关闭前整体持有（随状态持久化），放行时一次性应用，摘要登记一次。
3. **幂等与阻塞**：同位置同摘要事务重放跳过；同位置不同内容抛 `DigestConflict` 人工阻塞，禁止猜测。
4. **窗口完整性**：缺位置（`StreamGap`）、流世代改变（`EpochChanged`）、high 未确认（`MarkerUnconfirmed`）、扫描中断或晚于 high（`LateScan`）都使该块未完成，不宣称基准完成。
5. **崩溃重放**：从已确认位置重放。high 未确认的旧窗口标记 `superseded`，旧扫描候选只留证不重发，换新 low/high 与新 scan 身份重开；high 已确认则复用同一窗口。
6. **删除路径唯一**：删除只依据明确流 delete 事件或完整基准代切换。块扫描为空不能凭空删除既有镜像行；初始镜像可能不全。
7. **完成判据**：所有计划块窗口关闭且无遗留持有事务，才递增基准代并置 `baseline_complete`；“扫过所有区间”不等于无漏入仓。

## 运行

无第三方依赖，Python 3.11+。数据为不可信输入，读取它的 Python 一律用 `-I`，并经 `tools/run_cli.py` 显式注入项目路径（`-I` 不含工作目录）：

```sh
python -I tools/run_cli.py snapshot data/normal.json --state-dir /tmp/mirror
python -I tools/run_cli.py verify   data/normal.json --state-dir /tmp/mirror
python -I tools/run_cli.py report   data/normal.json --state-dir /tmp/mirror
python -I tools/demo_live.py
python -I tools/demo_crash_recovery.py
python -I -c "import sys;sys.path.insert(0,'.');import unittest;\
 unittest.TextTestRunner().run(unittest.TestLoader().discover('tests'))"
```
