# 在线入仓的分块快照与变更窗消重

此项目实现生产表不停机情况下的 Python 入仓镜像：生产表太大无法一次锁全量导出，补数程序按不可变主键的半开区间分块，用来源提供的 low/high 水位标记协调非一致扫描与连续事务流，避免把扫描前的旧值覆盖回仓、也不漏掉扫描期间被删除的设备。设备、批次、组织、事件和成本均为合成，不连接真实工厂或生产仓库。

## 协议要点

每块先写 low marker 并等它在事务流中出现，再做非一致区间扫描（资产摘要立即落盘），扫完写 high marker 并等到该标记。窗口 `(low, high)` 不含 marker 本身：其间任何触及本块主键的已提交变更（含删除）都使该键的扫描行失效；未被触及的行仅在镜像无权威行时作为补数候选；变化键一律以事务流的实际新状态为准，不靠扫描返回顺序猜新旧。

事务提交后整体可见，一个事务可同时改多个块，镜像修改仍整体应用；同位置同摘要幂等、不同内容阻塞。流位置缺口、流世代改变、high 未确认、扫描中断或晚于 high 都使该块未完成。状态整包原子落盘，崩溃后从已确认位置重放，high 未确认的旧窗口换新 marker 重开、旧扫描只留证。删除只依据明确流事件，空扫描不删镜像行；所有计划块完成才可标基准补数完成。

## 模块

- `warehouse_data/protocol.py`：记录校验、事务摘要、协议异常。
- `warehouse_data/source.py`：来源适配器三能力——单调事务流、可确认 low/high marker、半开区间扫描；`FrozenSource` 面向带 sha256 身份的冻结资料，`LiveSource` 是不停机写入的内存动态源。
- `warehouse_data/state.py`：崩溃安全的原子状态（窗口、low/high、扫描摘要、消重键、持有事务、流进度、逐键来源）。
- `warehouse_data/engine.py`：分块窗口状态机、变更窗消重、跨块事务 FIFO 持有与原子应用、重放。
- `warehouse_data/report.py`：与来源固定结束位置快照逐行对照、逐键来源与窗口报告。
- `warehouse_data/io.py` / `domain.py` / `cli.py`：资料读取、身份核对与命令行。
- `tools/`：隔离模式启动器与动态源、崩溃恢复演示。
- [资料约定](docs/data-contract.md)定义字段、窗口语义与不变量。

## 运行

Python 3.11 以上，无第三方依赖。数据文件是不可信输入，读取它们的 Python 一律用 `-I`，由 `tools/run_cli.py` 显式注入项目根（`-I` 下 `sys.path` 不含工作目录）。项目根目录运行：

```sh
# 冻结资料：补数、逐行对照、来源报告
python -I tools/run_cli.py snapshot data/normal.json --state-dir /tmp/mirror
python -I tools/run_cli.py verify   data/normal.json --state-dir /tmp/mirror
python -I tools/run_cli.py report   data/normal.json --state-dir /tmp/mirror

# 动态来源四个展示点（low 前旧值、窗内 update/delete、high 后更新、跨块事务）
python -I tools/demo_live.py
# 扫描后 high 未确认崩溃 → 新窗口重开、旧扫描只留证
python -I tools/demo_crash_recovery.py

# 测试与编译检查
python -I -c "import sys;sys.path.insert(0,'.');import unittest;\
 unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().discover('tests'))"
python -m compileall -q warehouse_data tests tools
python -I tools/run_cli.py inspect data/normal.json
```

`inspect` 只读原始资料，不生成仓库新版本或计算领域结果。`invalid-config.json` 有基本形状错误；`missing-log-position.json` 缺流位置、`unconfirmed-high.json` 的 high marker 未确认，二者运行 `snapshot` 都以退出码 2 保持未完成，不会输出“补数完成”。null 与缺字段分别保留，有理数和 decimal 用字符串存储，不能先经 float 丢精度。

Dockerfile 固定 Linux 镜像，`docker build -t warehouse-material .` 构建；`docker run --rm warehouse-material` 显示摘要。所有资料本地可读，不需要账号、外部数据库或在线数据源。
