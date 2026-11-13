"""崩溃恢复演示：窗口在 high 未确认时中断，重开新窗口且旧扫描只留证。

阶段一：写 low、做扫描后立即注入故障（high 尚未确认），状态已落盘。
阶段二：生产继续提交窗内事务与新 marker；新进程从已确认位置重放，
        旧窗口标记 superseded（证据保留、候选不再重发），换新 low/high 重开完成补数。
用 `python -I` 运行。
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from warehouse_data.engine import SnapshotEngine  # noqa: E402
from warehouse_data.protocol import ProtocolError  # noqa: E402
from warehouse_data.report import diff_against_reference, provenance_report  # noqa: E402
from warehouse_data.source import LiveSource  # noqa: E402
from warehouse_data.state import StateStore  # noqa: E402

CHUNKS = [
    {"id": "c1", "range": [0, 3], "low": "L1", "high": "H1"},
    {"id": "c2", "range": [3, 6], "low": "L2", "high": "H2"},
]


def main() -> None:
    src = LiveSource({1: "old-a", 2: "old-b", 4: "old-d"})
    src.commit_marker("L1")
    with tempfile.TemporaryDirectory() as d:
        store = StateStore(Path(d) / "mirror-state.json")

        print("== 阶段一：扫描后、high 确认前崩溃 ==")
        state = store.init_or_load(src.epoch, src.initial_position, [])
        try:
            SnapshotEngine(src, store, state, mutable_scan=True,
                           failpoint="after_scan").run_backfill(CHUNKS)
        except ProtocolError as e:
            print(f"进程终止：{e}")
        disk = json.loads((Path(d) / "mirror-state.json").read_text(encoding="utf-8"))
        old = disk["chunks"]["c1"]["windows"][0]
        print(f"旧窗口 {old['window_id']}：scan_digest 已留证={bool(old['scan_digest'])}，"
              f"high_pos={old['high_pos']}，closed={old['closed']}")

        print("\n== 生产继续写入（不停止）==")
        src.commit_transaction("tx2", [
            {"key": 1, "op": "upsert", "value": "new-a"},
            {"key": 2, "op": "delete"},
            {"key": 4, "op": "upsert", "value": "new-d"},
        ])
        src.commit_marker("H1")
        src.commit_transaction("tx4", [{"key": 1, "op": "upsert", "value": "after-high"}])
        src.commit_marker("L2")
        src.commit_marker("H2")

        print("\n== 阶段二：新进程从已确认位置重放 ==")
        state2 = store.init_or_load(src.epoch, src.initial_position, [])
        SnapshotEngine(src, store, state2, mutable_scan=True).run_backfill(CHUNKS)
        windows = state2["chunks"]["c1"]["windows"]
        print(f"c1 窗口数={len(windows)}；旧窗口 superseded={windows[0]['superseded']}，"
              f"新窗口 closed={windows[1]['closed']}，新 scan_id 不同="
              f"{windows[0]['scan_id'] != windows[1]['scan_id']}")

        reference = [{"key": k, "value": v} for k, v in sorted(src._table.items())]
        diff = diff_against_reference(state2, reference)
        print("\n== 逐行对照 ==")
        print(json.dumps(diff, ensure_ascii=False, indent=2))
        for row in provenance_report(state2):
            print(f"key={row['key']:<2} {row['status']:<7} value={row['value']!s:<11} <- {row['origin']}")
        if not diff["matches"]:
            raise SystemExit("恢复后镜像与来源结束快照不一致")
        assert windows[0]["superseded"] and windows[1]["closed"]
        print("\n崩溃恢复成功：旧扫描只留证，新窗口完成，逐行对照一致。")


if __name__ == "__main__":
    main()
