"""动态来源端到端演示：生产写入不停止的分块增量快照。

展示题目点名的四个关键点，全部在同一份连续事务流上：
  1. low 之前的旧值（old-a）被扫描看到，但因窗内变更而消重，绝不回灌；
  2. 窗口 (low, high) 内的 update 与 delete 都以事务流新状态为准（含删除设备）；
  3. high 之后到达的更新（after-high）不能被晚于它的扫描/旧窗口覆盖；
  4. 一个事务（tx2）同时改两个块，镜像修改仍作为一个整体只应用一次。

最后与来源在固定结束位置的表快照逐行对照。用 `python -I` 运行。
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from warehouse_data.engine import SnapshotEngine  # noqa: E402
from warehouse_data.report import diff_against_reference, provenance_report  # noqa: E402
from warehouse_data.source import LiveSource  # noqa: E402
from warehouse_data.state import StateStore  # noqa: E402

CHUNKS = [
    {"id": "c1", "range": [0, 3], "low": "L1", "high": "H1"},
    {"id": "c2", "range": [3, 6], "low": "L2", "high": "H2"},
]


def build_live_source() -> LiveSource:
    # 生产表持续被写入；扫描只能拿到调用瞬间的非一致观测。
    src = LiveSource({1: "old-a", 2: "old-b", 4: "old-d"})
    src.commit_marker("L1")                                   # 位置1
    src.commit_transaction("tx2", [                           # 位置2：跨 c1、c2 的整体事务
        {"key": 1, "op": "upsert", "value": "new-a"},
        {"key": 2, "op": "delete"},
        {"key": 4, "op": "upsert", "value": "new-d"},
    ])
    src.commit_marker("H1")                                   # 位置3
    src.commit_transaction("tx4", [                           # 位置4：high 之后的更新
        {"key": 1, "op": "upsert", "value": "after-high"},
    ])
    src.commit_marker("L2")                                   # 位置5
    src.commit_marker("H2")                                   # 位置6
    return src


def main() -> None:
    src = build_live_source()
    with tempfile.TemporaryDirectory() as d:
        store = StateStore(Path(d) / "mirror-state.json")
        state = store.init_or_load(src.epoch, src.initial_position, [])
        # 动态源扫描内容可变：崩溃重放只留旧扫描证据，不重新当作候选。
        SnapshotEngine(src, store, state, mutable_scan=True).run_backfill(CHUNKS)

        reference = [{"key": k, "value": v} for k, v in sorted(src._table.items())]
        diff = diff_against_reference(state, reference)
        print("== 来源固定结束位置快照 ==")
        print(json.dumps(reference, ensure_ascii=False))
        print("\n== 镜像逐键来源 ==")
        for row in provenance_report(state):
            print(f"key={row['key']:<2} {row['status']:<7} value={row['value']!s:<11} <- {row['origin']}")
        print("\n== 逐行对照 ==")
        print(json.dumps(diff, ensure_ascii=False, indent=2))
        if not diff["matches"]:
            raise SystemExit("镜像与来源结束快照不一致")

        # 断言四个展示点。
        prov = {p["key"]: p for p in provenance_report(state)}
        assert prov[1]["value"] == "after-high" and "tx4" in prov[1]["origin"], "high 后更新被覆盖"
        assert prov[2]["status"] == "deleted" and "tx2" in prov[2]["origin"], "窗内删除丢失"
        assert prov[4]["value"] == "new-d" and "tx2" in prov[4]["origin"], "跨块事务未整体应用"
        applied = [e for e in state["journal"] if e["event"] == "txn-applied"]
        assert [(e["txid"], e["changes"]) for e in applied] == [("tx2", 3), ("tx4", 1)], applied
        assert "old-a" not in json.dumps(state), "low 前旧值被错误回灌"
        print("\n四个展示点全部满足，逐行对照一致。")


if __name__ == "__main__":
    main()
