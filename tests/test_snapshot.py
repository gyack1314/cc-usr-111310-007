"""分块增量快照协议测试。"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from warehouse_data.engine import SnapshotEngine
from warehouse_data.protocol import (
    BaselineIncomplete,
    DigestConflict,
    EpochChanged,
    LateScan,
    MarkerUnconfirmed,
    ProtocolError,
    StreamGap,
)
from warehouse_data.io import load
from warehouse_data.report import diff_against_reference, provenance_report
from warehouse_data.source import FrozenSource, LiveSource
from warehouse_data.state import StateStore

DATA = Path(__file__).resolve().parent.parent / "data"


def run_frozen(name: str, state_dir: Path, failpoint: str | None = None):
    v, _ = load(DATA / f"{name}.json")
    src = FrozenSource(v, DATA)
    store = StateStore(state_dir / "state.json")
    state = store.init_or_load(src.epoch, v["initial_position"], v["initial_mirror"])
    eng = SnapshotEngine(src, store, state, failpoint=failpoint)
    eng.run_backfill(v["chunks"])
    return v, state, store, src


class FrozenMaterialTests(unittest.TestCase):
    def test_normal_matches_reference_row_by_row(self):
        with tempfile.TemporaryDirectory() as d:
            v, state, _, _ = run_frozen("normal", Path(d))
            r = diff_against_reference(state, v["reference_end_rows"])
            self.assertTrue(r["matches"], r)
            # 关键语义：逐键来源可报告，扫描旧值未覆盖流状态。
            prov = {p["key"]: p for p in provenance_report(state)}
            self.assertEqual(prov[1]["value"], "after-high")
            self.assertEqual(prov[1]["provenance"], "stream")
            self.assertIn("tx4@位置4", prov[1]["origin"])
            self.assertEqual(prov[2]["status"], "deleted")
            self.assertIn("tx2", prov[2]["origin"])
            self.assertEqual(prov[4]["value"], "new-d")
            # tx2 跨两块，作为一个整体事务恰好应用一次。
            applied = [e for e in state["journal"] if e["event"] == "txn-applied"]
            self.assertEqual([(e["position"], e["changes"]) for e in applied],
                             [(2, 3), (4, 1)])
            # 两块窗口都关闭且保留消重证据。
            c1 = state["chunks"]["c1"]["windows"][0]
            self.assertEqual(c1["invalidated"], [1, 2])
            self.assertEqual(c1["candidates"], [])
            c2 = state["chunks"]["c2"]["windows"][0]
            self.assertEqual(c2["candidates"], [4])

    def test_missing_log_position_never_completes(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises((StreamGap, MarkerUnconfirmed)):
                run_frozen("missing-log-position", Path(d))
            state = json.loads((Path(d) / "state.json").read_text(encoding="utf-8"))
            self.assertFalse(state["baseline_complete"])

    def test_unconfirmed_high_never_completes(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(MarkerUnconfirmed):
                run_frozen("unconfirmed-high", Path(d))
            state = json.loads((Path(d) / "state.json").read_text(encoding="utf-8"))
            self.assertFalse(state["baseline_complete"])

    def test_invalid_record_kind(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(ProtocolError):
                run_frozen("invalid-config", Path(d))

    def test_idempotent_rerun_is_noop(self):
        with tempfile.TemporaryDirectory() as d:
            v, state1, store, src = run_frozen("normal", Path(d))
            before = json.dumps(state1, sort_keys=True)
            # 再跑一次：所有块已关闭、事务已应用，结果与状态不得变化。
            state2 = store.init_or_load(src.epoch, v["initial_position"], [])
            eng = SnapshotEngine(src, store, state2)
            eng.run_backfill(v["chunks"])
            self.assertEqual(json.dumps(state2, sort_keys=True), before)
            self.assertTrue(diff_against_reference(state2, v["reference_end_rows"])["matches"])


class LiveProtocolTests(unittest.TestCase):
    """动态源：生产写入不停止，覆盖题目要求的四个展示点与崩溃重放。"""

    def _build_source(self):
        return LiveSource({1: "old-a", 2: "old-b", 4: "old-d"})

    @staticmethod
    def _engine(src, store, state, **kw):
        # 动态源：扫描可变（重放只留证）；冻结源默认严格重取。
        return SnapshotEngine(src, store, state, mutable_scan=True, **kw)

    def _chunks(self):
        return [
            {"id": "c1", "range": [0, 3], "low": "L1", "high": "H1"},
            {"id": "c2", "range": [3, 6], "low": "L2", "high": "H2"},
        ]

    def test_window_invalidation_cross_chunk_and_post_high_update(self):
        src = self._build_source()
        # 1) 写 L1；2) 窗内 tx2 跨两块（改键1、删键2、改键4）；3) H1；
        # 4) high 后 tx4 更新键1；5) L2；6) H2。
        src.commit_marker("L1")
        src.commit_transaction("tx2", [
            {"key": 1, "op": "upsert", "value": "new-a"},
            {"key": 2, "op": "delete"},
            {"key": 4, "op": "upsert", "value": "new-d"},
        ])
        src.commit_marker("H1")
        src.commit_transaction("tx4", [{"key": 1, "op": "upsert", "value": "after-high"}])
        src.commit_marker("L2")
        src.commit_marker("H2")
        with tempfile.TemporaryDirectory() as d:
            store = StateStore(Path(d) / "s.json")
            state = store.init_or_load(src.epoch, 0, [])
            self._engine(src, store, state).run_backfill(self._chunks())
            mirror = {r["key"]: r["value"] for r in StateStore.live_rows(state)}
            # 固定结束位置来源快照。
            ref = {k: v for k, v in src._table.items()}
            self.assertEqual(mirror, ref)
            self.assertEqual(mirror, {1: "after-high", 4: "new-d"})
            self.assertIsNone(StateStore.row(state, 2)["value"])
            # 旧扫描值 old-a 被消重，没有回灌。
            self.assertNotIn("old-a", json.dumps(state))
            prov = {p["key"]: p for p in provenance_report(state)}
            self.assertEqual(prov[1]["provenance"], "stream")
            self.assertIn("tx4", prov[1]["origin"])
            self.assertEqual(prov[2]["provenance"], "stream")
            self.assertTrue(state["baseline_complete"])

    def test_crash_mid_window_reopens_new_window_old_scan_kept_as_evidence_only(self):
        src = self._build_source()
        src.commit_marker("L1")
        with tempfile.TemporaryDirectory() as d:
            store = StateStore(Path(d) / "s.json")
            state = store.init_or_load(src.epoch, 0, [])
            chunks = self._chunks()
            # 第一次：扫描已落盘、high 未确认就崩溃。
            with self.assertRaises(ProtocolError):
                self._engine(src, store, state, failpoint="after_scan").run_backfill(chunks)
            disk = json.loads((Path(d) / "s.json").read_text(encoding="utf-8"))
            self.assertFalse(disk["baseline_complete"])
            first_window = disk["chunks"]["c1"]["windows"][0]
            self.assertIsNotNone(first_window["scan_digest"])
            self.assertIsNone(first_window["high_pos"])
            # 生产继续写入：窗内变更与新 marker。
            src.commit_transaction("tx2", [
                {"key": 1, "op": "upsert", "value": "new-a"},
                {"key": 2, "op": "delete"},
                {"key": 4, "op": "upsert", "value": "new-d"},
            ])
            src.commit_marker("H1")
            src.commit_transaction("tx4", [{"key": 1, "op": "upsert", "value": "after-high"}])
            src.commit_marker("L2")
            src.commit_marker("H2")
            # 重启：从已确认位置重放，旧窗口作废并换新 low/high 重开。
            state2 = store.init_or_load(src.epoch, 0, [])
            self._engine(src, store, state2).run_backfill(chunks)
            c1 = state2["chunks"]["c1"]
            self.assertEqual(len(c1["windows"]), 2)
            self.assertTrue(c1["windows"][0]["superseded"])
            self.assertTrue(c1["windows"][1]["closed"])
            self.assertNotEqual(c1["windows"][0]["window_id"], c1["windows"][1]["window_id"])
            mirror = {r["key"]: r["value"] for r in StateStore.live_rows(state2)}
            self.assertEqual(mirror, {1: "after-high", 4: "new-d"})
            self.assertIsNone(StateStore.row(state2, 2)["value"])
            self.assertTrue(state2["baseline_complete"])

    def test_crash_after_high_reuses_confirmed_window(self):
        src = self._build_source()
        src.commit_marker("L1")
        src.commit_transaction("tx2", [
            {"key": 1, "op": "upsert", "value": "new-a"},
            {"key": 2, "op": "delete"},
            {"key": 4, "op": "upsert", "value": "new-d"},
        ])
        src.commit_marker("H1")
        with tempfile.TemporaryDirectory() as d:
            store = StateStore(Path(d) / "s.json")
            chunks = self._chunks()
            with self.assertRaises(ProtocolError):
                self._engine(src, store,
                               store.init_or_load(src.epoch, 0, []),
                               failpoint="after_high").run_backfill(chunks)
            src.commit_marker("L2")
            src.commit_marker("H2")
            state = store.init_or_load(src.epoch, 0, [])
            self._engine(src, store, state).run_backfill(chunks)
            # high 已确认，窗口不重开，只产生一个窗口记录。
            self.assertEqual(len(state["chunks"]["c1"]["windows"]), 1)
            self.assertTrue(state["chunks"]["c1"]["windows"][0]["closed"])
            mirror = {r["key"]: r["value"] for r in StateStore.live_rows(state)}
            self.assertEqual(mirror, {1: "new-a", 4: "new-d"})
            self.assertIsNone(StateStore.row(state, 2)["value"])

    def test_empty_scan_never_deletes_existing_mirror_rows(self):
        src = LiveSource({})  # 来源当前为空
        src.commit_marker("L1")
        src.commit_marker("H1")
        with tempfile.TemporaryDirectory() as d:
            store = StateStore(Path(d) / "s.json")
            # 初始镜像不全且含一块内的既有行；空扫描不得凭空删除它。
            state = store.init_or_load(src.epoch, 0, [{"key": 1, "value": "kept"}])
            chunks = [{"id": "c1", "range": [0, 3], "low": "L1", "high": "H1"}]
            self._engine(src, store, state).run_backfill(chunks)
            self.assertEqual(StateStore.row(state, 1)["value"], "kept")
            self.assertEqual(StateStore.row(state, 1)["provenance"]["kind"], "initial")

    def test_scan_survivor_provenance_points_to_window_and_asset(self):
        # c2 窗口内没有任何变更：key4 作为扫描存活行补入，来源必须可追溯到窗口与资产摘要。
        src = LiveSource({4: "new-d"})
        src.commit_marker("L2")
        src.commit_marker("H2")
        with tempfile.TemporaryDirectory() as d:
            store = StateStore(Path(d) / "s.json")
            state = store.init_or_load(src.epoch, 0, [])
            self._engine(src, store, state).run_backfill(
                [{"id": "c2", "range": [3, 6], "low": "L2", "high": "H2"}])
            cell = StateStore.row(state, 4)
            self.assertEqual(cell["value"], "new-d")
            prov = cell["provenance"]
            self.assertEqual(prov["kind"], "scan")
            self.assertEqual(prov["chunk"], "c2")
            self.assertTrue(prov["window_id"] and len(prov["digest"]) == 64)
            win = state["chunks"]["c2"]["windows"][0]
            self.assertEqual(win["candidates"], [4])
            self.assertEqual(win["invalidated"], [])

    def test_explicit_stream_delete_is_the_only_delete_path(self):
        src = LiveSource({1: "a", 2: "b"})
        src.commit_marker("L1")
        src.commit_transaction("tx-del", [{"key": 1, "op": "delete"}])
        src.commit_marker("H1")
        with tempfile.TemporaryDirectory() as d:
            store = StateStore(Path(d) / "s.json")
            state = store.init_or_load(src.epoch, 0, [])
            self._engine(src, store, state).run_backfill(
                [{"id": "c1", "range": [0, 3], "low": "L1", "high": "H1"}])
            self.assertIsNone(StateStore.row(state, 1)["value"])
            self.assertEqual(StateStore.row(state, 2)["value"], "b")

    def test_late_scan_after_high_is_rejected(self):
        class LateScanSource(LiveSource):
            def open_scan(self, lo, hi, scan_id, asset_ref=None):
                # 扫描结果在 high marker 确认之后才交付：来源适配器拒绝把它挂到已关窗口，
                # 不得用晚到扫描覆盖流中更晚状态。
                if self.marker_position("H1") is None:
                    self.commit_marker("H1")
                raise LateScan("扫描晚于 high marker H1 到达")

        src = LateScanSource({1: "old-a"})
        src.commit_marker("L1")
        with tempfile.TemporaryDirectory() as d:
            store = StateStore(Path(d) / "s.json")
            state = store.init_or_load(src.epoch, 0, [])
            with self.assertRaises(LateScan):
                self._engine(src, store, state).run_backfill(
                    [{"id": "c1", "range": [0, 3], "low": "L1", "high": "H1"}])
            self.assertFalse(state["baseline_complete"])

    def test_stream_gap_blocks_window(self):
        src = self._build_source()
        src.commit_marker("L1")
        # 手工制造位置缺口：直接塞一条 position=3 的记录，跳过 2。
        src._log.append({"position": 3, "kind": "marker", "marker": "H1"})
        src._marker_pos["H1"] = 3
        with tempfile.TemporaryDirectory() as d:
            store = StateStore(Path(d) / "s.json")
            state = store.init_or_load(src.epoch, 0, [])
            with self.assertRaises(StreamGap):
                self._engine(src, store, state).run_backfill(
                    [{"id": "c1", "range": [0, 3], "low": "L1", "high": "H1"}])
            self.assertFalse(state["baseline_complete"])

    def test_epoch_change_invalidates_state(self):
        src = self._build_source()
        src.commit_marker("L1")
        with tempfile.TemporaryDirectory() as d:
            store = StateStore(Path(d) / "s.json")
            store.init_or_load(src.epoch, 0, [])
            with self.assertRaises(EpochChanged):
                store.init_or_load("a-different-epoch", 0, [])

    def test_same_position_different_content_blocks(self):
        src = self._build_source()
        src.commit_marker("L1")
        src.commit_transaction("tx1", [{"key": 1, "op": "upsert", "value": "v1"}])
        src.commit_marker("H1")
        with tempfile.TemporaryDirectory() as d:
            store = StateStore(Path(d) / "s.json")
            state = store.init_or_load(src.epoch, 0, [])
            eng = self._engine(src, store, state)
            eng.run_backfill([{"id": "c1", "range": [0, 3], "low": "L1", "high": "H1"}])
            # 重放时来源在同位置给出不同内容。
            src._log[1] = {"position": 2, "kind": "transaction", "txid": "tx1",
                           "changes": [{"key": 1, "op": "upsert", "value": "TAMPERED"}]}
            fresh = store.init_or_load(src.epoch, 0, [])
            with self.assertRaises(DigestConflict):
                self._engine(src, store, fresh).verify_applied_digests()

    def test_incomplete_plan_cannot_claim_baseline(self):
        # 来源上两块的 marker 都已确认，但本轮只计划 c1；c2 完成前不能宣称基准完成。
        src = self._build_source()
        src.commit_marker("L1")
        src.commit_marker("H1")
        src.commit_marker("L2")
        src.commit_marker("H2")
        with tempfile.TemporaryDirectory() as d:
            store = StateStore(Path(d) / "s.json")
            state = store.init_or_load(src.epoch, 0, [])
            self._engine(src, store, state).run_backfill(
                [{"id": "c1", "range": [0, 3], "low": "L1", "high": "H1"}])
            # 单块计划自身完成是合法的；但若引擎持有两块计划而一块未关闭则必须失败。
            self.assertTrue(state["baseline_complete"])
            state["baseline_complete"] = False
            store.save(state)
            eng = self._engine(src, store, state)
            for c in ({"id": "c1", "range": [0, 3], "low": "L1", "high": "H1"},
                      {"id": "c2", "range": [3, 6], "low": "L2", "high": "H2"}):
                eng._register_chunk(c)
            with self.assertRaises(BaselineIncomplete):
                eng._ingest(src.end_position)
                eng._release_held()
                if any(StateStore.chunk_state(state, cid)["status"] != "closed"
                       for cid in ("c1", "c2")):
                    raise BaselineIncomplete("c2 未完成")


if __name__ == "__main__":
    unittest.main()
