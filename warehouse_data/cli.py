"""命令行：inspect 只读核对；snapshot/verify/report 驱动分块增量快照协议。"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .domain import summarize
from .engine import SnapshotEngine
from .io import load
from .protocol import ProtocolError
from .report import chunk_report, diff_against_reference, provenance_report
from .source import FrozenSource
from .state import StateStore


def _load_manifest(file: str):
    v, digest = load(file)
    root = Path(file).resolve().parent
    return v, digest, root


def cmd_snapshot(a) -> None:
    v, digest, root = _load_manifest(a.file)
    source = FrozenSource(v, root)
    store = StateStore(Path(a.state_dir) / "mirror-state.json")
    state = store.init_or_load(source.epoch, v["initial_position"], v["initial_mirror"])
    engine = SnapshotEngine(source, store, state, failpoint=a.failpoint)
    engine.run_backfill(v["chunks"])
    result = {
        "manifest_sha256": digest,
        "baseline_complete": state["baseline_complete"],
        "durable_position": state["durable_position"],
        "diff": diff_against_reference(state, v["reference_end_rows"]),
        "keys": provenance_report(state),
        "windows": chunk_report(state),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


def cmd_verify(a) -> None:
    v, digest, _ = _load_manifest(a.file)
    store = StateStore(Path(a.state_dir) / "mirror-state.json")
    state = store.init_or_load(v["source_epoch"], v["initial_position"], [])
    result = {"manifest_sha256": digest, **diff_against_reference(state, v["reference_end_rows"])}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["matches"]:
        raise SystemExit(1)


def cmd_report(a) -> None:
    v, _, _ = _load_manifest(a.file)
    store = StateStore(Path(a.state_dir) / "mirror-state.json")
    state = store.init_or_load(v["source_epoch"], v["initial_position"], [])
    print(json.dumps({
        "durable_position": state["durable_position"],
        "baseline_complete": state["baseline_complete"],
        "keys": provenance_report(state),
        "windows": chunk_report(state),
        "journal_tail": state["journal"][-a.tail:],
    }, ensure_ascii=False, indent=2))


def cmd_inspect(a) -> None:
    v, digest = load(a.file)
    print(json.dumps(
        {"manifest_sha256": digest, "summary": summarize(v, Path(a.file).parent)},
        ensure_ascii=False, indent=2,
    ))


def main():
    p = argparse.ArgumentParser(description="分块增量快照入仓镜像")
    sub = p.add_subparsers(dest="command", required=True)

    pi = sub.add_parser("inspect", help="只读核对原始资料")
    pi.add_argument("file")
    pi.set_defaults(func=cmd_inspect)

    ps = sub.add_parser("snapshot", help="执行分块增量快照补数")
    ps.add_argument("file")
    ps.add_argument("--state-dir", required=True)
    ps.add_argument("--failpoint", choices=["after_low", "after_scan", "after_high"],
                    default=None, help="故障注入：在指定步骤崩溃（状态已持久化）")
    ps.set_defaults(func=cmd_snapshot)

    pv = sub.add_parser("verify", help="与来源固定结束位置快照逐行对照")
    pv.add_argument("file")
    pv.add_argument("--state-dir", required=True)
    pv.set_defaults(func=cmd_verify)

    pr = sub.add_parser("report", help="输出逐键来源与窗口报告")
    pr.add_argument("file")
    pr.add_argument("--state-dir", required=True)
    pr.add_argument("--tail", type=int, default=20)
    pr.set_defaults(func=cmd_report)

    a = p.parse_args()
    try:
        a.func(a)
    except ProtocolError as e:
        # 协议失败：块保持未完成，退出码 2，绝不输出“补数完成”。
        print(json.dumps({"protocol_error": type(e).__name__, "message": str(e)},
                         ensure_ascii=False), file=sys.stderr)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
