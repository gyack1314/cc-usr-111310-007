"""结果核对与来源报告。"""
from __future__ import annotations

from .state import StateStore


def diff_against_reference(state: dict, reference_rows: list[dict]) -> dict:
    """与来源固定结束位置快照逐行对照：值、缺失键、多余存活键都要列出。"""
    ref = {r["key"]: r["value"] for r in reference_rows}
    got = {r["key"]: r["value"] for r in StateStore.live_rows(state)}
    missing = [{"key": k, "expected": ref[k]} for k in sorted(ref.keys() - got.keys())]
    extra = [{"key": k, "actual": got[k]} for k in sorted(got.keys() - ref.keys())]
    mismatched = [
        {"key": k, "expected": ref[k], "actual": got[k]}
        for k in sorted(ref.keys() & got.keys()) if ref[k] != got[k]
    ]
    return {
        "matches": not (missing or extra or mismatched),
        "reference_rows": len(ref),
        "mirror_live_rows": len(got),
        "missing": missing,
        "extra": extra,
        "mismatched": mismatched,
    }


def provenance_report(state: dict) -> list[dict]:
    """每个键来自扫描（哪块/哪窗/资产摘要）还是哪个事务（position/txid/op）。"""
    out = []
    for key, cell in sorted(state["mirror"].items(), key=lambda kv: int(kv[0])):
        p = cell["provenance"]
        kind = p["kind"]
        if kind == "scan":
            origin = f"扫描 {p['chunk']}/{p['window_id']} 资产 {p['digest'][:12]}"
        elif kind == "stream":
            suffix = "删除" if p["op"] == "delete" else f"新值"
            origin = f"事务 {p['txid']}@位置{p['position']}（{p['op']}，{suffix}）"
        else:
            origin = "初始镜像"
        out.append({
            "key": int(key),
            "status": "deleted" if cell["value"] is None else "live",
            "value": cell["value"],
            "provenance": kind,
            "origin": origin,
        })
    return out


def chunk_report(state: dict) -> list[dict]:
    rows = []
    for cid, ch in sorted(state["chunks"].items()):
        for w in ch["windows"]:
            rows.append({
                "chunk": cid,
                "window_id": w["window_id"],
                "attempt": w["attempt"],
                "low": f"{w['low_marker']}@{w['low_pos']}",
                "high": (f"{w['high_marker']}@{w['high_pos']}"
                         if w["high_pos"] is not None else "未确认"),
                "scan_asset": w["scan_digest"],
                "scan_rows": w["scan_rows"],
                "invalidated_keys": w["invalidated"],
                "backfilled_keys": w["candidates"],
                "closed": w["closed"],
                "superseded": w["superseded"],
            })
    return rows
