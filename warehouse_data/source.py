"""来源适配器。

提供协议要求的三项能力：
1. 单调提交位置的完整事务流（``read_after``），事务提交后整体可见；
2. 可确认已进入流的 low/high marker（``ensure_marker`` / ``marker_position``）；
3. 按不可变主键的半开区间 [lo, hi) 扫描（``open_scan``），非一致观测。

``FrozenSource`` 面向仓库中带 sha256 身份的冻结资料；
``LiveSource`` 是内存动态来源，用于演示生产写入不停止、窗口崩溃后换新 marker 重开。
"""
from __future__ import annotations

import hashlib
import json
from typing import Optional

from . import io as _io
from .protocol import (
    MARKER,
    MarkerUnconfirmed,
    ScanOutOfRange,
    TRANSACTION,
    validate_record,
)


class FrozenSource:
    """冻结清单来源：marker 与扫描资产都已固定，缺失即协议失败。"""

    def __init__(self, manifest: dict, root):
        self._m = manifest
        self._root = root
        self.epoch = manifest["source_epoch"]
        self.initial_position = manifest["initial_position"]
        self.end_position = manifest["end_position"]
        self._marker_pos: dict[str, int] = {}
        for rec in manifest["stream"]:
            r = validate_record(rec)
            if r["kind"] == MARKER:
                if r["marker"] in self._marker_pos:
                    raise MarkerUnconfirmed("marker 重复定义")
                self._marker_pos[r["marker"]] = r["position"]

    def read_after(self, position: int) -> list[dict]:
        out = []
        for rec in self._m["stream"]:
            r = validate_record(rec)
            if r["position"] > position:
                out.append(r)
        return out

    def marker_position(self, name: str) -> Optional[int]:
        return self._marker_pos.get(name)

    def ensure_marker(self, name: str) -> int:
        # 冻结来源不能由客户端写 marker：未确认就是未确认，禁止伪造成功。
        pos = self._marker_pos.get(name)
        if pos is None:
            raise MarkerUnconfirmed(f"marker {name!r} 未在流中确认")
        return pos

    def open_scan(self, lo: int, hi: int, scan_id: str, asset_ref: dict) -> dict:
        raw = _io.asset(self._root, asset_ref)
        digest = hashlib.sha256(raw).hexdigest()
        rows = []
        for line in raw.decode("utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if type(row.get("key")) is not int or not isinstance(row.get("value"), str):
                raise ScanOutOfRange("扫描行形状不符")
            rows.append({"key": row["key"], "value": row["value"]})
        if "rows" in asset_ref and asset_ref["rows"] != len(rows):
            raise ScanOutOfRange(
                f"扫描行数 {len(rows)} 与清单声明 {asset_ref['rows']} 不符")
        _check_range(rows, lo, hi)
        return {"scan_id": scan_id, "digest": digest, "rows": rows}


class LiveSource:
    """内存动态来源。marker 与事务各自原子提交到同一条单调日志。"""

    def __init__(self, initial_table: dict[int, str], epoch: str = "live-1", initial_position: int = 0):
        self.epoch = epoch
        self.initial_position = initial_position
        self._table = dict(initial_table)
        self._log: list[dict] = []
        self._marker_pos: dict[str, int] = {}

    @property
    def end_position(self) -> int:
        return self._log[-1]["position"] if self._log else self.initial_position

    def _next_position(self) -> int:
        return (self._log[-1]["position"] + 1) if self._log else self.initial_position + 1

    def commit_marker(self, name: str) -> int:
        if name in self._marker_pos:
            raise MarkerUnconfirmed("marker 已存在，不能重复写入")
        pos = self._next_position()
        rec = {"position": pos, "kind": MARKER, "marker": name}
        self._log.append(validate_record(rec))
        self._marker_pos[name] = pos
        return pos

    def commit_transaction(self, txid: str, changes: list[dict]) -> int:
        pos = self._next_position()
        rec = {"position": pos, "kind": TRANSACTION, "txid": txid, "changes": changes}
        rec = validate_record(rec)
        # 提交点：日志追加与表变更作为一个原子步骤，提交后才整体可见。
        self._log.append(rec)
        for ch in rec["changes"]:
            if ch["op"] == "upsert":
                self._table[ch["key"]] = ch["value"]
            else:
                self._table.pop(ch["key"], None)
        return pos

    def read_after(self, position: int) -> list[dict]:
        return [dict(r) if r["kind"] == MARKER else
                {"position": r["position"], "kind": TRANSACTION, "txid": r["txid"],
                 "changes": [dict(c) for c in r["changes"]]}
                for r in self._log if r["position"] > position]

    def marker_position(self, name: str) -> Optional[int]:
        return self._marker_pos.get(name)

    def ensure_marker(self, name: str) -> int:
        existing = self._marker_pos.get(name)
        if existing is not None:
            return existing
        return self.commit_marker(name)

    def open_scan(self, lo: int, hi: int, scan_id: str, asset_ref: dict | None = None) -> dict:
        # 非一致观测：读取调用瞬间的表状态，不与任何事务快照对齐。
        rows = [{"key": k, "value": v} for k, v in sorted(self._table.items()) if lo <= k < hi]
        _check_range(rows, lo, hi)
        body = "".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n" for r in rows)
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        return {"scan_id": scan_id, "digest": digest, "rows": rows}


def _check_range(rows: list[dict], lo: int, hi: int) -> None:
    seen = set()
    for row in rows:
        key = row["key"]
        if not (lo <= key < hi):
            raise ScanOutOfRange(f"扫描键 {key} 越出半开区间 [{lo},{hi})")
        if key in seen:
            raise ScanOutOfRange(f"扫描键 {key} 重复，每键最多一行")
        seen.add(key)
