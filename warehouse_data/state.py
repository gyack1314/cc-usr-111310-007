"""崩溃安全的镜像状态。

每个持久化步骤后整包原子写入（临时文件 + os.replace），重启后只能从已确认位置重放。
状态保存：流世代、流持久进度、每块窗口与 low/high、扫描资产摘要、被消重键、
已应用事务摘要、逐键来源（provenance）以及完整事件日志。
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .protocol import EpochChanged


class StateStore:
    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)

    def init_or_load(self, epoch: str, initial_position: int, initial_mirror: list[dict]) -> dict:
        if self.path.exists():
            state = json.loads(self.path.read_text(encoding="utf-8"))
            if state.get("epoch") != epoch:
                # 流世代改变：旧窗口与旧进度全部失效，必须走完整基准代切换协议。
                raise EpochChanged(
                    f"流世代改变: 状态={state.get('epoch')!r} 来源={epoch!r}，旧窗口作废"
                )
            return state
        mirror = {}
        for row in initial_mirror:
            if type(row.get("key")) is not int or not isinstance(row.get("value"), str):
                raise ValueError("初始镜像行形状不符")
            mirror[str(row["key"])] = {
                "value": row["value"],
                "provenance": {"kind": "initial"},
            }
        state = {
            "epoch": epoch,
            "initial_position": initial_position,
            "durable_position": initial_position,
            "mirror": mirror,
            "chunks": {},
            "applied": {},
            "held": [],
            "baseline_complete": False,
            "baseline_generation": 0,
            "journal": [],
        }
        self.save(state)
        return state

    def save(self, state: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".state-", suffix=".tmp", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
            dir_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    # ---- 镜像行 ----------------------------------------------------------------
    @staticmethod
    def row(state: dict, key: int):
        return state["mirror"].get(str(key))

    @staticmethod
    def put_row(state: dict, key: int, value: str | None, provenance: dict) -> None:
        state["mirror"][str(key)] = {"value": value, "provenance": provenance}

    @staticmethod
    def live_rows(state: dict) -> list[dict]:
        """未删除（value 非 None）的镜像行，按键排序。"""
        return [
            {"key": int(k), "value": v["value"]}
            for k, v in sorted(state["mirror"].items(), key=lambda kv: int(kv[0]))
            if v["value"] is not None
        ]

    # ---- 块窗口 ----------------------------------------------------------------
    @staticmethod
    def chunk_state(state: dict, chunk_id: str) -> dict:
        return state["chunks"].setdefault(
            chunk_id,
            {"status": "open", "completed_window_id": None, "windows": []},
        )

    @staticmethod
    def open_window(state: dict, chunk_id: str, window_id: str,
                    low_marker: str, low_pos: int) -> dict:
        ch = StateStore.chunk_state(state, chunk_id)
        w = {
            "window_id": window_id,
            "attempt": len(ch["windows"]) + 1,
            "low_marker": low_marker,
            "low_pos": low_pos,
            "high_marker": None,
            "high_pos": None,
            "scan_id": None,
            "scan_digest": None,
            "scan_rows": 0,
            "invalidated": [],
            "candidates": [],
            "closed": False,
            "superseded": False,
        }
        ch["windows"].append(w)
        return w

    @staticmethod
    def journal(state: dict, event: str, **fields) -> None:
        state["journal"].append({"event": event, **fields})
