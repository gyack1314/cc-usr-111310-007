"""分块增量快照引擎。

窗口协议（每块）：
1. 写 low marker 并等到它在流中出现，先把流吃到 low；
2. 在 low/high 之间执行非一致的半开区间扫描，资产摘要立即落盘；
3. 写 high marker 并等到该标记，把流吃到 high；
4. (low, high) 内任何触及本块主键的已提交变更（含删除）使对应扫描行失效；
5. 未被触及的扫描行仅在镜像无权威行时补入；变化键一律以事务流实际新状态为准；
6. 扫描顺序不参与新旧判断；晚到扫描不得覆盖 high 之后流中更晚的状态。

一个事务可同时改多个块：在所有相关块窗口关闭前整体持有，之后一次性原子应用。
"""
from __future__ import annotations

from .protocol import (
    DELETE,
    MARKER,
    TRANSACTION,
    BaselineIncomplete,
    DigestConflict,
    EpochChanged,
    ProtocolError,
    StreamGap,
    transaction_digest,
)
from .state import StateStore


class SnapshotEngine:
    def __init__(self, source, store: StateStore, state: dict, *, failpoint: str | None = None,
                 mutable_scan: bool = False):
        self.source = source
        self.store = store
        self.state = state
        self.failpoint = failpoint
        # mutable_scan: 动态源的非一致扫描每次内容可变，重放只留证不重新拉取比对；
        # 冻结源资产不可变，重放必须按 sha256 严格核对。
        # 晚到扫描由来源适配器在 open_scan 直接抛 LateScan（high 已确认，扫描不得再挂窗）。
        self.mutable_scan = mutable_scan
        # 持有中的事务必须随状态持久化：durable_position 已越过其位置，
        # 崩溃后流不会重放，只靠状态恢复，否则跨块事务会静默丢失。
        self._held: list[dict] = state.get("held", [])
        self._plan: dict[str, dict] = {}
        self._key_chunk: dict[int, str] = {}

    # ---- 公共入口 --------------------------------------------------------------
    def run_backfill(self, chunks: list[dict]) -> dict:
        # 恢复路径先核对已应用/持有事务与来源是否同摘要：durable_position 已越过这些位置，
        # 普通重放不会再读到它们，只能主动校验，否则同位置篡改会被静默接受。
        if self.state.get("applied") or self.state.get("held"):
            self.verify_applied_digests()
        for c in chunks:
            self._register_chunk(c)
        pending = [c for c in chunks
                   if StateStore.chunk_state(self.state, c["id"])["status"] != "closed"]
        if not pending:
            # 全部计划块已完成：重跑必须是无操作，不写状态、不开新窗口、不重发扫描候选。
            return self.state
        for c in pending:
            self._run_window(c)
        # 所有计划块窗口关闭后，流仍可继续：吃到来源当前结尾并放行全部持有事务。
        self._ingest(self.source.end_position)
        self._release_held()
        if self._held:
            raise BaselineIncomplete("仍有事务等待未完成块，不能结束基准")
        if any(StateStore.chunk_state(self.state, c["id"])["status"] != "closed" for c in chunks):
            raise BaselineIncomplete("存在未完成块，不能把扫过所有区间当作无漏入仓")
        self.state["baseline_complete"] = True
        self.state["baseline_generation"] += 1
        StateStore.journal(self.state, "baseline-complete",
                           generation=self.state["baseline_generation"],
                           end_position=self.state["durable_position"])
        self.store.save(self.state)
        return self.state

    # ---- 块注册 ----------------------------------------------------------------
    def _register_chunk(self, c: dict) -> None:
        cid = c["id"]
        lo, hi = c["range"]
        if type(lo) is not int or type(hi) is not int or not (lo < hi):
            raise ProtocolError(f"块 {cid} 半开区间非法")
        if cid in self._plan:
            raise ProtocolError(f"块 id 重复: {cid}")
        for k in range(lo, hi):
            if k in self._key_chunk:
                raise ProtocolError(f"主键 {k} 同时落入两个计划块")
            self._key_chunk[k] = cid
        self._plan[cid] = c

    # ---- 单块窗口 --------------------------------------------------------------
    def _run_window(self, c: dict) -> None:
        cid, (lo, hi) = c["id"], tuple(c["range"])
        ch = StateStore.chunk_state(self.state, cid)
        w = self._resumable_window(ch)

        if w is None:
            low_pos = self.source.ensure_marker(c["low"])
            self._ingest(low_pos)
            w = StateStore.open_window(self.state, cid, f"{cid}-w{len(ch['windows']) + 1}",
                                       c["low"], low_pos)
            StateStore.journal(self.state, "window-opened", chunk=cid,
                               window_id=w["window_id"], low_marker=c["low"], low_pos=low_pos)
            self.store.save(self.state)
            self._crash("after_low")

        # 动态源上：扫描已落盘但 high 始终未确认就崩溃 → 旧窗口作废，换新 marker 重开。
        # 若 high 已确认（after_high 崩溃），同一窗口直接复用，不得重扫或重开。
        high_confirmed = w is not None and w["high_pos"] is not None
        if (w is not None and not high_confirmed and w["scan_id"] is not None
                and self.mutable_scan):
            w["superseded"] = True
            StateStore.journal(self.state, "window-superseded", chunk=cid,
                               window_id=w["window_id"], reason="high 未确认，旧扫描只留证")
            self.store.save(self.state)
            low2 = f"{c['low']}-r{w['attempt']}"
            high2 = f"{c['high']}-r{w['attempt']}"
            c = {**c, "low": low2, "high": high2}
            low_pos = self.source.ensure_marker(low2)
            self._ingest(low_pos)
            w = StateStore.open_window(self.state, cid, f"{cid}-w{len(ch['windows']) + 1}",
                                       low2, low_pos)
            StateStore.journal(self.state, "window-opened", chunk=cid,
                               window_id=w["window_id"], low_marker=low2, low_pos=low_pos)
            self.store.save(self.state)

        if w is None or w["scan_id"] is None:
            asset = self.source.open_scan(lo, hi, f"scan-{w['window_id']}", c.get("scan"))
            w["scan_id"] = asset["scan_id"]
            w["scan_digest"] = asset["digest"]
            w["scan_rows"] = len(asset["rows"])
            w["_rows"] = asset["rows"]
            StateStore.journal(self.state, "scan-opened", chunk=cid,
                               window_id=w["window_id"], scan_id=asset["scan_id"],
                               digest=asset["digest"], rows=len(asset["rows"]))
            self.store.save(self.state)
            self._crash("after_scan")
        elif not self.mutable_scan:
            # 冻结资产重放：重新取回并核对摘要，旧候选不重新当作新结果。
            asset = self.source.open_scan(lo, hi, w["scan_id"], c.get("scan"))
            if asset["digest"] != w["scan_digest"]:
                raise ProtocolError(f"块 {cid} 扫描资产摘要与已存证据不一致")
            w["_rows"] = asset["rows"]
        else:
            # 动态源重放：非一致扫描内容可能已变，旧扫描结果不再重发，只留摘要证据。
            w["_rows"] = []

        if w["high_pos"] is None:
            high_pos = self.source.ensure_marker(c["high"])
            self._ingest(high_pos)
            w["high_marker"] = c["high"]
            w["high_pos"] = high_pos
            StateStore.journal(self.state, "high-confirmed", chunk=cid,
                               window_id=w["window_id"], high_marker=c["high"], high_pos=high_pos)
            self.store.save(self.state)
            self._crash("after_high")

        self._close_window(c, w)

    def _resumable_window(self, ch: dict) -> dict | None:
        for w in reversed(ch["windows"]):
            if not w["closed"] and not w["superseded"]:
                return w
        return None

    def _close_window(self, c: dict, w: dict) -> None:
        cid, (lo, hi) = c["id"], tuple(c["range"])
        # 窗口 (low, high) 不含 marker 本身；重放该区间全部已提交事务，按位置而非顺序猜新旧。
        invalidated: set[int] = set()
        for rec in self.source.read_after(w["low_pos"]):
            if rec["position"] >= w["high_pos"]:
                break
            if rec["kind"] != TRANSACTION:
                continue
            for key in [ch["key"] for ch in rec["changes"] if lo <= ch["key"] < hi]:
                invalidated.add(key)
                StateStore.journal(self.state, "key-invalidated", chunk=cid,
                                   window_id=w["window_id"], key=key,
                                   position=rec["position"], txid=rec["txid"])
        w["invalidated"] = sorted(invalidated)

        survivors = []
        for row in w["_rows"]:
            key = row["key"]
            if key in invalidated:
                continue
            existing = StateStore.row(self.state, key)
            if existing is not None:
                # 流（或既有镜像）已是权威行：扫描永远不覆盖更晚状态。
                StateStore.journal(self.state, "scan-skipped-present", chunk=cid,
                                   window_id=w["window_id"], key=key,
                                   present_provenance=existing["provenance"].get("kind"))
                continue
            StateStore.put_row(self.state, key, row["value"], {
                "kind": "scan", "chunk": cid, "window_id": w["window_id"],
                "scan_id": w["scan_id"], "digest": w["scan_digest"],
            })
            survivors.append(key)
            StateStore.journal(self.state, "scan-backfill", chunk=cid,
                               window_id=w["window_id"], key=key, value=row["value"])
        w["candidates"] = survivors
        w["closed"] = True
        w.pop("_rows", None)
        ch = StateStore.chunk_state(self.state, cid)
        ch["status"] = "closed"
        ch["completed_window_id"] = w["window_id"]
        StateStore.journal(self.state, "window-closed", chunk=cid,
                           window_id=w["window_id"], invalidated=sorted(invalidated),
                           backfilled=survivors)
        self.store.save(self.state)
        self._release_held()

    def verify_applied_digests(self) -> None:
        """重放全部流，核对每个已应用位置与持有位置的事务摘要；同位置不同内容即阻塞。"""
        by_pos = {r["position"]: r for r in self.source.read_after(
            self.state.get("initial_position", 0)) if r["kind"] == TRANSACTION}
        for pos, seen in self.state["applied"].items():
            rec = by_pos.get(int(pos))
            if rec is not None and seen != transaction_digest(rec):
                raise DigestConflict(
                    f"位置 {pos} 出现内容不同的事务 {rec['txid']}，禁止重放覆盖")
        # 持有中的事务也必须与来源一致：它尚未应用，崩溃后只能原样等待放行。
        for rec in self.state.get("held", []):
            cur = by_pos.get(rec["position"])
            if cur is None or transaction_digest(cur) != transaction_digest(rec):
                raise DigestConflict(
                    f"位置 {rec['position']} 的持有事务 {rec['txid']} 与来源不一致，窗口作废")

    # ---- 流摄取与事务持有 -------------------------------------------------------
    def _ingest(self, until_pos: int) -> None:
        while True:
            batch = [r for r in self.source.read_after(self.state["durable_position"])
                     if r["position"] <= until_pos]
            if not batch:
                return
            for rec in sorted(batch, key=lambda r: r["position"]):
                expected = self.state["durable_position"] + 1
                if rec["position"] != expected:
                    raise StreamGap(
                        f"流位置不连续：期望 {expected}，实际 {rec['position']}；"
                        "变更窗口不完整，本块保持未完成"
                    )
                if self.source.epoch != self.state["epoch"]:
                    raise EpochChanged("流世代改变，进行中的窗口全部失效")
                if rec["kind"] == MARKER:
                    self.state["durable_position"] = rec["position"]
                    StateStore.journal(self.state, "marker-observed",
                                       marker=rec["marker"], position=rec["position"])
                else:
                    self._ingest_transaction(rec)
                    self.state["durable_position"] = rec["position"]
                self.store.save(self.state)

    def _ingest_transaction(self, rec: dict) -> None:
        pos = rec["position"]
        digest = transaction_digest(rec)
        seen = self.state["applied"].get(str(pos))
        if seen is not None:
            if seen != digest:
                # 同位置不同内容：禁止猜测，人工阻塞，窗口不算完成。
                raise DigestConflict(f"位置 {pos} 出现内容不同的事务 {rec['txid']}")
            StateStore.journal(self.state, "txn-duplicate-skip", position=pos, txid=rec["txid"])
            return
        if not self._held and self._releasable(rec):
            self._apply(rec, digest)
        else:
            # 持有队列非空时必须排在队尾：任何事务都不得越过提交位置更早、
            # 仍在等待窗口的事务先应用，否则旧事务放行会覆盖更新的状态。
            self._held.append(rec)
            self.state["held"] = self._held
            awaiting = sorted({
                self._key_chunk[k]
                for k in (c["key"] for c in rec["changes"])
                if k in self._key_chunk
                and StateStore.chunk_state(self.state, self._key_chunk[k])["status"] != "closed"
            })
            if self._held[:-1]:
                reason = "排在更早的未放行事务之后"
            else:
                reason = "等待相关块窗口关闭"
            StateStore.journal(self.state, "txn-held", position=pos, txid=rec["txid"],
                               keys=[c["key"] for c in rec["changes"]],
                               awaiting=awaiting, reason=reason)

    def _releasable(self, rec: dict) -> bool:
        # 事务整体可见：它触及的每个已计划块都关闭后才能整体应用。
        # 不属于任何计划块的键不阻塞（计划块在开窗前已全部注册）。
        for ch in rec["changes"]:
            cid = self._key_chunk.get(ch["key"])
            if cid is None:
                continue
            if StateStore.chunk_state(self.state, cid)["status"] != "closed":
                return False
        return True

    def _release_held(self) -> None:
        # 持有队列按提交位置排列。放行必须保持提交顺序：队首事务的条件若还不满足，
        # 其后的事务即使条件满足也不得先应用，否则位置更大的状态会被旧事务覆盖。
        still_held = []
        applied_any = False
        blocked = False
        for rec in self._held:
            if not blocked and self._releasable(rec):
                # 持有的事务已在摄取时做过幂等/摘要检查，放行时直接整体应用；
                # 不能再走 _ingest_transaction，否则会被当作“无阻塞新事务”而跳过。
                self._apply(rec, transaction_digest(rec))
                applied_any = True
            else:
                blocked = True
                still_held.append(rec)
        self._held = still_held
        self.state["held"] = self._held
        if applied_any:
            self.store.save(self.state)

    def _apply(self, rec: dict, digest: str) -> None:
        # 事务内变更按列表顺序整体应用，upsert/delete 落到同一原子状态保存里。
        for ch in rec["changes"]:
            prov = {"kind": "stream", "txid": rec["txid"],
                    "position": rec["position"], "op": ch["op"]}
            if ch["op"] == DELETE:
                StateStore.put_row(self.state, ch["key"], None, prov)
                StateStore.journal(self.state, "txn-delete", key=ch["key"],
                                   position=rec["position"], txid=rec["txid"])
            else:
                StateStore.put_row(self.state, ch["key"], ch["value"], prov)
                StateStore.journal(self.state, "txn-upsert", key=ch["key"],
                                   value=ch["value"], position=rec["position"], txid=rec["txid"])
        self.state["applied"][str(rec["position"])] = digest
        StateStore.journal(self.state, "txn-applied", position=rec["position"],
                           txid=rec["txid"], changes=len(rec["changes"]), digest=digest)

    def _crash(self, step: str) -> None:
        if self.failpoint == step:
            StateStore.journal(self.state, "crash-injected", step=step)
            self.store.save(self.state)
            raise ProtocolError(f"故障注入：{step} 后进程终止（状态已持久化）")
