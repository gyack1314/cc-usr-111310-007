"""分块增量快照协议的记录类型、形状校验与异常。

来源契约（见 docs/data-contract.md）：
- position 是来源连续日志位置，单调递增，每项是完整事务或已确认 marker；
- 窗口 (low, high) 不含 marker 本身，按提交位置过滤触及键；
- 事务整体可见、提交后才可见，源主键不可更新，update 旧像新像同键；
- 扫描是非一致整块观测，每键最多一行且落在半开区间 [lo, hi)。
"""
from __future__ import annotations

import hashlib
import json

MARKER = "marker"
TRANSACTION = "transaction"
UPSERT = "upsert"
DELETE = "delete"


class ProtocolError(Exception):
    """协议违反：窗口必须保持未完成，不能当作补数成功。"""


class StreamGap(ProtocolError):
    """流位置不连续（缺位置），连续变更窗口被破坏。"""


class EpochChanged(ProtocolError):
    """流世代改变，旧窗口全部失效。"""


class MarkerUnconfirmed(ProtocolError):
    """low/high marker 未在流中确认出现。"""


class DigestConflict(ProtocolError):
    """同位置重复事务摘要不同，必须人工阻塞，禁止幂等跳过。"""


class LateScan(ProtocolError):
    """扫描结果晚于 high 到达，不得覆盖流中更晚状态。"""


class ScanOutOfRange(ProtocolError):
    """扫描行越出半开主键区间或键重复。"""


class BaselineIncomplete(ProtocolError):
    """仍有计划块未完成，不能宣称基准补数完成。"""


class CrashInjected(ProtocolError):
    """测试用故障注入：模拟进程在指定步骤崩溃。"""


def validate_record(rec: dict) -> dict:
    """校验一条流记录，返回规范化后的浅拷贝。"""
    if not isinstance(rec, dict):
        raise ProtocolError("流记录必须是对象")
    pos = rec.get("position")
    if type(pos) is not int or pos <= 0:
        raise ProtocolError("position 必须为正整数")
    kind = rec.get("kind")
    if kind == MARKER:
        name = rec.get("marker")
        if not isinstance(name, str) or not name:
            raise ProtocolError("marker 缺少名称")
        return {"position": pos, "kind": MARKER, "marker": name}
    if kind == TRANSACTION:
        txid = rec.get("txid")
        if not isinstance(txid, str) or not txid:
            raise ProtocolError("事务缺少 txid")
        changes = rec.get("changes")
        if not isinstance(changes, list) or not changes:
            raise ProtocolError("事务变更必须是非空列表")
        norm = []
        for ch in changes:
            if not isinstance(ch, dict):
                raise ProtocolError("变更必须是对象")
            key = ch.get("key")
            if type(key) is not int:
                raise ProtocolError("主键必须是整数且不可更新")
            op = ch.get("op")
            if op == UPSERT:
                value = ch.get("value")
                if not isinstance(value, str):
                    raise ProtocolError("upsert 值必须是完整 string")
                norm.append({"key": key, "op": UPSERT, "value": value})
            elif op == DELETE:
                if "value" in ch and ch["value"] is not None:
                    raise ProtocolError("delete 不能携带 value")
                norm.append({"key": key, "op": DELETE})
            else:
                raise ProtocolError(f"未知变更操作: {op!r}")
        keys = [c["key"] for c in norm]
        if len(set(keys)) != len(keys):
            raise ProtocolError("同一事务内主键重复")
        return {"position": pos, "kind": TRANSACTION, "txid": txid, "changes": norm}
    raise ProtocolError(f"来源操作未知: {kind!r}")


def record_position(rec: dict) -> int:
    pos = rec["position"]
    if type(pos) is not int or pos <= 0:
        raise ProtocolError("position 必须为正整数")
    return pos


def transaction_digest(rec: dict) -> str:
    """同位置重复事务的幂等判据：位置、txid 与全部变更的稳定摘要。"""
    payload = json.dumps(
        {"position": rec["position"], "txid": rec["txid"], "changes": rec["changes"]},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def touched_keys(rec: dict) -> list[int]:
    return [ch["key"] for ch in rec["changes"]]
