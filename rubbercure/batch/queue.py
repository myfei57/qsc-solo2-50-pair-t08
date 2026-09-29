"""待硫化任务队列。

模具登记之后进入调度队列，等空闲机位。队列按优先级排序、同优先级按入队
先后（FIFO），保证同一批制品按登记顺序投产。普通队列有容量上限，插单走
独立的加急容量；两队列都满时拒绝入队，避免无界积压。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from ..errors import RubberCureError

URGENT_PRIORITY = 100
MIN_PRIORITY = 0
MAX_PRIORITY = 100


@dataclass(frozen=True)
class QueueEntry:
    """一条待投产任务。"""

    mold_id: str
    product_code: str
    thickness_mm: float
    priority: int
    sequence: int
    enqueued_at: float
    urgent: bool = False

    def as_dict(self) -> dict:
        return {
            "mold_id": self.mold_id,
            "product_code": self.product_code,
            "thickness_mm": round(self.thickness_mm, 3),
            "priority": self.priority,
            "sequence": self.sequence,
            "enqueued_at": round(self.enqueued_at, 3),
            "urgent": self.urgent,
        }


class DispatchQueue:
    """有界优先队列，额外保留一条有限容量的插单通道。"""

    def __init__(self, max_queue: int, max_urgent_queue: int = 0) -> None:
        if max_queue < 1:
            raise RubberCureError("队列容量必须为正", component="batch")
        if max_urgent_queue < 0:
            raise RubberCureError("插单队列容量不能为负", component="batch")
        self._max = int(max_queue)
        self._max_urgent = int(max_urgent_queue)
        self._entries: list[QueueEntry] = []
        self._urgent_entries: list[QueueEntry] = []
        self._sequence = 0
        self._lock = threading.RLock()

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries) + len(self._urgent_entries)

    @staticmethod
    def _check_priority(priority: int) -> int:
        value = int(priority)
        if not MIN_PRIORITY <= value <= MAX_PRIORITY:
            raise RubberCureError(
                f"优先级必须在 {MIN_PRIORITY} 到 {MAX_PRIORITY} 之间", component="batch"
            )
        return value

    def push(
        self,
        mold_id: str,
        product_code: str,
        thickness_mm: float,
        enqueued_at: float,
        priority: int = 50,
        urgent: bool = False,
    ) -> QueueEntry:
        """入队一条任务；``urgent=True`` 时使用独立的插单容量。"""

        priority = self._check_priority(priority)
        with self._lock:
            bucket = self._urgent_entries if urgent else self._entries
            capacity = self._max_urgent if urgent else self._max
            if len(bucket) >= capacity:
                label = "插单队列" if urgent else "调度队列"
                raise RubberCureError(f"{label}已满（{capacity}）", component="batch")
            self._sequence += 1
            entry = QueueEntry(
                mold_id=mold_id,
                product_code=product_code,
                thickness_mm=float(thickness_mm),
                priority=priority,
                sequence=self._sequence,
                enqueued_at=float(enqueued_at),
                urgent=urgent,
            )
            bucket.append(entry)
            return entry

    def push_urgent(
        self,
        mold_id: str,
        product_code: str,
        thickness_mm: float,
        enqueued_at: float,
        priority: int = URGENT_PRIORITY,
    ) -> QueueEntry:
        """把插单放入独立加急容量，并默认置于最高优先级。"""

        return self.push(
            mold_id,
            product_code,
            thickness_mm,
            enqueued_at,
            priority=priority,
            urgent=True,
        )

    def _ordered(self) -> list[QueueEntry]:
        return sorted(
            [*self._urgent_entries, *self._entries],
            key=lambda item: (-item.priority, 0 if item.urgent else 1, item.sequence),
        )

    def peek_next(self) -> QueueEntry | None:
        """查看优先级最高、同优先级最早入队的一条任务，不移除。"""

        with self._lock:
            if not self._entries and not self._urgent_entries:
                return None
            return self._ordered()[0]

    def pop_next(self) -> QueueEntry | None:
        """取出优先级最高、同优先级最早入队的一条任务。"""

        with self._lock:
            if not self._entries and not self._urgent_entries:
                return None
            entry = self._ordered()[0]
            bucket = self._urgent_entries if entry.urgent else self._entries
            bucket.remove(entry)
            return entry

    def remove(self, mold_id: str) -> bool:
        """按模具编号撤出任务。"""

        with self._lock:
            for entry in list(self._urgent_entries):
                if entry.mold_id == mold_id:
                    self._urgent_entries.remove(entry)
                    return True
            for entry in list(self._entries):
                if entry.mold_id == mold_id:
                    self._entries.remove(entry)
                    return True
        return False

    def entries(self) -> list[dict]:
        with self._lock:
            return [entry.as_dict() for entry in self._ordered()]

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "size": len(self),
                "capacity": self._max + self._max_urgent,
                "urgent_size": len(self._urgent_entries),
                "urgent_capacity": self._max_urgent,
                "entries": [entry.as_dict() for entry in self._ordered()],
            }
