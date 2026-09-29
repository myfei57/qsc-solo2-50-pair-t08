"""模次分配。

调度把队首任务分配给一台空闲硫化机：装模、记录分配、发布事件。分配要求
目标机当前没有在机模具——同一台机在完成上报之前不能被再次分配，否则两个
模次会挤在同一台机上。目标机位可以由完成上报的稳定次序显式指定。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..eventbus import Event, EventBus
from ..mold.registry import MoldRegistry
from ..press.transitions import PressPhase
from .queue import DispatchQueue

if TYPE_CHECKING:  # pragma: no cover - 仅用于类型标注
    from ..press.fleet import PressFleet


@dataclass(frozen=True)
class Assignment:
    """一次模次分配。"""

    press_id: str
    mold_id: str
    product_code: str
    thickness_mm: float
    assigned_at: float

    def as_dict(self) -> dict:
        return {
            "press_id": self.press_id,
            "mold_id": self.mold_id,
            "product_code": self.product_code,
            "thickness_mm": round(self.thickness_mm, 3),
            "assigned_at": round(self.assigned_at, 3),
        }


def assign_entry(
    fleet: "PressFleet",
    queue: DispatchQueue,
    registry: MoldRegistry,
    bus: EventBus,
    assigned_at: float,
    target_press_id: str | None = None,
) -> Assignment | None:
    """把队首任务分配给指定或编号最小的空闲机位；无机位时返回 None。"""

    if len(queue) == 0:
        return None

    idle = [unit for unit in fleet.all() if unit.phase is PressPhase.IDLE and unit.mold_id is None]
    target = None
    if target_press_id is not None:
        for unit in idle:
            if unit.press_id == target_press_id:
                target = unit
                break
    elif idle:
        target = idle[0]

    if target is None:
        return None

    entry = queue.peek_next()
    if entry is None:
        return None
    registry.require_active(entry.mold_id)
    removed = queue.pop_next()
    if removed is None or removed.sequence != entry.sequence:
        return None
    target.load_mold(entry.mold_id)
    assignment = Assignment(
        press_id=target.press_id,
        mold_id=entry.mold_id,
        product_code=entry.product_code,
        thickness_mm=entry.thickness_mm,
        assigned_at=assigned_at,
    )
    bus.publish(
        Event.of(
            "batch",
            "batch",
            "mold_assigned",
            subject=entry.mold_id,
            severity="info",
            **assignment.as_dict(),
        )
    )
    return assignment


def assign_next(
    fleet: "PressFleet",
    queue: DispatchQueue,
    registry: MoldRegistry,
    bus: EventBus,
    assigned_at: float,
) -> Assignment | None:
    """把队首任务分配给一台空闲机位；无任务或无机位时返回 None。"""

    return assign_entry(fleet, queue, registry, bus, assigned_at)
