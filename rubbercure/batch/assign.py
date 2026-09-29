"""模次分配。

调度把队首任务分配给一台空闲硫化机：装模、记录分配、发布事件。分配要求
目标机当前没有在机模具——同一台机在完成上报之前不能被再次分配，否则两个
模次会挤在同一台机上。完成上报触发的派工回到上报机位，保证同一批完成上报
按稳定顺序一一对应。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..eventbus import Event, EventBus
from ..mold.registry import MoldRegistry
from .queue import DispatchQueue

if TYPE_CHECKING:  # pragma: no cover - 仅用于类型标注
    from ..press.fleet import PressFleet
    from ..press.press import CuringPress


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


def _available(fleet: "PressFleet") -> list["CuringPress"]:
    return [unit for unit in fleet.idle_presses() if unit.mold_id is None]


def assign_next(
    fleet: "PressFleet",
    queue: DispatchQueue,
    registry: MoldRegistry,
    bus: EventBus,
    assigned_at: float,
    target_press_id: str | None = None,
) -> Assignment | None:
    """把队首任务分配给指定或编号最小的空闲机位；无任务或无机位时返回 None。"""

    idle = _available(fleet)
    if not idle:
        return None
    if target_press_id is not None:
        candidates = [unit for unit in idle if unit.press_id == target_press_id]
        if not candidates:
            return None
        target = candidates[0]
    else:
        target = idle[0]

    entry = queue.peek_next()
    if entry is None:
        return None
    registry.require_active(entry.mold_id)
    queue.pop_next()
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
