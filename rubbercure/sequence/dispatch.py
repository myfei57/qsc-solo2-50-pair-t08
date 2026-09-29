"""机群调度顺序。

调度顺序固定为：完成上报、顺序确认、下一模分配、写审计。完成上报来自各台
硫化机，顺序确认把并发上报排成可复现顺序，再据此挑空闲机位分配下一模，
最后把这次派工写进审计。顺序确认必须先于分配，否则下一模会跟着线程调度
随机挑机。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..batch.batch import BatchScheduler
from ..batch.ordering import order_completions, ordering_keys
from ..clock import Clock
from ..eventbus import Event, EventBus
from ..mold.registry import MoldRegistry
from ..press.fleet import PressFleet
from ..press.press import CuringPress
from ..press.transitions import PressPhase
from ..timer.latch import CureLatch
from .selfcheck import SequenceRecorder, verify_order

DISPATCH_ORDER: tuple[tuple[str, str, str], ...] = (
    ("completion_report", "order_confirm", "完成上报后才做顺序确认"),
    ("order_confirm", "mold_assigned", "顺序确认后才分配下一模"),
    ("mold_assigned", "audit_note", "分配之后才写调度审计"),
)


@dataclass
class DispatchSequence:
    """机群调度顺序执行器。"""

    clock: Clock
    bus: EventBus
    registry: MoldRegistry
    scheduler: BatchScheduler
    fleet: PressFleet
    press: CuringPress
    latch: CureLatch
    recorder: SequenceRecorder = field(init=False)

    def __post_init__(self) -> None:
        self.recorder = SequenceRecorder(self.bus, "dispatch")

    def run(self) -> dict:
        """按调度顺序执行，返回执行报告。"""

        self.recorder.clear()
        press = self.press
        timer = press.timer.snapshot()
        mold_id = str(timer.get("mold_id") or press.press_id)
        product_code = str(timer.get("product_code") or "")
        generation = int(timer.get("curve_generation") or 0)

        report = self.scheduler.record_report(
            press.press_id,
            mold_id,
            product_code,
            self.clock.now(),
            generation,
        )
        self.recorder.note(
            "completion_report",
            "batch",
            press_id=press.press_id,
            mold_id=mold_id,
            sequence=report.sequence,
        )
        self.latch.reset(mold_id, self.clock.now())
        self.press.clear_mold()
        if self.press.phase is PressPhase.DONE:
            self.press.idle()
        self.press.timer.reset(mold_id)

        pending_before = self.scheduler.unsettled_reports()
        ordered = order_completions(
            pending_before,
            None,
        )
        self.recorder.note(
            "order_confirm",
            "batch",
            confirmed=len(ordered),
            window_s=self.scheduler.limits.completion_settle_s,
        )

        settlement = self.scheduler.settle_dispatch(
            self.fleet,
            self.registry,
            self.clock.now(),
        )
        assignments = settlement["assignments"]
        assignment = next(
            (item for item in assignments if item["press_id"] == press.press_id),
            assignments[0] if assignments else None,
        )
        self.recorder.note(
            "mold_assigned",
            "batch",
            press_id=assignment["press_id"] if assignment else "",
            mold_id=assignment["mold_id"] if assignment else "",
            held=settlement["held"],
            assigned_count=len(assignments),
        )

        pending_after = self.scheduler.unsettled_reports()
        self.bus.publish(
            Event.of(
                "batch",
                "batch",
                "dispatch_ordered",
                subject=mold_id,
                severity="info",
                ordered=[item.mold_id for item in ordered],
                ordering=ordering_keys(ordered),
                held=settlement["held"],
                assignments=assignments,
                assignment=assignment,
                pending=[item.as_dict() for item in pending_after],
            )
        )
        self.recorder.note("audit_note", "audit", mold_id=mold_id)
        return self.report(mold_id, assignment, settlement)

    def report(self, mold_id: str, assignment, settlement: dict | None = None) -> dict:
        settlement = settlement or {}
        return {
            "mold_id": mold_id,
            "steps": self.recorder.names(),
            "violations": verify_order(self.recorder.names(), DISPATCH_ORDER),
            "queued": self.scheduler.queue.entries(),
            "assigned": assignment,
            "assignments": settlement.get("assignments", [] if assignment is None else [assignment]),
            "held": settlement.get("held", False),
            "scheduler_phase": self.scheduler.phase.value,
        }
