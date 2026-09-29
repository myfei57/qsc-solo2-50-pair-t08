"""机群调度顺序。

调度顺序固定为：完成上报、顺序确认、下一模分配、写审计。完成上报来自各台
硫化机，顺序确认把同一归集窗口内的并发上报排成可复现顺序，再据此向各上报
机位分配下一模，最后把这次派工写进审计。上报只入待结算队列，真正分配由
调度器统一结算，避免下一模跟着线程到达顺序漂移。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..batch.batch import BatchScheduler
from ..batch.ordering import ordering_keys
from ..batch.report import CompletionReport
from ..clock import Clock
from ..eventbus import Event, EventBus
from ..mold.registry import MoldRegistry
from ..press.fleet import PressFleet
from ..press.press import CuringPress
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
    settle_immediately: bool = False
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
        now = self.clock.now()

        report = self.scheduler.record_report(
            press.press_id,
            mold_id,
            product_code,
            now,
            generation,
            received_at=now,
        )
        self.recorder.note(
            "completion_report",
            "batch",
            press_id=press.press_id,
            mold_id=mold_id,
            sequence=report.sequence,
        )

        pending_before = list(self.scheduler.pending_reports())
        settled = self.scheduler.dispatch_ready(
            self.fleet,
            self.registry,
            now,
            include_all=self.settle_immediately,
        )
        remaining_sequences = {item["sequence"] for item in self.scheduler.pending_reports()}
        ordered = sorted(
            (
                CompletionReport(
                    press_id=item["press_id"],
                    mold_id=item["mold_id"],
                    product_code=item["product_code"],
                    completed_at=item["completed_at"],
                    curve_generation=item["curve_generation"],
                    sequence=item["sequence"],
                )
                for item in pending_before
                if item["sequence"] not in remaining_sequences
            ),
            key=lambda item: (
                item.completed_at,
                item.press_id,
                item.mold_id,
                item.curve_generation,
                item.sequence,
            ),
        )
        self.recorder.note(
            "order_confirm",
            "batch",
            confirmed=len(ordered),
            pending=settled["pending"],
            window_s=self.scheduler.limits.dispatch_window_s,
            settle_delay_s=self.scheduler.limits.settle_delay_s,
        )

        assignments = settled["assignments"]
        assignment = assignments[0] if assignments else None
        self.recorder.note(
            "mold_assigned",
            "batch",
            press_id=assignment["press_id"] if assignment else "",
            mold_id=assignment["mold_id"] if assignment else "",
            count=len(assignments),
        )

        self.bus.publish(
            Event.of(
                "batch",
                "batch",
                "dispatch_ordered",
                subject=mold_id,
                severity="info",
                ordered=[item.mold_id for item in ordered],
                ordering=ordering_keys(ordered),
                reports=settled["reports"],
                assignments=assignments,
                assigned=assignment,
            )
        )
        self.recorder.note("audit_note", "audit", mold_id=mold_id)
        return self.report(mold_id, assignments)

    def report(self, mold_id: str, assignments: list[dict]) -> dict:
        return {
            "mold_id": mold_id,
            "steps": self.recorder.names(),
            "violations": verify_order(self.recorder.names(), DISPATCH_ORDER),
            "queued": self.scheduler.queue.entries(),
            "assigned": assignments[0] if assignments else None,
            "assignments": assignments,
            "scheduler_phase": self.scheduler.phase.value,
        }
