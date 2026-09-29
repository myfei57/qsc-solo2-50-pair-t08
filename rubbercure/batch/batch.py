"""机群调度器。

保存待投产队列、待结算完成上报与最近若干条完成记录，提供入队、插单、完成
上报、按稳定顺序结算四件事。上报序号在调度器内统一分配；真正派工只在
:meth:`BatchScheduler.dispatch_ready` 中发生，因此同一份上报集合无论线程谁先
进入，都会按完成时刻、机号、上报序号形成同一条派工结果。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..config import BatchLimits
from ..eventbus import Event, EventBus
from ..timer.curve import CurveBook
from .assign import Assignment, assign_next
from .ordering import order_completions
from .profile import BatchProfile
from .queue import URGENT_PRIORITY, DispatchQueue, QueueEntry
from .report import CompletionReport, report_completion
from .state import SchedulerPhase, SchedulerStateMachine

_REPORT_MEMORY = 512

if TYPE_CHECKING:  # pragma: no cover - 仅用于类型标注，避免环
    from ..mold.registry import MoldRegistry
    from ..press.fleet import PressFleet


@dataclass(frozen=True)
class PendingCompletion:
    """尚未触发派工的完成上报。"""

    report: CompletionReport
    received_at: float

    def key(self) -> tuple:
        return (
            self.report.completed_at,
            self.report.press_id,
            self.report.mold_id,
            self.report.curve_generation,
            self.report.sequence,
        )


class BatchScheduler:
    """机群调度器。"""

    def __init__(self, bus: EventBus, limits: BatchLimits, curves: CurveBook) -> None:
        self.limits = limits
        self.bus = bus
        self.curves = curves
        self.queue = DispatchQueue(limits.max_queue, limits.max_urgent_queue)
        self.lock = threading.RLock()
        self._state = SchedulerStateMachine()
        self._reports: list[CompletionReport] = []
        self._pending: list[PendingCompletion] = []
        self._report_sequence = 0

    @property
    def phase(self) -> SchedulerPhase:
        return self._state.phase

    def transition(self, target: SchedulerPhase) -> SchedulerPhase:
        return self._state.transition(target)

    def _activate(self) -> None:
        if self._state.phase is SchedulerPhase.DRAINED:
            self.transition(SchedulerPhase.IDLE)

    def enqueue(
        self,
        profile: BatchProfile,
        enqueued_at: float,
        priority: int = 50,
        urgent: bool = False,
    ) -> QueueEntry:
        """把一份配方入队；加急配方使用独立插单容量。"""

        with self.lock:
            self._activate()
            return self.queue.push(
                profile.mold_id,
                profile.product_code,
                profile.thickness_mm,
                enqueued_at,
                priority=priority,
                urgent=urgent,
            )

    def enqueue_urgent(
        self,
        profile: BatchProfile,
        enqueued_at: float,
        priority: int = URGENT_PRIORITY,
    ) -> QueueEntry:
        """把一份配方作为插单排入加急队列。"""

        with self.lock:
            self._activate()
            return self.queue.push_urgent(
                profile.mold_id,
                profile.product_code,
                profile.thickness_mm,
                enqueued_at,
                priority=priority,
            )

    def record_report(
        self,
        press_id: str,
        mold_id: str,
        product_code: str,
        completed_at: float,
        curve_generation: int,
        received_at: float | None = None,
        track_pending: bool = True,
    ) -> CompletionReport:
        """登记一条完成上报（统一分配上报序号），但暂不立即派工。"""

        with self.lock:
            self._activate()
            self._report_sequence += 1
            sequence = self._report_sequence
            report = report_completion(
                press_id,
                mold_id,
                product_code,
                completed_at,
                curve_generation,
                sequence,
                self.bus,
            )
            moment = completed_at if received_at is None else float(received_at)
            if track_pending:
                self._pending.append(PendingCompletion(report, moment))
            self._reports.append(report)
            if len(self._reports) > _REPORT_MEMORY:
                del self._reports[: len(self._reports) - _REPORT_MEMORY]
            return report

    def _report_is_ready(self, fleet: "PressFleet", report: CompletionReport) -> bool:
        """判断上报机位是否已经完成取件并可供下一模。"""

        try:
            unit = fleet.press(report.press_id)
        except Exception:
            return False
        current_mold = unit.mold_id
        if current_mold == report.mold_id:
            return False
        if current_mold is not None:
            return False
        timer_mold = unit.timer.snapshot().get("mold_id")
        return timer_mold in (None, report.mold_id) or report.mold_id == report.press_id

    def dispatch_ready(
        self,
        fleet: "PressFleet",
        registry: "MoldRegistry",
        now: float,
        *,
        include_all: bool = False,
    ) -> dict:
        """结算完成上报，并按固定顺序向各自机位派工。

        默认只结算完成时刻已超过归集延时的上报；手动结算可用
        ``include_all=True`` 立即结算当前已经取件的上报。
        """

        with self.lock:
            selected_reports: list[CompletionReport] = []
            assignments: list[Assignment] = []
            ready: list[PendingCompletion] = []
            stale_sequences: set[int] = set()

            for pending in self._pending:
                matured = include_all or (
                    now - pending.report.completed_at > self.limits.settle_delay_s
                )
                if not matured:
                    continue
                try:
                    unit = fleet.press(pending.report.press_id)
                    current_mold = unit.mold_id
                    if current_mold is not None and current_mold != pending.report.mold_id:
                        stale_sequences.add(pending.report.sequence)
                        continue
                    is_ready = self._report_is_ready(fleet, pending.report)
                except Exception:
                    continue
                if is_ready:
                    ready.append(pending)

            if ready:
                self._activate()
            assignments: list[Assignment] = []
            assigned_press_ids: set[str] = set()
            for pending in sorted(ready, key=lambda item: item.key()):
                if pending.report.press_id in assigned_press_ids:
                    continue
                assignment = assign_next(
                    fleet,
                    self.queue,
                    registry,
                    self.bus,
                    now,
                    target_press_id=pending.report.press_id,
                )
                if assignment is not None:
                    assignments.append(assignment)
                    assigned_press_ids.add(assignment.press_id)

            if assignments:
                self.transition(SchedulerPhase.DISPATCHING)

            for pending in sorted(ready, key=lambda item: item.key()):
                selected_reports.append(pending.report)

            self._pending = [
                pending
                for pending in self._pending
                if pending.report.sequence not in stale_sequences
                and pending.report.sequence not in {item.sequence for item in selected_reports}
            ]

            if selected_reports and not self.queue and not self._pending:
                self.transition(SchedulerPhase.DRAINED)
            elif ready:
                self.transition(SchedulerPhase.IDLE)
            self.bus.publish(
                Event.of(
                    "batch",
                    "batch",
                    "dispatch_batch_settled",
                    subject="fleet-scheduler",
                    severity="info",
                    reports=[item.as_dict() for item in selected_reports],
                    assignments=[item.as_dict() for item in assignments],
                    settle_delay_s=self.limits.settle_delay_s,
                    forced=include_all,
                )
            )
            return {
                "reports": [item.as_dict() for item in selected_reports],
                "assignments": [item.as_dict() for item in assignments],
                "pending": len(self._pending),
            }

    def ordered_reports(self, limit: int = 20, window_s: float | None = None) -> list[dict]:
        """按可复现顺序返回最近的上报。"""

        with self.lock:
            reports = list(self._reports[-limit:])
        window = self.limits.dispatch_window_s if window_s is None else window_s
        return [report.as_dict() for report in order_completions(reports, window)]

    def recent_reports(self, limit: int = 20) -> list[CompletionReport]:
        """返回最近的上报（保持原始记录，供排序与审计使用）。"""

        with self.lock:
            return list(self._reports[-limit:])

    def pending_reports(self) -> list[dict]:
        """返回等待结算的完成上报，顺序与派工时采用的顺序一致。"""

        with self.lock:
            pending = sorted(self._pending, key=lambda item: item.key())
            return [item.report.as_dict() for item in pending]

    def drain(self, drained_at: float) -> dict:
        """把队列清空并进入排空相位。"""

        with self.lock:
            self.transition(SchedulerPhase.DRAINED)
            removed = 0
            while self.queue.pop_next() is not None:
                removed += 1
        self.bus.publish(
            Event.of("batch", "batch", "queue_drained", subject="dispatch-queue", removed=removed)
        )
        return {"removed": removed, "phase": self._state.phase.value, "at": round(drained_at, 3)}

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "phase": self._state.phase.value,
                "history": self._state.history(),
                "queue": self.queue.snapshot(),
                "report_sequence": self._report_sequence,
                "pending_reports": self.pending_reports(),
                "recent_reports": [report.as_dict() for report in self._reports[-10:]],
            }
