"""机群调度器。

保存待投产队列与最近若干条完成上报，提供入队、排产、完成上报与有序回放
四件事。上报序号在调度器内统一分配，排序始终走可复现的键；控制台与顺序
自查都从这里取数。
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Iterable

from ..config import BatchLimits
from ..eventbus import Event, EventBus
from ..timer.curve import CurveBook
from .assign import Assignment, assign_entry
from .ordering import order_completions
from .profile import BatchProfile
from .queue import DispatchQueue, QueueEntry
from .report import CompletionReport, report_completion
from .state import SchedulerPhase, SchedulerStateMachine

if TYPE_CHECKING:  # pragma: no cover - 仅用于类型标注
    from ..mold.registry import MoldRegistry
    from ..press.fleet import PressFleet

_REPORT_MEMORY = 512


class BatchScheduler:
    """机群调度器。"""

    def __init__(self, bus: EventBus, limits: BatchLimits, curves: CurveBook) -> None:
        self.limits = limits
        self.bus = bus
        self.curves = curves
        self.queue = DispatchQueue(limits.max_queue)
        self.lock = threading.RLock()
        self._state = SchedulerStateMachine()
        self._reports: list[CompletionReport] = []
        self._report_sequence = 0
        self._settled_sequences: set[int] = set()

    @property
    def phase(self) -> SchedulerPhase:
        return self._state.phase

    def transition(self, target: SchedulerPhase) -> SchedulerPhase:
        return self._state.transition(target)

    def enqueue(self, profile: BatchProfile, enqueued_at: float, priority: int = 50) -> QueueEntry:
        """把一份配方入队。"""

        with self.lock:
            entry = self.queue.push(
                profile.mold_id,
                profile.product_code,
                profile.thickness_mm,
                enqueued_at,
                priority=priority,
            )
            if self.phase is SchedulerPhase.DRAINED:
                self.transition(SchedulerPhase.IDLE)
            return entry

    def record_report(
        self,
        press_id: str,
        mold_id: str,
        product_code: str,
        completed_at: float,
        curve_generation: int,
    ) -> CompletionReport:
        """登记一条完成上报（统一分配上报序号）。"""

        with self.lock:
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
            self._reports.append(report)
            if len(self._reports) > _REPORT_MEMORY:
                removed = self._reports[: len(self._reports) - _REPORT_MEMORY]
                del self._reports[: len(removed)]
                removed_sequences = {item.sequence for item in removed}
                self._settled_sequences.difference_update(removed_sequences)
            return report

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

    def unsettled_reports(self) -> list[CompletionReport]:
        """返回尚未参与完成派工的上报，按稳定次序排列。"""

        with self.lock:
            reports = [
                report for report in self._reports if report.sequence not in self._settled_sequences
            ]
        return order_completions(reports)

    def mark_reports_settled(self, sequences: Iterable[int]) -> None:
        """把指定上报序号标记为已结算（并发探测等非生产上报使用）。"""

        with self.lock:
            known = {report.sequence for report in self._reports}
            self._settled_sequences.update(
                sequence for sequence in sequences if sequence in known
            )

    def settle_dispatch(
        self,
        fleet: "PressFleet",
        registry: "MoldRegistry",
        now: float,
        *,
        window_s: float | None = None,
    ) -> dict:
        """按完成上报的稳定次序结算一批派工。

        已经空闲的完成机位先进入待结算集合。只要仍有在机硫化机，并且最早待结算
        上报还在聚合窗口内，就等待同一轮上报；窗口到点后按完成时刻、机号和上报
        序号依次派工。剩余空闲机位可继续消费队列，但不会抢占等待完成的机位。
        """

        settle_window = (
            self.limits.completion_settle_s
            if window_s is None
            else float(window_s)
        )
        with self.lock:
            self.transition(SchedulerPhase.DISPATCHING)
            pending = self.unsettled_reports()
            active_presses = [unit for unit in fleet.all() if unit.mold_id is not None]
            ready_press_ids = {
                unit.press_id
                for unit in fleet.all()
                if unit.mold_id is None and unit.phase.value in ("idle", "done")
            }
            ready_reports = [report for report in pending if report.press_id in ready_press_ids]

            hold = bool(active_presses and ready_reports and settle_window > 0)
            if hold:
                oldest = min(report.completed_at for report in ready_reports)
                hold = now - oldest < settle_window

            assignments: list[Assignment] = []
            settled_sequences: list[int] = []
            if hold:
                self.transition(SchedulerPhase.IDLE)
            else:
                reserved = set()
                for report in ready_reports:
                    if len(self.queue) > 0:
                        assignment = assign_entry(
                            fleet,
                            self.queue,
                            registry,
                            self.bus,
                            now,
                            target_press_id=report.press_id,
                        )
                        if assignment is not None:
                            assignments.append(assignment)
                            reserved.add(report.press_id)
                    settled_sequences.append(report.sequence)

                if len(self.queue) > 0:
                    spare = [
                        unit
                        for unit in fleet.all()
                        if unit.mold_id is None
                        and unit.phase.value == "idle"
                        and unit.press_id not in reserved
                        and not any(
                            report.press_id == unit.press_id
                            for report in pending
                            if report.sequence not in settled_sequences
                        )
                    ]
                    for unit in spare:
                        assignment = assign_entry(
                            fleet,
                            self.queue,
                            registry,
                            self.bus,
                            now,
                            target_press_id=unit.press_id,
                        )
                        if assignment is None:
                            break
                        assignments.append(assignment)

                self._settled_sequences.update(settled_sequences)
                has_held_press = any(
                    unit.mold_id is not None
                    for unit in fleet.all()
                )
                if len(self.queue) == 0 and not has_held_press:
                    self.transition(SchedulerPhase.DRAINED)
                else:
                    self.transition(SchedulerPhase.IDLE)

            return {
                "held": hold,
                "settle_window_s": round(settle_window, 3),
                "pending": [report.as_dict() for report in pending],
                "ready": [report.as_dict() for report in ready_reports],
                "settled_sequences": settled_sequences,
                "assignments": [assignment.as_dict() for assignment in assignments],
            }

    def drain(self, drained_at: float) -> dict:
        """把队列清空并进入排空相位。"""

        with self.lock:
            if self.phase is not SchedulerPhase.DRAINED:
                self.transition(SchedulerPhase.DISPATCHING)
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
                "unsettled_reports": [report.as_dict() for report in self.unsettled_reports()],
                "recent_reports": [report.as_dict() for report in self._reports[-10:]],
            }
