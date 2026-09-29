"""派工队列、插单容量与完成上报的稳定结算。"""

from __future__ import annotations

import threading
from pathlib import Path

from rubbercure.app import build_platform
from rubbercure.clock import ManualClock
from rubbercure.config import default_config
from rubbercure.errors import RubberCureError
from rubbercure.batch.queue import DispatchQueue


def _platform(tmp_path: Path):
    return build_platform(
        default_config(),
        clock=ManualClock(),
        storage_root=str(tmp_path / "state"),
    )


def test_priority_queue_orders_by_priority_then_fifo(tmp_path: Path):
    platform = _platform(tmp_path)
    platform.register_mold("low", cavity_count=2)
    platform.register_mold("early", cavity_count=2)
    platform.register_mold("late", cavity_count=2)

    platform.enqueue_mold("low", priority=10)
    platform.enqueue_mold("early", priority=80)
    platform.enqueue_mold("late", priority=80)

    assert [item["mold_id"] for item in platform.scheduler.queue.entries()] == [
        "early",
        "late",
        "low",
    ]


def test_urgent_queue_has_separate_capacity_and_overtakes(tmp_path: Path):
    platform = _platform(tmp_path)
    platform.register_mold("normal", cavity_count=2)
    platform.register_mold("rush", cavity_count=2)

    platform.enqueue_mold("normal", priority=90)
    queued = platform.enqueue_mold("rush", urgent=True, priority=90)

    assert queued["entry"]["urgent"] is True
    assert [item["mold_id"] for item in platform.scheduler.queue.entries()] == ["rush", "normal"]
    snapshot = platform.scheduler.queue.snapshot()
    assert snapshot["urgent_size"] == 1
    assert snapshot["urgent_capacity"] == platform.config.batch.max_urgent_queue


def test_urgent_queue_rejects_overflow_without_replacing_normal_work(tmp_path: Path):
    platform = _platform(tmp_path)
    for index in range(platform.config.batch.max_urgent_queue):
        mold_id = f"rush-{index}"
        platform.register_mold(mold_id, cavity_count=2)
        platform.enqueue_mold(mold_id, urgent=True)

    platform.register_mold("rush-overflow", cavity_count=2)
    try:
        platform.enqueue_mold("rush-overflow", urgent=True)
    except RubberCureError as error:
        assert "插单队列已满" in str(error)
    else:
        raise AssertionError("插单队列容量满后必须拒绝新插单")


def test_direct_priority_queue_is_total_order_and_validates_priority():
    queue = DispatchQueue(max_queue=2, max_urgent_queue=1)
    queue.push("normal-a", "RB-215", 6.0, 0.0, priority=50)
    queue.push("normal-b", "RB-215", 6.0, 1.0, priority=50)
    queue.push_urgent("rush", "RB-215", 6.0, 2.0, priority=50)

    assert [item["mold_id"] for item in queue.entries()] == ["rush", "normal-a", "normal-b"]
    assert [queue.pop_next().mold_id for _ in range(3)] == [
        "rush",
        "normal-a",
        "normal-b",
    ]
    try:
        queue.push("bad", "RB-215", 6.0, 3.0, priority=101)
    except RubberCureError as error:
        assert "优先级" in str(error)
    else:
        raise AssertionError("越界优先级必须拒绝")


def test_concurrent_completion_reports_deterministically_assign_to_reporting_presses(
    tmp_path: Path,
):
    platform = _platform(tmp_path)
    press_ids = platform.fleet.press_ids()
    for index in range(len(press_ids)):
        platform.register_mold(f"mold-{index}", cavity_count=2)
        platform.enqueue_mold(f"mold-{index}", priority=100 - index)

    def reporter(order: int) -> None:
        # 故意按机号逆序并发进入；结果不能依赖线程调度。
        press_id = press_ids[len(press_ids) - order - 1]
        platform.scheduler.record_report(
            press_id,
            f"finished-{press_id}",
            "RB-215",
            0.0,
            1,
            received_at=0.0,
        )

    threads = [threading.Thread(target=reporter, args=(index,)) for index in range(len(press_ids))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    settled = platform.settle_dispatch(include_all=True)
    assert [item["press_id"] for item in settled["reports"]] == press_ids
    assert [item["press_id"] for item in settled["assignments"]] == press_ids
    assert [item["mold_id"] for item in settled["assignments"]] == [
        f"mold-{index}" for index in range(len(press_ids))
    ]
    assert [platform.press(press_id).mold_id for press_id in press_ids] == [
        f"mold-{index}" for index in range(len(press_ids))
    ]


def test_dispatch_waiting_for_settle_delay_does_not_assign_early(tmp_path: Path):
    platform = _platform(tmp_path)
    platform.register_mold("next", cavity_count=2)
    platform.enqueue_mold("next")
    platform.scheduler.record_report(
        "press-01",
        "finished",
        "RB-215",
        platform.clock.now(),
        1,
        received_at=platform.clock.now(),
    )

    early = platform.settle_dispatch()
    assert early["assignments"] == []
    assert early["pending"] == 1
    platform.clock.advance(platform.config.batch.settle_delay_s + 0.1)
    matured = platform.settle_dispatch()
    assert matured["assignments"][0]["press_id"] == "press-01"
    assert matured["assignments"][0]["mold_id"] == "next"
