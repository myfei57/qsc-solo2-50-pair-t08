"""机群调度：优先队列、插单与并发完成上报的确定性派工。"""

from __future__ import annotations

from rubbercure.app import build_platform
from rubbercure.batch.queue import DispatchQueue
from rubbercure.clock import ManualClock
from rubbercure.config import default_config
from rubbercure.errors import RubberCureError
from rubbercure.press.transitions import PressPhase
from rubbercure.timer.state import TimerPhase


def test_dispatch_queue_orders_by_priority_then_fifo_sequence():
    queue = DispatchQueue(max_queue=4)
    queue.push("mold-a", "RB-215", 6.0, 1.0, priority=50)
    queue.push("mold-b", "RB-215", 6.0, 2.0, priority=90)
    queue.push("mold-c", "RB-215", 6.0, 3.0, priority=50)
    queue.push("mold-d", "RB-215", 6.0, 4.0, priority=70)

    assert [item["mold_id"] for item in queue.entries()] == [
        "mold-b",
        "mold-d",
        "mold-a",
        "mold-c",
    ]
    assert queue.pop_next().mold_id == "mold-b"
    assert queue.pop_next().mold_id == "mold-d"
    assert queue.pop_next().mold_id == "mold-a"
    assert queue.pop_next().mold_id == "mold-c"


def test_bounded_queue_rejects_insertion_when_full():
    queue = DispatchQueue(max_queue=1)
    queue.push("mold-a", "RB-215", 6.0, 1.0)

    try:
        queue.push("rush", "RB-215", 6.0, 2.0, priority=90)
    except RubberCureError as error:
        assert "队列已满" in error.message
    else:  # pragma: no cover - 未抛错即失败
        raise AssertionError("满队列必须拒绝插单")


def test_rush_insertion_jumps_ahead_of_existing_fifo_tasks(platform):
    for mold_id in ("normal-1", "normal-2", "rush"):
        platform.register_mold(mold_id, cavity_count=2)
    platform.enqueue_mold("normal-1", priority=50)
    platform.enqueue_mold("normal-2", priority=50)
    platform.enqueue_mold("rush", priority=90)

    assert [item["mold_id"] for item in platform.scheduler.queue.entries()] == [
        "rush",
        "normal-1",
        "normal-2",
    ]


def _prepare_queue_and_done_presses(platform):
    for mold_id in ("next-1", "next-2", "next-3"):
        platform.register_mold(mold_id, cavity_count=2)
        platform.enqueue_mold(mold_id, priority=50)

    for index, unit in enumerate(platform.fleet.all(), start=1):
        completed_mold = f"busy-{unit.press_id}"
        unit._state._phase = PressPhase.DONE
        unit._state._history.append(PressPhase.DONE)
        unit._mold_id = completed_mold
        unit.timer.begin(
            completed_mold,
            platform.config.press.default_product_code,
            platform.config.timer.default_nominal_s,
            1,
        )
        unit.timer._state.transition(TimerPhase.COMPLETE)


def _assignment_by_arrival_order(arrival_order, tmp_path):
    platform = build_platform(
        default_config(),
        clock=ManualClock(),
        storage_root=str(tmp_path / "-".join(str(item) for item in arrival_order)),
    )
    _prepare_queue_and_done_presses(platform)
    final = None
    for index in arrival_order:
        press_id = f"press-{index:02d}"
        final = platform.run_dispatch(press_id)
    return {(item["press_id"], item["mold_id"]) for item in final["assignments"]}


def test_simultaneous_completions_dispatch_in_stable_press_order(tmp_path):
    expected = {
        ("press-01", "next-1"),
        ("press-02", "next-2"),
        ("press-03", "next-3"),
    }

    assert _assignment_by_arrival_order((3, 2, 1), tmp_path) == expected
    assert _assignment_by_arrival_order((2, 3, 1), tmp_path) == expected
    assert _assignment_by_arrival_order((1, 2, 3), tmp_path) == expected


def test_completion_barrier_waits_until_report_set_settles(platform):
    _prepare_queue_and_done_presses(platform)

    first = platform.run_dispatch("press-03")
    assert first["held"] is True
    assert first["assigned"] is None

    second = platform.run_dispatch("press-02")
    assert second["held"] is True
    assert second["assigned"] is None

    third = platform.run_dispatch("press-01")
    assert third["held"] is False
    assert [(item["press_id"], item["mold_id"]) for item in third["assignments"]] == [
        ("press-01", "next-1"),
        ("press-02", "next-2"),
        ("press-03", "next-3"),
    ]
