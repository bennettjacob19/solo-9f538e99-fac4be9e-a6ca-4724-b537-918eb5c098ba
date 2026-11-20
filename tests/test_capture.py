"""Crash-log capture: attribution, timeout, API failure and delay."""

from __future__ import annotations

import threading
import time

import pytest

from app.capture import CaptureManager, CaptureState

POD = ("shop", "web-0", "app")


def wait_until_done(cap, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not cap.pending:
            return cap
        time.sleep(0.02)
    raise AssertionError(f"capture still {cap.state} after {timeout}s: {cap.message}")


@pytest.fixture()
def captures(kube):
    manager = CaptureManager(kube, timeout_seconds=4, poll_seconds=0.05)
    yield manager


def crash(sim, text="boom line\nsecond line", pod="web-0", namespace="shop", container="app", **extra):
    body = {"container": container, "text": text, "reason": "Error", "exit_code": 2}
    body.update(extra)
    response = sim.post(f"/api/sim/pods/{namespace}/{pod}/crash", json=body)
    assert response.status_code == 201, response.text
    return response.json()


def test_capture_returns_the_exit_log_and_identity(captures, sim):
    cap = captures.start(*POD)
    assert cap.state is CaptureState.WAITING
    assert cap.baseline is not None
    crash(sim, "first line\npanic: connection refused")

    wait_until_done(cap)
    assert cap.state is CaptureState.CAPTURED
    assert "panic: connection refused" in cap.logs
    # The identity shown comes from the observed snapshot, not the baseline.
    assert cap.container_id.startswith("containerd://")
    assert cap.exit_reason == "Error"
    assert cap.exit_code == 2
    assert cap.target_restart_count == 1


def test_tail_is_limited_to_last_100_lines(captures, sim):
    cap = captures.start(*POD)
    text = "\n".join(f"line-{index:03d}" for index in range(1, 251))
    crash(sim, text)

    wait_until_done(cap)
    assert cap.state is CaptureState.CAPTURED
    lines = cap.logs.splitlines()
    assert len(lines) == 100
    assert lines[-1] == "line-250"
    assert lines[0] == "line-151"


def test_timeout_is_reported_and_nothing_is_read(captures):
    captures.timeout_seconds = 0.3
    cap = captures.start(*POD)
    wait_until_done(cap)
    assert cap.state is CaptureState.TIMEOUT
    assert "等待超时" in cap.message
    assert cap.logs == ""


def test_api_failure_is_reported(kube, monkeypatch):
    def explode(*args, **kwargs):
        from app.kube import KubeError

        raise KubeError("连接被拒绝")

    monkeypatch.setattr(kube, "container_snapshot", explode)
    manager = CaptureManager(kube, timeout_seconds=2, poll_seconds=0.05)
    cap = manager.start(*POD)
    assert cap.state is CaptureState.ERROR
    assert "API 读取失败" in cap.message or "失败" in cap.message


def test_pod_recreated_with_same_name_is_unconfirmed(captures, sim):
    cap = captures.start(*POD)
    # Same name, new UID: the restart count of the new object is invisible to
    # this capture, and the baseline identity must not be reused.
    sim.post("/api/sim/pods/shop/web-0/replace", json={"node": "node-a"})

    wait_until_done(cap)
    assert cap.state is CaptureState.UNCONFIRMED
    assert "同名" in cap.message or "UID" in cap.message
    assert cap.logs == ""


def test_skipped_restart_count_is_unconfirmed(kube, sim):
    # Wait until the watcher has taken its first (count 0) sample, then let
    # three exits happen between two samples.  The next observation jumps
    # straight past the target count, so no single exit can be singled out.
    manager = CaptureManager(kube, timeout_seconds=6, poll_seconds=0.4)
    cap = manager.start(*POD)
    deadline = time.time() + 4
    while time.time() < deadline and "等待中" not in cap.message:
        time.sleep(0.01)
    assert "等待中" in cap.message
    crash(sim, "first")
    crash(sim, "second")
    crash(sim, "third")

    wait_until_done(cap)
    assert cap.state is CaptureState.UNCONFIRMED
    assert "跳过" in cap.message
    assert cap.logs == ""


def test_restart_during_log_read_discards_the_log(captures, sim):
    """The delayed read is interrupted by another restart: no log is kept."""
    captures.default_delay_seconds = 0.6
    cap = captures.start(*POD, delay_seconds=0.6)
    crash(sim, "the log we might have grabbed")

    # Wait for the capture to observe the first restart, then crash again while
    # it sits in the read delay.
    deadline = time.time() + 4
    while time.time() < deadline and cap.state is not CaptureState.READING:
        time.sleep(0.01)
    assert cap.state is CaptureState.READING
    crash(sim, "the second exit")

    wait_until_done(cap)
    assert cap.state is CaptureState.UNCONFIRMED
    assert "又发生了一次重启" in cap.message
    assert cap.logs == ""


def test_restart_while_the_api_reads_the_log_discards_it(captures, sim):
    """A restart that lands *during* the log fetch is caught by the re-check.

    The simulator blocks the log response so the second crash is guaranteed to
    happen between the pre-read and post-read identity checks.
    """
    sim.post("/api/sim/pods/shop/web-0/log-delay", json={"delay_ms": 1500})
    cap = captures.start(*POD, delay_seconds=0)
    crash(sim, "log from the first exit")

    deadline = time.time() + 4
    while time.time() < deadline and cap.state is not CaptureState.READING:
        time.sleep(0.01)
    assert cap.state is CaptureState.READING
    # The pre-read identity check is immediate; the log fetch then blocks for
    # 1.5s in the simulator, so this crash lands inside that window.
    time.sleep(0.4)
    crash(sim, "log from the second exit")

    wait_until_done(cap, timeout=10)
    assert cap.state is CaptureState.UNCONFIRMED
    assert "读取日志期间" in cap.message
    assert "又发生了一次重启" in cap.message
    assert cap.logs == ""


def test_log_read_error_is_reported_as_api_failure(kube, monkeypatch, sim):
    """A previous-log fetch failure is its own state, not a silent success."""

    def broken(*args, **kwargs):
        from app.kube import KubeError

        raise KubeError("读取 previous 日志失败: 500")

    monkeypatch.setattr(kube, "previous_logs", broken)
    manager = CaptureManager(kube, timeout_seconds=2, poll_seconds=0.05)
    cap = manager.start(*POD)
    crash(sim, "never shown")

    wait_until_done(cap)
    assert cap.state is CaptureState.ERROR
    assert "previous 日志" in cap.message
    assert cap.logs == ""
