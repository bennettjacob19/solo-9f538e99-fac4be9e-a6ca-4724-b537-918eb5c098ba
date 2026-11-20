"""Capture the log of the *next* crash of a container.

A container that crash-loops overwrites ``previous`` logs on every restart, so
by the time an operator opens the page only the most recent exit is visible.
This module lets the operator arm a capture first, then waits for the restart
count to move by exactly one and reads the ``previous`` log of the instance
that just exited.

Attribution is the whole point.  A log is only shown when all three identity
marks agree with the moment the restart was observed:

* the Pod UID (a same-named re-created Pod is a different object),
* the container restart count (exactly baseline + 1, never skipped or rolled
  back), and
* the last-exited container ID.

Those three are re-read immediately before and immediately after the log
fetch.  If either check disagrees — the Pod was re-created, the counter moved
again, or the exited container changed — the log is discarded and the capture
is reported as unconfirmed.  The operator is told that attribution failed
rather than being handed a log that may belong to a different container
instance.

The capture runs on a background thread, so the drain console stays fully
usable while it waits.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

from .kube import KubeClient, KubeError
from .models import ContainerSnapshot

DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_POLL_SECONDS = 0.4
LOG_TAIL_LINES = 100
MAX_DELAY_SECONDS = 30
MAX_KEPT_CAPTURES = 50


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _created_key(cap: "Capture") -> str:
    # The created_at stamp has one-second resolution; fall back to the ID as a
    # stable tie-breaker so ordering never flaps between renders.
    return f"{cap.created_at}-{cap.capture_id}"


class CaptureState(str, Enum):
    WAITING = "waiting"
    READING = "reading"
    CAPTURED = "captured"
    UNCONFIRMED = "unconfirmed"
    TIMEOUT = "timeout"
    ERROR = "error"

    @property
    def label(self) -> str:
        return {
            CaptureState.WAITING: "等待重启",
            CaptureState.READING: "已观察到重启，读取日志",
            CaptureState.CAPTURED: "已抓到崩溃日志",
            CaptureState.UNCONFIRMED: "无法确认归属",
            CaptureState.TIMEOUT: "等待超时",
            CaptureState.ERROR: "API 读取失败",
        }[self]

    @property
    def tone(self) -> str:
        return {
            CaptureState.WAITING: "info",
            CaptureState.READING: "warn",
            CaptureState.CAPTURED: "ok",
            CaptureState.UNCONFIRMED: "danger",
            CaptureState.TIMEOUT: "warn",
            CaptureState.ERROR: "danger",
        }[self]

    @property
    def pending(self) -> bool:
        return self in (CaptureState.WAITING, CaptureState.READING)

    @property
    def keeps_logs(self) -> bool:
        return self is CaptureState.CAPTURED


@dataclass
class Capture:
    namespace: str
    pod: str
    container: str
    capture_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    state: CaptureState = CaptureState.WAITING
    message: str = ""
    baseline: ContainerSnapshot | None = None
    observed: ContainerSnapshot | None = None
    logs: str = ""
    delay_seconds: float = 0.0
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    created_at: str = field(default_factory=_now)
    finished_at: str | None = None
    checks: list[str] = field(default_factory=list)

    # -- views -------------------------------------------------------------
    @property
    def key(self) -> str:
        return f"{self.namespace}/{self.pod}"

    @property
    def target_restart_count(self) -> int | None:
        return None if self.baseline is None else self.baseline.restart_count + 1

    @property
    def container_id(self) -> str:
        return self.observed.last_container_id if self.observed else ""

    @property
    def exit_reason(self) -> str:
        return self.observed.last_reason if self.observed else ""

    @property
    def exit_code(self) -> int | None:
        return self.observed.last_exit_code if self.observed else None

    @property
    def exit_message(self) -> str:
        return self.observed.last_message if self.observed else ""

    @property
    def pod_uid(self) -> str:
        return self.observed.pod_uid if self.observed else (
            self.baseline.pod_uid if self.baseline else ""
        )

    @property
    def pending(self) -> bool:
        return self.state.pending

    @property
    def short_container_id(self) -> str:
        raw = self.container_id.split("://", 1)[-1]
        return raw[:12] if raw else ""


def _tail(text: str, lines: int) -> str:
    parts = text.splitlines()
    if len(parts) <= lines:
        return text
    return "\n".join(parts[-lines:])


class CaptureManager:
    """Owns armed captures and their background watchers."""

    def __init__(
        self,
        kube: KubeClient,
        *,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        delay_seconds: float = 0.0,
    ):
        self.kube = kube
        self.timeout_seconds = timeout_seconds
        self.poll_seconds = poll_seconds
        self.default_delay_seconds = delay_seconds
        self._captures: dict[str, Capture] = {}
        self._lock = threading.RLock()

    # -- queries -----------------------------------------------------------
    def get(self, capture_id: str) -> Capture | None:
        with self._lock:
            return self._captures.get(capture_id)

    def recent(self, limit: int = 8) -> list[Capture]:
        """Most recent captures, pending ones first (they are what is changing)."""
        with self._lock:
            items = list(self._captures.values())
        pending = sorted((c for c in items if c.pending), key=_created_key)
        done = sorted(
            (c for c in items if not c.pending),
            key=_created_key,
            reverse=True,
        )
        return (pending + done)[:limit]

    # -- arming ------------------------------------------------------------
    def start(
        self,
        namespace: str,
        pod: str,
        container: str,
        delay_seconds: float | None = None,
    ) -> Capture:
        """Read the baseline synchronously, then watch on a background thread.

        The baseline read is the moment the operator committed to "next
        restart".  If it fails, the capture is returned in the ``error``
        state and nothing is armed.
        """
        delay = self.default_delay_seconds if delay_seconds is None else delay_seconds
        delay = max(0.0, min(float(delay), MAX_DELAY_SECONDS))
        cap = Capture(
            namespace=namespace,
            pod=pod,
            container=container,
            delay_seconds=delay,
            timeout_seconds=self.timeout_seconds,
        )
        with self._lock:
            self._captures[cap.capture_id] = cap
            self._prune()

        try:
            baseline = self.kube.container_snapshot(namespace, pod, container)
        except KubeError as exc:
            self._finish(cap, CaptureState.ERROR, f"读取 Pod 状态失败：{exc}")
            return cap
        if baseline is None:
            self._finish(
                cap, CaptureState.ERROR, f"Pod {namespace}/{pod} 不存在，无法开始抓取。"
            )
            return cap

        cap.baseline = baseline
        self._check(
            cap,
            f"基线：Pod UID {baseline.pod_uid[:8]}，容器 {container} "
            f"重启次数 {baseline.restart_count}，"
            f"上次退出容器 ID {baseline.last_container_id or '（无）'}",
        )
        if delay:
            self._check(cap, f"下一次日志读取将延迟 {delay:g} 秒。")
        cap.state = CaptureState.WAITING
        cap.message = (
            f"已开始等待：{self.timeout_seconds:g} 秒内 "
            f"{namespace}/{pod} 的容器 {container} 重启次数由 "
            f"{baseline.restart_count} 变为 {baseline.restart_count + 1} 时抓取日志。"
        )
        threading.Thread(
            target=self._watch, args=(cap,), name=f"capture-{cap.capture_id}", daemon=True
        ).start()
        return cap

    # -- watcher -----------------------------------------------------------
    def _watch(self, cap: Capture) -> None:
        try:
            observed = self._wait_for_restart(cap)
            if observed is not None:
                self._read_and_verify(cap, observed)
        except KubeError as exc:
            self._finish(cap, CaptureState.ERROR, f"读取 Kubernetes API 失败：{exc}")
        except Exception as exc:  # pragma: no cover - defensive
            self._finish(cap, CaptureState.ERROR, f"抓取过程异常：{exc}")

    def _wait_for_restart(self, cap: Capture) -> ContainerSnapshot | None:
        assert cap.baseline is not None
        baseline = cap.baseline
        target = baseline.restart_count + 1
        deadline = time.monotonic() + cap.timeout_seconds
        last_note = ""
        while True:
            snap = self.kube.container_snapshot(cap.namespace, cap.pod, cap.container)
            if snap is None:
                self._finish(
                    cap,
                    CaptureState.UNCONFIRMED,
                    f"Pod {cap.key} 已不存在（可能已删除或同名重建），"
                    "无法确认重启属于原 Pod，已丢弃本次日志。",
                )
                return None
            if snap.pod_uid != baseline.pod_uid:
                self._finish(
                    cap,
                    CaptureState.UNCONFIRMED,
                    f"Pod 同名重建：UID {baseline.pod_uid[:8]} → "
                    f"{snap.pod_uid[:8]}，无法确认重启属于原 Pod，已丢弃本次日志。",
                )
                return None
            if snap.restart_count == target:
                if not snap.last_container_id:
                    self._finish(
                        cap,
                        CaptureState.UNCONFIRMED,
                        "重启次数已加一，但 API 未给出上次退出的容器 ID，"
                        "无法确认日志归属，已丢弃本次日志。",
                    )
                    return None
                self._check(
                    cap,
                    f"观察到重启次数 {baseline.restart_count} → {target}，"
                    f"Pod UID {snap.pod_uid[:8]} 一致，"
                    f"刚退出容器 ID {snap.last_container_id}。",
                )
                cap.observed = snap
                cap.state = CaptureState.READING
                cap.message = (
                    "已观察到重启次数恰好加一，正在等待读取并核对日志归属。"
                )
                return snap
            if snap.restart_count > baseline.restart_count:
                # Anything past the target means this capture cannot say which
                # exit the ``previous`` log would belong to, whether it jumped
                # straight past it or moved by more than one.
                self._finish(
                    cap,
                    CaptureState.UNCONFIRMED,
                    f"重启次数从 {baseline.restart_count} 变为 {snap.restart_count}，"
                    f"跳过目标 {target}（可能连续重启），无法确认是哪一次退出的"
                    "日志，已丢弃本次日志。",
                )
                return None
            if snap.restart_count < baseline.restart_count:
                self._finish(
                    cap,
                    CaptureState.UNCONFIRMED,
                    f"重启次数从 {baseline.restart_count} 回退到 "
                    f"{snap.restart_count}，无法确认日志归属，已丢弃本次日志。",
                )
                return None
            note = (
                f"等待中：当前重启次数 {snap.restart_count}，目标 {target}。"
            )
            if note != last_note:
                cap.message = note
                last_note = note
            if time.monotonic() >= deadline:
                self._finish(
                    cap,
                    CaptureState.TIMEOUT,
                    f"等待超时：{cap.timeout_seconds:g} 秒内未观察到容器重启次数"
                    f"由 {baseline.restart_count} 加一。未读取任何日志。",
                )
                return None
            time.sleep(self.poll_seconds)

    def _read_and_verify(self, cap: Capture, observed: ContainerSnapshot) -> None:
        """Delay, then verify / read / verify again before showing anything."""
        if cap.delay_seconds:
            self._check(cap, f"延迟读取开始，{cap.delay_seconds:g} 秒后读取日志。")
            time.sleep(cap.delay_seconds)

        before = self.kube.container_snapshot(cap.namespace, cap.pod, cap.container)
        problem = self._attribution_problem(before, observed, "读取日志前")
        if problem:
            self._finish(
                cap,
                CaptureState.UNCONFIRMED,
                f"{problem}，无法确认日志归属，已丢弃本次日志。",
            )
            return

        try:
            raw = self.kube.previous_logs(
                cap.namespace,
                cap.pod,
                cap.container,
                tail_lines=LOG_TAIL_LINES,
            )
        except KubeError as exc:
            self._finish(cap, CaptureState.ERROR, f"读取 previous 日志失败：{exc}")
            return

        after = self.kube.container_snapshot(cap.namespace, cap.pod, cap.container)
        problem = self._attribution_problem(after, observed, "读取日志期间")
        if problem:
            self._finish(
                cap,
                CaptureState.UNCONFIRMED,
                f"{problem}，无法确认日志归属，已丢弃本次日志。",
            )
            return

        cap.logs = _tail(raw, LOG_TAIL_LINES)
        self._check(
            cap,
            "读取前后核对通过：Pod UID、重启次数、上次退出容器 ID 均与观察一致。",
        )
        self._finish(
            cap,
            CaptureState.CAPTURED,
            f"已抓到 {cap.container} 上一次退出的日志（末尾 "
            f"{len(cap.logs.splitlines())} 行）。",
        )

    @staticmethod
    def _attribution_problem(
        snap: ContainerSnapshot | None,
        observed: ContainerSnapshot,
        phase: str,
    ) -> str | None:
        if snap is None:
            return f"{phase} Pod 已不存在"
        if snap.pod_uid != observed.pod_uid:
            return (
                f"{phase} Pod UID 变化（{observed.pod_uid[:8]} → "
                f"{snap.pod_uid[:8]}），Pod 可能已同名重建"
            )
        if snap.restart_count != observed.restart_count:
            return (
                f"{phase} 又发生了一次重启（重启次数 {observed.restart_count} → "
                f"{snap.restart_count}）"
            )
        if snap.last_container_id != observed.last_container_id:
            return (
                f"{phase} 上次退出的容器 ID 已变化"
                f"（{observed.last_container_id} → "
                f"{snap.last_container_id or '（无）'}）"
            )
        return None

    def _prune(self) -> None:
        """Forget the oldest finished captures.  Must hold the lock."""
        if len(self._captures) <= MAX_KEPT_CAPTURES:
            return
        finished = sorted(
            (c for c in self._captures.values() if not c.pending),
            key=lambda c: c.created_at,
        )
        for cap in finished[: max(0, len(self._captures) - MAX_KEPT_CAPTURES)]:
            self._captures.pop(cap.capture_id, None)

    # -- helpers -----------------------------------------------------------
    def _check(self, cap: Capture, text: str) -> None:
        with self._lock:
            cap.checks.append(f"{_now()} · {text}")

    def _finish(self, cap: Capture, state: CaptureState, message: str) -> None:
        with self._lock:
            cap.state = state
            cap.message = message
            cap.finished_at = _now()
            if not state.keeps_logs:
                # Never leave a log behind on an unconfirmed capture: the page
                # must not be able to show a log whose attribution failed.
                cap.logs = ""
            cap.checks.append(f"{_now()} · {state.label}：{message}")
