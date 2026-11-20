"""HTTP-level tests: the page states the operator is supposed to see."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture()
def client(simulator_url, monkeypatch):
    monkeypatch.setenv("DRAIN_KUBE_MODE", "host")
    monkeypatch.setenv("DRAIN_KUBE_HOST", simulator_url)
    return TestClient(create_app())


def start(client: TestClient, node: str = "node-a") -> str:
    response = client.post("/drain", data={"node": node}, follow_redirects=False)
    assert response.status_code == 303
    return response.headers["location"].rsplit("/", 1)[-1]


def test_index_lists_nodes_and_cordon_button(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "node-a" in response.text
    assert "封锁并排空" in response.text


def test_round_page_separates_evictable_and_exempt_pods(client):
    round_id = start(client)
    page = client.get(f"/drain/{round_id}").text

    assert "本轮要驱逐的 Pod" in page
    assert "shop/web-0" in page
    assert "保留的 Pod（不驱逐）" in page
    assert "kube-system/log-agent-a" in page
    assert "DaemonSet" in page
    assert "静态 Pod" in page


def test_blocked_pods_are_flagged_and_node_is_not_offline(client, sim):
    round_id = start(client)
    sim.post("/api/sim/budgets/shop/web-pdb", json={"minAvailable": 4})
    client.post(f"/drain/{round_id}/run", follow_redirects=False)

    page = client.get(f"/drain/{round_id}").text
    assert "受预算阻挡" in page
    assert "尚不可下线" in page
    assert "待退出" in page

    payload = client.get("/api/rounds").json()[0]
    assert payload["offline"] is False
    assert payload["counts"]["blocked"] >= 1
    assert payload["counts"]["waiting"] >= 1


def test_offline_after_pods_exit_and_recheck(client, sim):
    round_id = start(client)
    client.post(f"/drain/{round_id}/run", follow_redirects=False)
    assert "尚不可下线" in client.get(f"/drain/{round_id}").text

    sim.post("/api/sim/pods/exit-terminating")
    client.post(f"/drain/{round_id}/recheck", follow_redirects=False)

    page = client.get(f"/drain/{round_id}").text
    assert "可下线" in page
    assert "已确认：节点不可调度，且已无普通 Pod" in page
    assert client.get("/api/rounds").json()[0]["offline"] is True


def test_interrupt_message_and_resume(client):
    round_id = start(client)
    client.post(f"/drain/{round_id}/run", follow_redirects=False)
    client.post(f"/drain/{round_id}/interrupt", follow_redirects=False)

    page = client.get(f"/drain/{round_id}").text
    assert "本轮已中断" in page
    assert "节点封锁和已经发生的驱逐都保留" in page
    assert "继续排空（重新读取集群）" in page

    client.post(f"/drain/{round_id}/run", follow_redirects=False)
    assert client.get("/api/rounds").json()[0]["phase"] in ("running", "complete")


def test_unknown_round_renders_notice(client):
    response = client.get("/drain/nope")
    assert response.status_code == 200
    assert "无法显示排空轮次" in response.text
    assert "不存在" in response.text


def test_index_offers_crash_log_capture(client):
    page = client.get("/").text
    assert "抓下次重启日志" in page
    assert "命名空间" in page

    page = client.get("/?ns=shop&pod=web-0").text
    assert 'name="container"' in page
    assert "开始抓取" in page


def test_arm_capture_and_show_captured_log(client, sim):
    response = client.post(
        "/captures",
        data={"namespace": "shop", "pod": "web-0", "container": "app", "delay_seconds": "0"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    location = response.headers["location"]
    capture_id = location.rsplit("capture=", 1)[-1]
    assert f"capture={capture_id}" in location

    sim.post(
        "/api/sim/pods/shop/web-0/crash",
        json={"container": "app", "text": "panic: disk full", "reason": "Error", "exit_code": 2},
    )

    import time

    payload = {}
    deadline = time.time() + 8
    while time.time() < deadline:
        payload = client.get(f"/api/captures/{capture_id}").json()
        if payload["state"] not in ("waiting", "reading"):
            break
        time.sleep(0.05)
    assert payload["state"] == "captured"
    assert "panic: disk full" in payload["logs"]
    assert payload["exit_reason"] == "Error"

    page = client.get(f"/captures/{capture_id}").text
    assert "已抓到本次退出的日志" in page
    assert "panic: disk full" in page
    assert "退出原因" in page


def test_capture_form_validation_error_is_shown(client):
    response = client.post(
        "/captures",
        data={"namespace": "shop", "pod": "", "container": "", "delay_seconds": "0"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "capture_error" in response.headers["location"]


def test_drain_stays_usable_while_capture_waits(client, sim):
    """A waiting capture must not block the drain controls."""
    response = client.post(
        "/captures",
        data={"namespace": "shop", "pod": "web-0", "container": "app", "delay_seconds": "0"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    round_id = start(client)
    assert round_id
    page = client.get(f"/drain/{round_id}").text
    assert "本轮要驱逐的 Pod" in page
