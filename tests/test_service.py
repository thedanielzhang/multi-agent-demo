from __future__ import annotations

from fastapi.testclient import TestClient

from mafia.config import AppConfig, ModeProfile
from mafia.service import create_app


def _receive_until(websocket, expected_type: str, *, key: str | None = None, value=None, limit: int = 20):
    for _ in range(limit):
        payload = websocket.receive_json()
        if payload.get("type") != expected_type:
            continue
        if key is None or payload.get(key) == value:
            return payload
    raise AssertionError(f"did not receive {expected_type!r}")


def _service_config(config: AppConfig, *, mode: ModeProfile | None = None, with_agents: bool = True) -> AppConfig:
    service_config = config.model_copy(deep=True)
    if mode is not None:
        service_config.mode = mode
    if not with_agents:
        service_config.agents = []
        service_config.topic.enabled = False
    else:
        service_config.agents = service_config.agents[:1]
        service_config.agents[0].scheduler.tick_rate_seconds = 0.05
        service_config.agents[0].generation.tick_rate_seconds = 0.05
        service_config.agents[0].personality.talkativeness = 1.0
        service_config.agents[0].personality.confidence = 1.0
        service_config.chat.typing_words_per_second = 100.0
    service_config.chat.max_duration_seconds = 10.0
    service_config.chat.max_messages = 100
    return service_config


def test_two_humans_receive_same_committed_message(baseline_config):
    app = create_app(_service_config(baseline_config, with_agents=False))
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws_one, client.websocket_connect("/ws") as ws_two:
            ws_one.send_json({"type": "join", "participant_id": "u1", "display_name": "Alice"})
            ws_two.send_json({"type": "join", "participant_id": "u2", "display_name": "Bob"})
            assert ws_one.receive_json()["type"] == "join"
            assert ws_two.receive_json()["type"] == "join"

            start = client.post("/start")
            assert start.status_code == 200
            assert start.json()["run_state"] == "running"
            _receive_until(ws_one, "run_state_changed", key="state", value="running")
            _receive_until(ws_two, "run_state_changed", key="state", value="running")

            ws_one.send_json({"type": "send_message", "text": "hello team", "client_message_id": "m-human-1"})
            one = _receive_until(ws_one, "message_committed")
            two = _receive_until(ws_two, "message_committed")
            assert one["message"] == two["message"]
            assert one["message"]["participant_id"] == "u1"
            assert one["message"]["kind"] == "human"


def test_human_and_agent_messages_share_same_public_shape(improved_config):
    app = create_app(_service_config(improved_config, mode=ModeProfile.IMPROVED_BUFFERED_ASYNC, with_agents=True))
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as websocket:
            websocket.send_json({"type": "join", "participant_id": "u1", "display_name": "Alice"})
            assert websocket.receive_json()["type"] == "join"

            start = client.post("/start")
            assert start.status_code == 200
            _receive_until(websocket, "run_state_changed", key="state", value="running")

            websocket.send_json({"type": "send_message", "text": "thai sounds good", "client_message_id": "m-human-2"})
            human = _receive_until(websocket, "message_committed")
            assert human["message"]["kind"] == "human"

            agent = None
            for _ in range(30):
                candidate = _receive_until(websocket, "message_committed")
                if candidate["message"]["kind"] == "agent":
                    agent = candidate
                    break
            assert agent is not None
            assert set(human["message"].keys()) == set(agent["message"].keys())
            assert human["message"]["participant_id"] == "u1"
            assert agent["message"]["participant_id"] == improved_config.agents[0].id


def test_control_endpoints_drive_room_state(baseline_config):
    app = create_app(_service_config(baseline_config, with_agents=False))
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as websocket:
            websocket.send_json({"type": "join", "participant_id": "u1", "display_name": "Alice"})
            assert websocket.receive_json()["type"] == "join"

            assert client.get("/status").json()["run_state"] == "idle"
            assert client.post("/start").json()["run_state"] == "running"
            assert _receive_until(websocket, "run_state_changed", key="state", value="running")["state"] == "running"

            assert client.post("/pause").json()["run_state"] == "paused"
            assert _receive_until(websocket, "run_state_changed", key="state", value="paused")["state"] == "paused"

            assert client.post("/resume").json()["run_state"] == "running"
            assert _receive_until(websocket, "run_state_changed", key="state", value="running")["state"] == "running"

            assert client.post("/stop").json()["run_state"] == "stopped"
            assert _receive_until(websocket, "run_state_changed", key="state", value="stopped")["state"] == "stopped"


def test_pages_and_config_api_render(baseline_config):
    app = create_app(_service_config(baseline_config, with_agents=False))
    with TestClient(app) as client:
        chat_page = client.get("/")
        assert chat_page.status_code == 200
        assert "Mafia Chatroom" in chat_page.text

        config_page = client.get("/config")
        assert config_page.status_code == 200
        assert "Chatroom Config" in config_page.text

        draft_config = client.get("/api/config")
        assert draft_config.status_code == 200
        assert draft_config.json()["runtime"]["provider"] == "scripted"

        schema = client.get("/api/config/schema")
        assert schema.status_code == 200
        body = schema.json()
        assert "scripted" in body["runtime_providers"]
        assert ModeProfile.BASELINE_TIME_TO_TALK.value in body["modes"]


def test_draft_config_applies_on_next_room_start_only(baseline_config):
    config = _service_config(baseline_config, with_agents=False)
    app = create_app(config)
    with TestClient(app) as client:
        original = client.get("/api/config").json()
        assert original["chat"]["scenario"] == config.chat.scenario

        started = client.post("/start")
        assert started.status_code == 200
        assert started.json()["active_config"]["chat"]["scenario"] == config.chat.scenario

        updated = client.get("/api/config").json()
        updated["chat"]["scenario"] = "You are strangers figuring out after-work plans."
        saved = client.put("/api/config", json=updated)
        assert saved.status_code == 200
        assert saved.json()["config"]["chat"]["scenario"] == "You are strangers figuring out after-work plans."

        status_while_running = client.get("/status").json()
        assert status_while_running["draft_config"]["chat"]["scenario"] == "You are strangers figuring out after-work plans."
        assert status_while_running["active_config"]["chat"]["scenario"] == config.chat.scenario

        assert client.post("/stop").status_code == 200
        restarted = client.post("/start")
        assert restarted.status_code == 200
        assert restarted.json()["active_config"]["chat"]["scenario"] == "You are strangers figuring out after-work plans."


def test_invalid_provider_is_rejected_on_config_save(baseline_config):
    app = create_app(_service_config(baseline_config, with_agents=False))
    with TestClient(app) as client:
        updated = client.get("/api/config").json()
        updated["runtime"]["provider"] = "mystery"
        response = client.put("/api/config", json=updated)
        assert response.status_code == 400
        assert "Unsupported runtime" in response.json()["detail"]


def test_blank_room_limits_are_allowed_in_draft_config(baseline_config):
    app = create_app(_service_config(baseline_config, with_agents=False))
    with TestClient(app) as client:
        updated = client.get("/api/config").json()
        updated["chat"]["max_duration_seconds"] = None
        updated["chat"]["max_messages"] = None
        response = client.put("/api/config", json=updated)
        assert response.status_code == 200
        body = response.json()
        assert body["config"]["chat"]["max_duration_seconds"] is None
        assert body["config"]["chat"]["max_messages"] is None


def test_debug_events_stream_over_websocket(improved_config):
    app = create_app(_service_config(improved_config, mode=ModeProfile.IMPROVED_BUFFERED_ASYNC, with_agents=True))
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as websocket:
            websocket.send_json({"type": "join", "participant_id": "u1", "display_name": "Alice"})
            assert websocket.receive_json()["type"] == "join"

            start = client.post("/start")
            assert start.status_code == 200
            _receive_until(websocket, "run_state_changed", key="state", value="running")

            websocket.send_json({"type": "send_message", "text": "thai sounds good", "client_message_id": "m-human-debug"})
            debug = _receive_until(
                websocket,
                "debug_event",
                key="subject",
                value="debug.event.agent.call.completed",
                limit=80,
            )
            assert debug["event"]["worker_kind"] in {"scheduler", "generator", "analyzer"}
            assert "input_summary" in debug["event"]
            assert "output_summary" in debug["event"]


def test_observer_socket_receives_live_room_events_without_join(improved_config):
    app = create_app(_service_config(improved_config, mode=ModeProfile.IMPROVED_BUFFERED_ASYNC, with_agents=True))
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as observer, client.websocket_connect("/ws") as sender:
            sender.send_json({"type": "join", "participant_id": "u1", "display_name": "Alice"})
            assert sender.receive_json()["type"] == "join"

            start = client.post("/start")
            assert start.status_code == 200
            _receive_until(observer, "run_state_changed", key="state", value="running")
            _receive_until(sender, "run_state_changed", key="state", value="running")

            sender.send_json({"type": "send_message", "text": "thai sounds good", "client_message_id": "observer-live-1"})
            observed = _receive_until(observer, "message_committed")
            assert observed["message"]["text"] == "thai sounds good"
            assert observed["message"]["participant_id"] == "u1"


def test_messages_api_returns_committed_history(improved_config):
    app = create_app(_service_config(improved_config, mode=ModeProfile.IMPROVED_BUFFERED_ASYNC, with_agents=True))
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as websocket:
            websocket.send_json({"type": "join", "participant_id": "u1", "display_name": "Alice"})
            assert websocket.receive_json()["type"] == "join"

            assert client.post("/start").status_code == 200
            _receive_until(websocket, "run_state_changed", key="state", value="running")

            websocket.send_json({"type": "send_message", "text": "pizza maybe?", "client_message_id": "history-1"})
            _receive_until(websocket, "message_committed")

            history = client.get("/api/messages")
            assert history.status_code == 200
            body = history.json()
            assert any(message["client_message_id"] == "history-1" for message in body)
