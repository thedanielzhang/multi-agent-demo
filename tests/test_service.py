from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from mafia.config import AppConfig, ModeProfile, RoomMode
from mafia.service import ChatRoomService, create_app


def _receive_until(websocket, expected_type: str, *, key: str | None = None, value=None, limit: int = 20):
    for _ in range(limit):
        payload = websocket.receive_json()
        if payload.get("type") != expected_type:
            continue
        if key is None or payload.get(key) == value:
            return payload
    raise AssertionError(f"did not receive {expected_type!r}")


def _receive_message_until(websocket, predicate, *, limit: int = 40):
    for _ in range(limit):
        payload = websocket.receive_json()
        if payload.get("type") != "message_committed":
            continue
        message = payload.get("message", {})
        if predicate(message):
            return payload
    raise AssertionError("did not receive matching committed message")


def _receive_payload_until(websocket, predicate, *, limit: int = 40):
    for _ in range(limit):
        payload = websocket.receive_json()
        if predicate(payload):
            return payload
    raise AssertionError("did not receive matching websocket payload")


def _join_socket(websocket, participant_id: str, display_name: str):
    first = websocket.receive_json()
    assert first["type"] == "room_snapshot"
    websocket.send_json({"type": "join", "participant_id": participant_id, "display_name": display_name})
    joined = _receive_until(websocket, "join")
    assert joined["participant"]["participant_id"] == participant_id
    return joined


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


def _mafia_service_config(config: AppConfig) -> AppConfig:
    service_config = _service_config(config, mode=ModeProfile.IMPROVED_BUFFERED_ASYNC, with_agents=True)
    service_config.room_mode = RoomMode.MAFIA
    service_config.mode = ModeProfile.IMPROVED_BUFFERED_ASYNC
    service_config.topic.enabled = False
    service_config.chat.max_duration_seconds = None
    service_config.chat.max_messages = None
    service_config.mafia.total_players = 5
    service_config.mafia.day_discussion_seconds = 1.0
    service_config.mafia.day_vote_seconds = 1.0
    service_config.mafia.day_reveal_seconds = 0.2
    service_config.mafia.night_action_seconds = 1.0
    service_config.mafia.night_reveal_seconds = 0.2
    return service_config


def test_app_config_defaults_regular_rooms_to_improved_mode():
    config = AppConfig(
        runtime={"provider": "scripted"},
        chat={"scenario": "You are coworkers planning lunch."},
        agents=[],
    )
    assert config.room_mode == RoomMode.REGULAR
    assert config.mode == ModeProfile.IMPROVED_BUFFERED_ASYNC
    assert config.authority.human_users_authoritative is True


def test_two_humans_receive_same_committed_message(baseline_config):
    app = create_app(_service_config(baseline_config, with_agents=False))
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws_one, client.websocket_connect("/ws") as ws_two:
            _join_socket(ws_one, "u1", "Alice")
            _join_socket(ws_two, "u2", "Bob")

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
            _join_socket(websocket, "u1", "Alice")

            start = client.post("/start")
            assert start.status_code == 200
            _receive_until(websocket, "run_state_changed", key="state", value="running")

            websocket.send_json({"type": "send_message", "text": "thai sounds good", "client_message_id": "m-human-2"})
            human = _receive_message_until(
                websocket,
                lambda message: message["kind"] == "human" and message["client_message_id"] == "m-human-2",
                limit=200,
            )
            assert human["message"]["kind"] == "human"

            agent = _receive_message_until(websocket, lambda message: message["kind"] == "agent", limit=200)
            assert set(human["message"].keys()) == set(agent["message"].keys())
            assert human["message"]["participant_id"] == "u1"
            assert agent["message"]["participant_id"] == improved_config.agents[0].id


def test_control_endpoints_drive_room_state(baseline_config):
    app = create_app(_service_config(baseline_config, with_agents=False))
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as websocket:
            _join_socket(websocket, "u1", "Alice")

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
        assert 'data-app-shell="mafia-react"' in chat_page.text
        assert '/assets/react-app.js' in chat_page.text

        config_page = client.get("/config")
        assert config_page.status_code == 200
        assert 'data-app-shell="mafia-react"' in config_page.text

        app_js = client.get("/assets/react-app.js")
        assert app_js.status_code == 200
        assert "createRoot" in app_js.text
        assert "Mafia game scenario:" in app_js.text
        assert "Agents are spinning up" in app_js.text

        app_css = client.get("/assets/react-app.css")
        assert app_css.status_code == 200
        assert ".app-shell" in app_css.text

        draft_config = client.get("/api/config")
        assert draft_config.status_code == 200
        assert draft_config.json()["runtime"]["provider"] == "scripted"

        schema = client.get("/api/config/schema")
        assert schema.status_code == 200
        body = schema.json()
        assert "scripted" in body["runtime_providers"]
        assert ModeProfile.BASELINE_TIME_TO_TALK.value in body["modes"]


def test_room_creation_and_room_paths_render_shell(baseline_config):
    app = create_app(_service_config(baseline_config, with_agents=False))
    with TestClient(app) as client:
        created = client.post("/api/rooms", json={"room_id": "team-lunch"})
        assert created.status_code == 200
        body = created.json()
        assert body["room_id"] == "team-lunch"
        assert body["room_path"] == "/rooms/team-lunch"

        room_page = client.get("/rooms/team-lunch")
        assert room_page.status_code == 200
        assert 'data-app-shell="mafia-react"' in room_page.text

        config_page = client.get("/rooms/team-lunch/config")
        assert config_page.status_code == 200
        assert 'data-app-shell="mafia-react"' in config_page.text


def test_room_scoped_routes_and_sockets_do_not_create_missing_rooms(baseline_config):
    app = create_app(_service_config(baseline_config, with_agents=False))
    with TestClient(app) as client:
        room_page = client.get("/rooms/ghost-room")
        assert room_page.status_code == 404

        status = client.get("/api/rooms/ghost-room/status")
        assert status.status_code == 404

        with client.websocket_connect("/ws/ghost-room") as websocket:
            error = websocket.receive_json()
            assert error["type"] == "error"
            assert error["message"] == "room not found"
            with pytest.raises(WebSocketDisconnect):
                websocket.receive_json()


def test_room_template_and_room_creation_accept_custom_config(baseline_config):
    app = create_app(_service_config(baseline_config, with_agents=True))
    with TestClient(app) as client:
        template = client.get("/api/room-template")
        assert template.status_code == 200
        draft = template.json()
        assert draft["chat"]["scenario"] == baseline_config.chat.scenario

        draft["chat"]["scenario"] = "You are planning a team offsite dinner."
        draft["agents"][0]["display_name"] = "Morgan"
        draft["agents"][0]["goals"] = ["Keep the group aligned", "Surface tradeoffs"]
        draft["agents"][0]["personality"]["talkativeness"] = 0.9

        created = client.post("/api/rooms", json={"room_id": "offsite-dinner", "config": draft})
        assert created.status_code == 200
        body = created.json()
        assert body["room_id"] == "offsite-dinner"
        assert body["room_path"] == "/rooms/offsite-dinner"

        status = client.get("/api/rooms/offsite-dinner/status")
        assert status.status_code == 200
        room_status = status.json()
        assert room_status["draft_config"]["chat"]["scenario"] == "You are planning a team offsite dinner."
        assert room_status["draft_config"]["agents"][0]["display_name"] == "Morgan"
        assert room_status["draft_config"]["agents"][0]["goals"] == ["Keep the group aligned", "Surface tradeoffs"]
        assert room_status["draft_config"]["agents"][0]["personality"]["talkativeness"] == 0.9


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
            _join_socket(websocket, "u1", "Alice")

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
            assert observer.receive_json()["type"] == "room_snapshot"
            _join_socket(sender, "u1", "Alice")

            start = client.post("/start")
            assert start.status_code == 200
            _receive_until(observer, "run_state_changed", key="state", value="running")
            _receive_until(sender, "run_state_changed", key="state", value="running")

            sender.send_json({"type": "send_message", "text": "thai sounds good", "client_message_id": "observer-live-1"})
            observed = _receive_message_until(
                observer,
                lambda message: message["kind"] == "human" and message["client_message_id"] == "observer-live-1",
                limit=80,
            )
            assert observed["message"]["text"] == "thai sounds good"
            assert observed["message"]["participant_id"] == "u1"


def test_messages_api_returns_committed_history(improved_config):
    app = create_app(_service_config(improved_config, mode=ModeProfile.IMPROVED_BUFFERED_ASYNC, with_agents=True))
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as websocket:
            _join_socket(websocket, "u1", "Alice")

            assert client.post("/start").status_code == 200
            _receive_until(websocket, "run_state_changed", key="state", value="running")

            websocket.send_json({"type": "send_message", "text": "pizza maybe?", "client_message_id": "history-1"})
            _receive_until(websocket, "message_committed")

            history = client.get("/api/messages")
            assert history.status_code == 200
            body = history.json()
            assert any(message["client_message_id"] == "history-1" for message in body)


def test_mafia_room_creation_autostarts_into_lobby_and_generates_personas(improved_config):
    config = _mafia_service_config(improved_config)
    config.agents = []
    app = create_app(config)
    with TestClient(app) as client:
        created = client.post("/api/rooms", json={"room_id": "mafia-table"})
        assert created.status_code == 200
        body = created.json()
        assert body["room_id"] == "mafia-table"
        assert body["room_mode"] == "mafia"
        assert body["run_state"] == "running"

        status = client.get("/api/rooms/mafia-table/status")
        assert status.status_code == 200
        room_status = status.json()
        assert room_status["run_state"] == "running"
        assert room_status["mafia_state"]["game_status"] == "lobby"
        assert room_status["mafia_state"]["phase"] == "lobby"
        assert len(room_status["draft_config"]["agents"]) == room_status["draft_config"]["mafia"]["total_players"]
        assert room_status["draft_config"]["room_mode"] == "mafia"


def test_mafia_lobby_requires_names_and_starts_only_after_manual_start(improved_config):
    app = create_app(_mafia_service_config(improved_config))
    with TestClient(app) as client:
        client.post("/api/rooms", json={"room_id": "ready-gate"})

        with client.websocket_connect("/ws/ready-gate") as ws_one, client.websocket_connect("/ws/ready-gate") as ws_two:
            assert ws_one.receive_json()["type"] == "room_snapshot"
            assert ws_two.receive_json()["type"] == "room_snapshot"

            ws_one.send_json({"type": "join", "participant_id": "u1", "display_name": ""})
            error = _receive_until(ws_one, "error")
            assert "name required" in error["message"]

            ws_one.send_json({"type": "join", "participant_id": "u1", "display_name": "Alice"})
            joined_one = _receive_until(ws_one, "join")
            assert joined_one["participant"]["participant_id"] == "u1"

            still_lobby = client.get("/api/rooms/ready-gate/status").json()
            assert still_lobby["mafia_state"]["phase"] == "lobby"

            ws_two.send_json({"type": "join", "participant_id": "u2", "display_name": "Bob"})
            joined_two = _receive_until(ws_two, "join")
            assert joined_two["participant"]["participant_id"] == "u2"

            still_lobby = client.get("/api/rooms/ready-gate/status").json()
            assert still_lobby["mafia_state"]["phase"] == "lobby"

            start = client.post("/api/rooms/ready-gate/start")
            assert start.status_code == 200
            started = _receive_payload_until(
                ws_one,
                lambda payload: payload.get("type") == "mafia_state_changed" and payload.get("state", {}).get("phase") == "day_discussion",
                limit=80,
            )
            assert started["state"]["game_status"] == "active"

            status = client.get("/api/rooms/ready-gate/status").json()
            assert status["mafia_state"]["phase"] == "day_discussion"
            assert len(status["mafia_state"]["roster"]) == status["draft_config"]["mafia"]["total_players"]


def test_mafia_lobby_reports_agent_spinup_ready_live(improved_config):
    app = create_app(_mafia_service_config(improved_config))
    with TestClient(app) as client:
        created = client.post("/api/rooms", json={"room_id": "spinup-live"})
        assert created.status_code == 200

        with client.websocket_connect("/ws/spinup-live") as websocket:
            snapshot = websocket.receive_json()
            assert snapshot["type"] == "room_snapshot"
            assert snapshot["status"]["run_state"] == "running"
            assert snapshot["status"]["mafia_state"]["phase"] == "lobby"

            presence = _receive_payload_until(
                websocket,
                lambda payload: payload.get("type") == "presence_changed"
                and (payload.get("mafia_lobby_spinup") or {}).get("ready") is True,
                limit=120,
            )
            spinup = presence["mafia_lobby_spinup"]
            assert spinup["active"] is True
            assert spinup["ready_count"] == spinup["total_agents"]
            assert spinup["failed_count"] == 0
            assert all(agent["status"] == "ready" for agent in spinup["agents"])


def test_mafia_game_start_waits_for_agent_spinup(monkeypatch, improved_config):
    original = ChatRoomService._mafia_lobby_spinup_status

    def fake_spinup_status(self, config=None, *, run_state=None):
        spinup = original(self, config, run_state=run_state)
        if spinup and spinup["active"]:
            return {
                **spinup,
                "ready": False,
                "ready_count": 0,
                "failed_count": 0,
                "pending_count": spinup["total_agents"],
                "agents": [
                    {
                        **agent,
                        "status": "spinning_up",
                    }
                    for agent in spinup["agents"]
                ],
            }
        return spinup

    monkeypatch.setattr(ChatRoomService, "_mafia_lobby_spinup_status", fake_spinup_status)
    app = create_app(_mafia_service_config(improved_config))
    with TestClient(app) as client:
        created = client.post("/api/rooms", json={"room_id": "spinup-gate"})
        assert created.status_code == 200

        second = client.post("/api/rooms/spinup-gate/start")
        assert second.status_code == 409
        assert "agents are still spinning up" in second.json()["detail"]


def test_mafia_game_can_start_with_all_models_and_no_joined_humans(improved_config):
    app = create_app(_mafia_service_config(improved_config))
    with TestClient(app) as client:
        client.post("/api/rooms", json={"room_id": "model-only"})

        start = client.post("/api/rooms/model-only/start")
        assert start.status_code == 200

        status = client.get("/api/rooms/model-only/status")
        assert status.status_code == 200
        room_status = status.json()
        assert room_status["mafia_state"]["game_status"] == "active"
        assert room_status["mafia_state"]["phase"] == "day_discussion"
        assert len(room_status["mafia_state"]["roster"]) == room_status["draft_config"]["mafia"]["total_players"]
        assert all(entry["is_human"] is False for entry in room_status["mafia_state"]["roster"])


def test_mafia_late_joiners_become_spectators(improved_config):
    app = create_app(_mafia_service_config(improved_config))
    with TestClient(app) as client:
        client.post("/api/rooms", json={"room_id": "late-join"})

        with client.websocket_connect("/ws/late-join") as ws_one:
            assert ws_one.receive_json()["type"] == "room_snapshot"
            ws_one.send_json({"type": "join", "participant_id": "u1", "display_name": "Alice"})
            _receive_until(ws_one, "join")
            started = client.post("/api/rooms/late-join/start")
            assert started.status_code == 200
            _receive_payload_until(
                ws_one,
                lambda payload: payload.get("type") == "mafia_state_changed" and payload.get("state", {}).get("phase") == "day_discussion",
                limit=80,
            )

        with client.websocket_connect("/ws/late-join") as ws_late:
            assert ws_late.receive_json()["type"] == "room_snapshot"
            ws_late.send_json({"type": "join", "participant_id": "u2", "display_name": "Bob"})
            joined = _receive_until(ws_late, "join")
            assert joined["participant"]["kind"] == "spectator"
            player_state = _receive_until(ws_late, "player_state")
            assert player_state["state"]["spectator"] is True
            assert player_state["state"]["can_chat"] is False


def test_mafia_day_vote_keeps_private_state_private_until_reveal(improved_config):
    config = _mafia_service_config(improved_config)
    config.mafia.day_discussion_seconds = 0.15
    config.mafia.day_vote_seconds = 0.8
    app = create_app(config)
    with TestClient(app) as client:
        client.post("/api/rooms", json={"room_id": "vote-room"})

        with client.websocket_connect("/ws/vote-room") as websocket:
            assert websocket.receive_json()["type"] == "room_snapshot"
            websocket.send_json({"type": "join", "participant_id": "u1", "display_name": "Alice"})
            _receive_until(websocket, "join")
            start = client.post("/api/rooms/vote-room/start")
            assert start.status_code == 200
            _receive_payload_until(
                websocket,
                lambda payload: payload.get("type") == "mafia_state_changed" and payload.get("state", {}).get("phase") == "day_discussion",
                limit=80,
            )
            _receive_payload_until(
                websocket,
                lambda payload: payload.get("type") == "mafia_state_changed" and payload.get("state", {}).get("phase") == "day_vote",
                limit=120,
            )
            private_vote_state = _receive_payload_until(
                websocket,
                lambda payload: payload.get("type") == "player_state" and payload.get("state", {}).get("can_vote") is True,
                limit=120,
            )["state"]
            assert private_vote_state["legal_targets"]

            status_before_vote = client.get("/api/rooms/vote-room/status").json()
            assert status_before_vote["mafia_state"]["phase"] == "day_vote"
            assert "day_votes" not in status_before_vote["mafia_state"]

            target = private_vote_state["legal_targets"][0]
            websocket.send_json({"type": "cast_vote", "target_participant_id": target})
            updated_private_state = _receive_payload_until(
                websocket,
                lambda payload: payload.get("type") == "player_state" and payload.get("state", {}).get("selected_target_participant_id") == target,
                limit=120,
            )["state"]
            assert updated_private_state["selected_target_participant_id"] == target

            status_after_vote = client.get("/api/rooms/vote-room/status").json()
            assert "day_votes" not in status_after_vote["mafia_state"]


def test_room_scoped_presence_and_message_history_are_isolated(baseline_config):
    app = create_app(_service_config(baseline_config, with_agents=False))
    with TestClient(app) as client:
        created = client.post("/api/rooms", json={"room_id": "team-a"})
        assert created.status_code == 200
        with client.websocket_connect("/ws/team-a") as observer, client.websocket_connect("/ws/team-a") as sender:
            snapshot = observer.receive_json()
            assert snapshot["type"] == "room_snapshot"

            _join_socket(sender, "u1", "Alice")
            presence = _receive_until(observer, "presence_changed", key="participant_count", value=1)
            assert presence["participant_count"] == 1

            start = client.post("/api/rooms/team-a/start")
            assert start.status_code == 200
            _receive_until(observer, "run_state_changed", key="state", value="running")
            _receive_until(sender, "run_state_changed", key="state", value="running")

            sender.send_json({"type": "send_message", "text": "hello from team a", "client_message_id": "room-a-1"})
            observed = _receive_until(observer, "message_committed")
            assert observed["message"]["text"] == "hello from team a"

        history_a = client.get("/api/rooms/team-a/messages")
        history_b = client.get("/api/rooms/team-b/messages")
        assert history_a.status_code == 200
        assert history_b.status_code == 404
        assert any(message["client_message_id"] == "room-a-1" for message in history_a.json())
