import qb_client


class _FakeSession:
    def __init__(self, exc=None):
        self.exc = exc
        self.calls = []

    def post(self, *args, **kwargs):
        self.calls.append(("post", args, kwargs))
        if self.exc:
            raise self.exc
        raise AssertionError("Unexpected post call without configured response")

    def get(self, *args, **kwargs):
        self.calls.append(("get", args, kwargs))
        if self.exc:
            raise self.exc
        raise AssertionError("Unexpected get call without configured response")


def test_qb_client_unreachable_login_sets_backoff(monkeypatch):
    monkeypatch.setattr(qb_client.config, "has_qbittorrent", lambda: True)
    monkeypatch.setattr(qb_client.config, "QB_URL", "http://qb:8080", raising=False)
    monkeypatch.setattr(qb_client.config, "QB_USER", "jam", raising=False)
    monkeypatch.setattr(qb_client.config, "QB_PASS", "1301", raising=False)

    client = qb_client.QBittorrentClient()
    client.session = _FakeSession(exc=qb_client.requests.ConnectionError("down"))

    assert client.login() is False
    assert client.last_error is not None
    assert client.last_error["kind"] == "unreachable"
    assert client.last_error.get("retry_in_sec", 0) >= 1

    # Subsequent login during backoff should short-circuit without another HTTP call.
    call_count = len(client.session.calls)
    assert client.login() is False
    assert len(client.session.calls) == call_count
    assert client.last_error["kind"] == "cooldown"


def test_qb_client_add_torrent_short_circuits_during_backoff(monkeypatch):
    monkeypatch.setattr(qb_client.config, "has_qbittorrent", lambda: True)
    monkeypatch.setattr(qb_client.config, "QB_URL", "http://qb:8080", raising=False)
    monkeypatch.setattr(qb_client.config, "QB_USER", "jam", raising=False)
    monkeypatch.setattr(qb_client.config, "QB_PASS", "1301", raising=False)

    client = qb_client.QBittorrentClient()
    client.session = _FakeSession(exc=qb_client.requests.ConnectionError("down"))
    client._next_login_after = qb_client.time.time() + 10

    assert client.add_torrent("magnet:?xt=urn:btih:test", title="Test") is False
    assert client.session.calls == []
