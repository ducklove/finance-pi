from __future__ import annotations

import finance_pi.http as http_module
from finance_pi.http import HttpJsonClient


class FakeResponse:
    content = b"ok"
    text = "ok"

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, str]:
        return {"status": "ok"}


class FakeClient:
    instances: list[FakeClient] = []

    def __init__(self, *args: object, **kwargs: object) -> None:
        self.requests: list[tuple[str, str]] = []
        self.is_closed = False
        FakeClient.instances.append(self)

    def request(self, method: str, path: str, **_kwargs: object) -> FakeResponse:
        self.requests.append((method, path))
        return FakeResponse()

    def close(self) -> None:
        self.is_closed = True


def test_http_json_client_reuses_shared_client(monkeypatch) -> None:
    FakeClient.instances = []
    monkeypatch.setattr(http_module.httpx, "Client", FakeClient)
    client = HttpJsonClient("test", "https://example.test")

    assert client.get_json("/one") == {"status": "ok"}
    assert client.get_text("/two") == "ok"

    assert len(FakeClient.instances) == 1
    assert FakeClient.instances[0].requests == [("GET", "/one"), ("GET", "/two")]

    client.close()
    assert FakeClient.instances[0].is_closed

    assert client.get_bytes("/three") == b"ok"
    assert len(FakeClient.instances) == 2

