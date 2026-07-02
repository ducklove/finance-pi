from __future__ import annotations

import httpx
import pytest

import finance_pi.http as http_module
from finance_pi.http import HttpJsonClient, RetryableApiError, SourceApiError


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


def _mock_client_factory(status_codes: list[int]) -> type[httpx.Client]:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        index = min(len(calls), len(status_codes) - 1)
        calls.append(index)
        status_code = status_codes[index]
        return httpx.Response(status_code, json={"status": "ok"}, request=request)

    class _MockedClient(httpx.Client):
        def __init__(self, *args: object, **kwargs: object) -> None:
            kwargs.pop("transport", None)
            super().__init__(*args, transport=httpx.MockTransport(handler), **kwargs)

    _MockedClient.calls = calls  # type: ignore[attr-defined]
    return _MockedClient


def _disable_sleep(monkeypatch: pytest.MonkeyPatch, *bound_methods: object) -> None:
    for method in bound_methods:
        monkeypatch.setattr(method.retry, "sleep", lambda _seconds: None)


def test_get_json_retries_on_500_then_succeeds(monkeypatch) -> None:
    mocked_client = _mock_client_factory([500, 500, 200])
    monkeypatch.setattr(http_module.httpx, "Client", mocked_client)
    client = HttpJsonClient("test", "https://example.test")
    _disable_sleep(monkeypatch, client.get_json)

    assert client.get_json("/path") == {"status": "ok"}
    assert len(mocked_client.calls) == 3


def test_get_json_retries_on_429(monkeypatch) -> None:
    mocked_client = _mock_client_factory([429, 200])
    monkeypatch.setattr(http_module.httpx, "Client", mocked_client)
    client = HttpJsonClient("test", "https://example.test")
    _disable_sleep(monkeypatch, client.get_json)

    assert client.get_json("/path") == {"status": "ok"}
    assert len(mocked_client.calls) == 2


def test_get_json_exhausts_retries_and_raises_retryable_error(monkeypatch) -> None:
    mocked_client = _mock_client_factory([500, 500, 500])
    monkeypatch.setattr(http_module.httpx, "Client", mocked_client)
    client = HttpJsonClient("test", "https://example.test")
    _disable_sleep(monkeypatch, client.get_json)

    with pytest.raises(RetryableApiError):
        client.get_json("/path")
    assert len(mocked_client.calls) == 3


def test_get_json_does_not_retry_on_404(monkeypatch) -> None:
    mocked_client = _mock_client_factory([404])
    monkeypatch.setattr(http_module.httpx, "Client", mocked_client)
    client = HttpJsonClient("test", "https://example.test")
    _disable_sleep(monkeypatch, client.get_json)

    with pytest.raises(SourceApiError) as exc_info:
        client.get_json("/path")
    assert not isinstance(exc_info.value, RetryableApiError)
    assert len(mocked_client.calls) == 1


def test_post_json_no_retry_does_not_retry_on_500(monkeypatch) -> None:
    mocked_client = _mock_client_factory([500, 200])
    monkeypatch.setattr(http_module.httpx, "Client", mocked_client)
    client = HttpJsonClient("test", "https://example.test")

    with pytest.raises(RetryableApiError):
        client.post_json_no_retry("/oauth2/tokenP", json={})
    assert len(mocked_client.calls) == 1

