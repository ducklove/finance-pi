from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

import finance_pi.http as http_module
from finance_pi.http import RetryableApiError
from finance_pi.sources.kis import KisAuthClient, KisToken, KisTokenCache


def test_kis_token_cache_roundtrip(tmp_path) -> None:
    cache = KisTokenCache(tmp_path / "token.json")
    token = KisToken(
        access_token="access-token",
        token_type="Bearer",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )

    cache.write("app-key", token)

    cached = cache.read("app-key")
    assert cached is not None
    assert cached.access_token == "access-token"
    assert cached.token_type == "Bearer"


def test_kis_token_cache_rejects_wrong_key_or_expired_token(tmp_path) -> None:
    cache = KisTokenCache(tmp_path / "token.json")
    token = KisToken(
        access_token="access-token",
        token_type="Bearer",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    cache.write("app-key", token)

    assert cache.read("other-app-key") is None

    expired = KisToken(
        access_token="expired",
        token_type="Bearer",
        expires_at=datetime.now(UTC) - timedelta(minutes=1),
    )
    cache.write("app-key", expired)
    assert cache.read("app-key") is None


def test_issue_token_does_not_retry_on_500(monkeypatch) -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(500, json={"error": "boom"}, request=request)

    class _MockedClient(httpx.Client):
        def __init__(self, *args: object, **kwargs: object) -> None:
            kwargs.pop("transport", None)
            super().__init__(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(http_module.httpx, "Client", _MockedClient)
    auth = KisAuthClient(base_url="https://example.test", app_key="key", app_secret="secret")

    with pytest.raises(RetryableApiError):
        auth.issue_token()

    assert len(calls) == 1
