from __future__ import annotations

from datetime import UTC, datetime, timedelta

from finance_pi.sources.kis import KisToken, KisTokenCache


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
