from __future__ import annotations

import hashlib
import json
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from finance_pi.http import HttpJsonClient


@dataclass(frozen=True)
class KisToken:
    access_token: str
    token_type: str
    expires_at: datetime

    def is_valid(self, *, skew: timedelta = timedelta(minutes=5)) -> bool:
        return datetime.now(UTC) + skew < self.expires_at


@dataclass(frozen=True)
class KisTokenCache:
    path: Path

    def read(self, app_key: str) -> KisToken | None:
        if not self.path.exists():
            return None
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return None
            if data.get("app_key_hash") != _app_key_hash(app_key):
                return None
            token = _token_from_mapping(data)
        except (OSError, ValueError, TypeError, KeyError):
            return None
        return token if token.is_valid() else None

    def write(self, app_key: str, token: KisToken) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "app_key_hash": _app_key_hash(app_key),
            "access_token": token.access_token,
            "token_type": token.token_type,
            "expires_at": token.expires_at.astimezone(UTC).isoformat(),
        }
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        with suppress(OSError):
            self.path.chmod(0o600)


@dataclass(frozen=True)
class KisAuthClient:
    base_url: str
    app_key: str
    app_secret: str

    def issue_token(self) -> KisToken:
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }
        data = HttpJsonClient("kis", self.base_url).post_json("/oauth2/tokenP", json=payload)
        expires_in = int(data.get("expires_in", 0))
        return KisToken(
            access_token=data["access_token"],
            token_type=data.get("token_type", "Bearer"),
            expires_at=datetime.now(UTC) + timedelta(seconds=expires_in),
        )


def _token_from_mapping(data: dict[str, Any]) -> KisToken:
    expires_at = datetime.fromisoformat(str(data["expires_at"]))
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=UTC)
    return KisToken(
        access_token=str(data["access_token"]),
        token_type=str(data.get("token_type", "Bearer")),
        expires_at=expires_at,
    )


def _app_key_hash(app_key: str) -> str:
    return hashlib.sha256(app_key.encode("utf-8")).hexdigest()
