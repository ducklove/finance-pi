from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx


@dataclass(frozen=True)
class KisToken:
    access_token: str
    token_type: str
    expires_at: datetime

    def is_valid(self, *, skew: timedelta = timedelta(minutes=5)) -> bool:
        return datetime.now(UTC) + skew < self.expires_at


@dataclass(frozen=True)
class KisAuthClient:
    base_url: str
    app_key: str
    app_secret: str

    def issue_token(self) -> KisToken:
        url = f"{self.base_url.rstrip('/')}/oauth2/tokenP"
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }
        response = httpx.post(url, json=payload, timeout=20)
        response.raise_for_status()
        data = response.json()
        expires_in = int(data.get("expires_in", 0))
        return KisToken(
            access_token=data["access_token"],
            token_type=data.get("token_type", "Bearer"),
            expires_at=datetime.now(UTC) + timedelta(seconds=expires_in),
        )
