from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


class SourceApiError(RuntimeError):
    def __init__(self, source: str, message: str, *, payload: Any | None = None) -> None:
        super().__init__(f"{source}: {message}")
        self.source = source
        self.payload = payload


@dataclass(frozen=True)
class HttpJsonClient:
    source: str
    base_url: str
    default_headers: dict[str, str] | None = None
    timeout: float = 30.0

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
    )
    def get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        with httpx.Client(base_url=self.base_url, timeout=self.timeout) as client:
            response = client.get(path, params=params, headers=self._headers(headers))
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise SourceApiError(self.source, "expected JSON object", payload=data)
            return data

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
    )
    def get_bytes(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> bytes:
        with httpx.Client(base_url=self.base_url, timeout=self.timeout) as client:
            response = client.get(path, params=params, headers=self._headers(headers))
            response.raise_for_status()
            return response.content

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
    )
    def post_json(
        self,
        path: str,
        *,
        json: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        with httpx.Client(base_url=self.base_url, timeout=self.timeout) as client:
            response = client.post(path, json=json, headers=self._headers(headers))
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise SourceApiError(self.source, "expected JSON object", payload=data)
            return data

    def _headers(self, headers: dict[str, str] | None) -> dict[str, str]:
        merged = dict(self.default_headers or {})
        merged.update(headers or {})
        return merged
