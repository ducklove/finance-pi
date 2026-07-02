from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


class SourceApiError(RuntimeError):
    def __init__(self, source: str, message: str, *, payload: Any | None = None) -> None:
        super().__init__(f"{source}: {message}")
        self.source = source
        self.payload = payload


class RetryableApiError(SourceApiError):
    """HTTP 429/5xx: transient, safe to retry with backoff."""


@dataclass(frozen=True)
class HttpJsonClient:
    source: str
    base_url: str
    default_headers: dict[str, str] | None = None
    timeout: float = 30.0
    _client: httpx.Client | None = field(default=None, init=False, repr=False, compare=False)
    _client_lock: threading.Lock = field(
        default_factory=threading.Lock,
        init=False,
        repr=False,
        compare=False,
    )

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.TransportError, RetryableApiError)
        ),
    )
    def get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        response = self._request("GET", path, params=params, headers=headers)
        data = response.json()
        if not isinstance(data, dict):
            raise SourceApiError(self.source, "expected JSON object", payload=data)
        return data

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.TransportError, RetryableApiError)
        ),
    )
    def get_bytes(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> bytes:
        return self._request("GET", path, params=params, headers=headers).content

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.TransportError, RetryableApiError)
        ),
    )
    def get_text(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        return self._request("GET", path, params=params, headers=headers).text

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.TransportError, RetryableApiError)
        ),
    )
    def post_json(
        self,
        path: str,
        *,
        json: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        response = self._request("POST", path, json=json, headers=headers)
        data = response.json()
        if not isinstance(data, dict):
            raise SourceApiError(self.source, "expected JSON object", payload=data)
        return data

    def post_json_no_retry(
        self,
        path: str,
        *,
        json: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Like post_json but never retries. Use for endpoints where retrying on
        429/5xx is unsafe (e.g. KIS token issuance, which allows one call/minute)."""
        response = self._request("POST", path, json=json, headers=headers)
        data = response.json()
        if not isinstance(data, dict):
            raise SourceApiError(self.source, "expected JSON object", payload=data)
        return data

    def close(self) -> None:
        with self._client_lock:
            client = self._client
            object.__setattr__(self, "_client", None)
        if client is not None:
            client.close()

    def __enter__(self) -> HttpJsonClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        client = self._shared_client()
        response = client.request(
            method,
            path,
            params=params,
            json=json,
            headers=self._headers(headers),
        )
        _raise_for_status(self.source, response)
        return response

    def _shared_client(self) -> httpx.Client:
        client = self._client
        if client is not None and not client.is_closed:
            return client
        with self._client_lock:
            client = self._client
            if client is None or client.is_closed:
                client = httpx.Client(base_url=self.base_url, timeout=self.timeout)
                object.__setattr__(self, "_client", client)
            return client

    def _headers(self, headers: dict[str, str] | None) -> dict[str, str]:
        merged = dict(self.default_headers or {})
        merged.update(headers or {})
        return merged


def _raise_for_status(source: str, response: httpx.Response) -> None:
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        body = response.text[:500]
        message = (
            f"HTTP {response.status_code} {response.reason_phrase} "
            f"for {response.url}; body={body}"
        )
        if response.status_code == 429 or response.status_code >= 500:
            raise RetryableApiError(source, message) from exc
        raise SourceApiError(source, message) from exc
