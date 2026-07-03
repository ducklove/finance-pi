from __future__ import annotations

from datetime import date

import pytest

from finance_pi.http import SourceApiError
from finance_pi.sources.kis.client import KisDailyPriceClient


class FakeHttpClient:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self._responses = list(responses)
        self.calls = 0

    def get_json(self, *_args: object, **_kwargs: object) -> dict[str, object]:
        self.calls += 1
        index = min(self.calls - 1, len(self._responses) - 1)
        return self._responses[index]


def _success_payload() -> dict[str, object]:
    return {"rt_cd": "0", "output1": {}, "output2": []}


def _rate_limit_payload() -> dict[str, object]:
    return {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "rate limited"}


def test_fetch_daily_prices_retries_on_rate_limit_then_succeeds(monkeypatch) -> None:
    fake_http = FakeHttpClient([_rate_limit_payload(), _rate_limit_payload(), _success_payload()])
    monkeypatch.setattr("finance_pi.sources.kis.client.sleep", lambda _seconds: None)
    client = KisDailyPriceClient(fake_http, "key", "secret", "token")

    rows = client.fetch_daily_prices("005930", date(2026, 4, 1), date(2026, 4, 1))

    assert rows == []
    assert fake_http.calls == 3


def test_fetch_daily_prices_gives_up_after_two_rate_limit_retries(monkeypatch) -> None:
    fake_http = FakeHttpClient(
        [_rate_limit_payload(), _rate_limit_payload(), _rate_limit_payload()]
    )
    monkeypatch.setattr("finance_pi.sources.kis.client.sleep", lambda _seconds: None)
    client = KisDailyPriceClient(fake_http, "key", "secret", "token")

    with pytest.raises(SourceApiError):
        client.fetch_daily_prices("005930", date(2026, 4, 1), date(2026, 4, 1))

    assert fake_http.calls == 3


def test_fetch_daily_prices_retry_knobs_extend_attempts_and_backoff(monkeypatch) -> None:
    fake_http = FakeHttpClient([*[_rate_limit_payload()] * 4, _success_payload()])
    sleeps: list[float] = []
    monkeypatch.setattr(
        "finance_pi.sources.kis.client.sleep", lambda seconds: sleeps.append(seconds)
    )
    client = KisDailyPriceClient(
        fake_http,
        "key",
        "secret",
        "token",
        retry_attempts=5,
        retry_sleep_seconds=0.5,
        retry_backoff_multiplier=3.0,
    )

    rows = client.fetch_daily_prices("005930", date(2026, 4, 1), date(2026, 4, 1))

    assert rows == []
    assert fake_http.calls == 5
    assert sleeps == [0.5, 1.5, 4.5, 13.5]


def test_fetch_daily_prices_single_attempt_disables_rate_limit_retry(monkeypatch) -> None:
    fake_http = FakeHttpClient([_rate_limit_payload(), _success_payload()])
    sleeps: list[float] = []
    monkeypatch.setattr(
        "finance_pi.sources.kis.client.sleep", lambda seconds: sleeps.append(seconds)
    )
    client = KisDailyPriceClient(fake_http, "key", "secret", "token", retry_attempts=1)

    with pytest.raises(SourceApiError):
        client.fetch_daily_prices("005930", date(2026, 4, 1), date(2026, 4, 1))

    assert fake_http.calls == 1
    assert sleeps == []


def test_fetch_daily_prices_does_not_retry_non_rate_limit_error(monkeypatch) -> None:
    fake_http = FakeHttpClient([{"rt_cd": "1", "msg_cd": "OTHER", "msg1": "bad request"}])
    monkeypatch.setattr("finance_pi.sources.kis.client.sleep", lambda _seconds: None)
    client = KisDailyPriceClient(fake_http, "key", "secret", "token")

    with pytest.raises(SourceApiError):
        client.fetch_daily_prices("005930", date(2026, 4, 1), date(2026, 4, 1))

    assert fake_http.calls == 1
