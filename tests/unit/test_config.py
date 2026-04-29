from __future__ import annotations

from finance_pi.config import KNOWN_DOTENV_KEYS, RuntimeSettings, diagnose_dotenv


def test_runtime_settings_treat_empty_base_urls_as_defaults(tmp_path, monkeypatch) -> None:
    for key in KNOWN_DOTENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "KIS_BASE_URL=",
                "KIS_APP_KEY= app-key ",
                "KIS_APP_SECRET= app-secret ",
                "OPENDART_BASE_URL=",
            ]
        ),
        encoding="utf-8",
    )

    settings = RuntimeSettings.load(tmp_path)

    assert settings.kis_base_url == RuntimeSettings.kis_base_url
    assert settings.opendart_base_url == RuntimeSettings.opendart_base_url
    assert settings.kis_app_key == "app-key"
    assert settings.kis_app_secret == "app-secret"
    assert settings.has_kis


def test_diagnose_dotenv_flags_wrapped_or_unknown_lines(tmp_path) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "KIS_APP_SECRET=first-half",
                "wrapped/secret=second-half",
                "NOT_USED_BY_APP=value",
                "dangling-secret-fragment",
            ]
        ),
        encoding="utf-8",
    )

    issues = diagnose_dotenv(tmp_path / ".env")

    assert [issue.line_no for issue in issues] == [2, 3, 4]
    assert "invalid environment key" in issues[0].message
    assert "unknown key" in issues[1].message
    assert "KEY=VALUE" in issues[2].message
