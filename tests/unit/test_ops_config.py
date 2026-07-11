from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_apache_proxies_admin_file_downloads() -> None:
    config = (REPO_ROOT / "ops/apache/finance-pi-admin.conf").read_text(encoding="utf-8")

    assert "ProxyPass /files " in config
    assert "ProxyPassReverse /files " in config


def test_systemd_services_enforce_runtime_safety_limits() -> None:
    admin = (REPO_ROOT / "ops/systemd/finance-pi-admin.service").read_text(encoding="utf-8")
    daily = (REPO_ROOT / "ops/systemd/finance-pi-daily.service").read_text(encoding="utf-8")

    assert "UMask=0077" in admin
    assert "UMask=0077" in daily
    assert "--no-strict" not in daily
    assert "TimeoutStartSec=90min" in daily
    assert "MemoryHigh=5G" in daily
    assert "MemoryMax=6G" in daily


def test_deploy_restricts_environment_file_permissions() -> None:
    deploy = (REPO_ROOT / "ops/deploy.sh").read_text(encoding="utf-8")

    assert "chmod 600 .env" in deploy
    assert 'install -m 0644 "$unit_file"' in deploy
    assert "systemctl --user daemon-reload" in deploy
    assert "/etc/apache2/conf-available/finance-pi-admin.conf" in deploy


def test_windows_deploy_streams_without_powershell_crlf_pipeline() -> None:
    deploy = (REPO_ROOT / "ops/deploy-from-windows.ps1").read_text(encoding="utf-8")

    assert "RedirectStandardInput = $true" in deploy
    assert "$process.StandardInput.Write($script)" in deploy
    assert "$script | ssh" not in deploy
