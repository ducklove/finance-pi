from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    """Filesystem locations used by a local finance-pi workspace."""

    root: Path = Path(".")
    data_dir: Path | None = None
    report_dir: Path | None = None

    @property
    def data_root(self) -> Path:
        return self.data_dir or self.root / "data"

    @property
    def reports_root(self) -> Path:
        return self.report_dir or self.data_root / "reports"

    @property
    def catalog_path(self) -> Path:
        return self.data_root / "catalog" / "finance_pi.duckdb"

    @classmethod
    def from_cwd(cls) -> ProjectPaths:
        return cls(root=Path.cwd())


@dataclass(frozen=True)
class RuntimeSettings:
    krx_openapi_key: str | None = None
    krx_base_url: str = "https://data-dbg.krx.co.kr"
    krx_stock_daily_path: str = "/svc/apis/sto/stk_bydd_trd"
    opendart_api_key: str | None = None
    opendart_base_url: str = "https://opendart.fss.or.kr"
    kis_base_url: str = "https://openapi.koreainvestment.com:9443"
    kis_app_key: str | None = None
    kis_app_secret: str | None = None
    kis_access_token: str | None = None

    @classmethod
    def load(cls, root: Path | None = None) -> RuntimeSettings:
        if root is not None:
            load_dotenv(root / ".env")
        return cls(
            krx_openapi_key=_env_first("KRX_OPENAPI_KEY", "KRX_AUTH_KEY"),
            krx_base_url=os.getenv("KRX_BASE_URL", cls.krx_base_url),
            krx_stock_daily_path=os.getenv("KRX_STOCK_DAILY_PATH", cls.krx_stock_daily_path),
            opendart_api_key=_env_first("OPENDART_API_KEY", "DART_API_KEY"),
            opendart_base_url=os.getenv("OPENDART_BASE_URL", cls.opendart_base_url),
            kis_base_url=os.getenv("KIS_BASE_URL", cls.kis_base_url),
            kis_app_key=os.getenv("KIS_APP_KEY"),
            kis_app_secret=os.getenv("KIS_APP_SECRET"),
            kis_access_token=os.getenv("KIS_ACCESS_TOKEN"),
        )

    @property
    def has_krx(self) -> bool:
        return bool(self.krx_openapi_key)

    @property
    def has_opendart(self) -> bool:
        return bool(self.opendart_api_key)

    @property
    def has_kis(self) -> bool:
        return bool(self.kis_access_token or (self.kis_app_key and self.kis_app_secret))


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", maxsplit=1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def parse_yyyymmdd(value: str) -> date:
    if len(value) == 8 and value.isdigit():
        return date(int(value[:4]), int(value[4:6]), int(value[6:8]))
    return date.fromisoformat(value)


def format_yyyymmdd(value: date) -> str:
    return value.strftime("%Y%m%d")


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None
