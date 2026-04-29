from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
KNOWN_DOTENV_KEYS = frozenset(
    {
        "KRX_OPENAPI_KEY",
        "KRX_AUTH_KEY",
        "KRX_BASE_URL",
        "KRX_STOCK_DAILY_PATH",
        "KRX_KOSPI_DAILY_PATH",
        "KRX_KOSDAQ_DAILY_PATH",
        "KRX_MARKETS",
        "OPENDART_API_KEY",
        "DART_API_KEY",
        "OPENDART_BASE_URL",
        "KIS_BASE_URL",
        "KIS_APP_KEY",
        "KIS_APP_SECRET",
        "KIS_ACCESS_TOKEN",
        "KIS_ACCOUNT_NO",
        "KIS_ACCOUNT_PRODUCT_CODE",
        "NAVER_FINANCE_BASE_URL",
        "NAVER_FINANCE_API_BASE_URL",
        "NAVER_FINANCE_USER_AGENT",
    }
)


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
class DotenvIssue:
    line_no: int
    message: str


@dataclass(frozen=True)
class RuntimeSettings:
    krx_openapi_key: str | None = None
    krx_base_url: str = "https://data-dbg.krx.co.kr"
    krx_stock_daily_path: str = "/svc/apis/sto/stk_bydd_trd"
    krx_kospi_daily_path: str = "/svc/apis/sto/stk_bydd_trd"
    krx_kosdaq_daily_path: str = "/svc/apis/sto/ksq_bydd_trd"
    krx_markets: tuple[str, ...] = ("KOSPI", "KOSDAQ")
    opendart_api_key: str | None = None
    opendart_base_url: str = "https://opendart.fss.or.kr"
    kis_base_url: str = "https://openapi.koreainvestment.com:9443"
    kis_app_key: str | None = None
    kis_app_secret: str | None = None
    kis_access_token: str | None = None
    naver_finance_base_url: str = "https://finance.naver.com"
    naver_finance_api_base_url: str = "https://api.finance.naver.com"
    naver_finance_user_agent: str = "Mozilla/5.0 finance-pi/0.1"

    @classmethod
    def load(cls, root: Path | None = None) -> RuntimeSettings:
        if root is not None:
            load_dotenv(root / ".env")
        krx_markets_value = _env_first("KRX_MARKETS") or ",".join(cls.krx_markets)
        return cls(
            krx_openapi_key=_env_first("KRX_OPENAPI_KEY", "KRX_AUTH_KEY"),
            krx_base_url=_env_first("KRX_BASE_URL") or cls.krx_base_url,
            krx_stock_daily_path=_env_first("KRX_STOCK_DAILY_PATH")
            or cls.krx_stock_daily_path,
            krx_kospi_daily_path=_env_first("KRX_KOSPI_DAILY_PATH")
            or cls.krx_kospi_daily_path,
            krx_kosdaq_daily_path=_env_first("KRX_KOSDAQ_DAILY_PATH")
            or cls.krx_kosdaq_daily_path,
            krx_markets=tuple(
                market.strip().upper()
                for market in krx_markets_value.split(",")
                if market.strip()
            ),
            opendart_api_key=_env_first("OPENDART_API_KEY", "DART_API_KEY"),
            opendart_base_url=_env_first("OPENDART_BASE_URL") or cls.opendart_base_url,
            kis_base_url=_env_first("KIS_BASE_URL") or cls.kis_base_url,
            kis_app_key=_env_first("KIS_APP_KEY"),
            kis_app_secret=_env_first("KIS_APP_SECRET"),
            kis_access_token=_env_first("KIS_ACCESS_TOKEN"),
            naver_finance_base_url=_env_first("NAVER_FINANCE_BASE_URL")
            or cls.naver_finance_base_url,
            naver_finance_api_base_url=_env_first("NAVER_FINANCE_API_BASE_URL")
            or cls.naver_finance_api_base_url,
            naver_finance_user_agent=_env_first("NAVER_FINANCE_USER_AGENT")
            or cls.naver_finance_user_agent,
        )

    @property
    def has_krx(self) -> bool:
        return bool(self.krx_openapi_key)

    @property
    def has_opendart(self) -> bool:
        return bool(self.opendart_api_key)

    @property
    def has_kis(self) -> bool:
        return bool(self.kis_app_key and self.kis_app_secret)


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", maxsplit=1)
        key = key.strip()
        if not _ENV_KEY_RE.fullmatch(key) or key not in KNOWN_DOTENV_KEYS:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def diagnose_dotenv(path: Path) -> list[DotenvIssue]:
    if not path.exists():
        return []
    issues: list[DotenvIssue] = []
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            issues.append(
                DotenvIssue(
                    line_no,
                    "ignored line without KEY=VALUE; keep API secrets on one line",
                )
            )
            continue
        key, value = line.split("=", maxsplit=1)
        key = key.strip()
        if not _ENV_KEY_RE.fullmatch(key):
            issues.append(
                DotenvIssue(
                    line_no,
                    "ignored invalid environment key; check for a wrapped or pasted secret",
                )
            )
            continue
        if key not in KNOWN_DOTENV_KEYS:
            issues.append(DotenvIssue(line_no, "unknown key ignored by finance-pi"))
        stripped = value.strip()
        if (
            len(stripped) == 1
            and stripped in {'"', "'"}
            or len(stripped) > 1
            and stripped[0] in {'"', "'"}
            and stripped[-1] != stripped[0]
        ):
            issues.append(
                DotenvIssue(
                    line_no,
                    "quoted value is not closed on the same line",
                )
            )
    return issues


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
            return value.strip()
    return None
