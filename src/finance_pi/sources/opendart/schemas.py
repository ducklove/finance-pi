from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class DartCompanyRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    snapshot_dt: date
    corp_code: str = Field(pattern=r"^\d{8}$")
    corp_name: str
    stock_code: str | None = Field(default=None, pattern=r"^\d{6}$")
    modify_date: str | None = None


class DartFilingRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rcept_dt: date
    corp_code: str = Field(pattern=r"^\d{8}$")
    corp_name: str
    stock_code: str | None = Field(default=None, pattern=r"^\d{6}$")
    rcept_no: str
    report_nm: str
    rm: str | None = None

    @property
    def is_correction(self) -> bool:
        return self.rm is not None and "정" in self.rm


class DartFinancialRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    corp_code: str = Field(pattern=r"^\d{8}$")
    security_id: str | None = None
    fiscal_period_end: date
    event_date: date
    rcept_dt: date
    available_date: date
    report_type: str
    account_id: str
    account_name: str
    amount: float
    is_consolidated: bool = True
    accounting_basis: str | None = None
