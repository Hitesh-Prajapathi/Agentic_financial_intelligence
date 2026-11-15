"""Pydantic models for a parsed Form 4 / 4/A ownership document."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field


class ReportingOwner(BaseModel):
    cik: str  # unpadded (leading zeros stripped), matches daily-index and company_tickers.json
    name: str | None = None
    is_director: bool = False
    is_officer: bool = False
    is_ten_pct: bool = False
    is_other: bool = False
    officer_title: str | None = None
    other_text: str | None = None


class NonDerivTxn(BaseModel):
    row_idx: int  # 0-based position among nonDerivativeTransaction elements in the document
    security_title: str | None = None
    transaction_date: date | None = None
    form_type_in_txn: str | None = None
    code: str | None = None  # transactionCoding/transactionCode; ALL codes kept (P, S, A, M, F, G, ...)
    equity_swap: bool | None = None
    shares: float | None = None
    price_per_share: float | None = None
    shares_footnote_ids: list[str] = Field(default_factory=list)
    price_footnote_ids: list[str] = Field(default_factory=list)
    acquired_disposed: str | None = None  # 'A' / 'D'
    post_transaction_shares: float | None = None
    direct_indirect: str | None = None  # 'D' / 'I'
    nature_of_ownership: str | None = None


class Filing(BaseModel):
    accession_number: str  # with dashes, e.g. 0001234567-26-000123
    form_type: str  # '4' or '4/A'
    schema_version: str | None = None
    accepted_at: datetime | None = None  # EDGAR acceptance time, naive UTC
    period_of_report: date | None = None
    date_of_original_submission: date | None = None
    issuer_cik: str  # unpadded
    issuer_name: str | None = None
    issuer_ticker: str | None = None  # stripped + upper; None if blank / 'NONE' / 'N/A'
    owners: list[ReportingOwner] = Field(default_factory=list)  # ALL reporting owners (joint filings)
    non_derivative: list[NonDerivTxn] = Field(default_factory=list)
    footnotes: dict[str, str] = Field(default_factory=dict)  # id -> text
    remarks: str | None = None
    aff_10b5_1: bool | None = None  # <aff10b5One> checkbox (Form 4 since April 2023); None if absent
    is_10b5_1: bool = False  # checkbox if present, else footnote/remarks regex fallback
    price_in_footnote: bool = False  # any P/S row with no price but a price footnote
