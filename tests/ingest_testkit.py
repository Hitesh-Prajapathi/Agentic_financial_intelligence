"""Offline EDGAR test helpers shared by tests/pipeline, tests/store and root-level tests."""

from datetime import date, datetime, timedelta


def recent_weekdays(n: int, before: date | None = None) -> list[date]:
    """The n most recent weekdays strictly before `before` (default today), oldest first."""
    d, out = before or date.today(), []
    while len(out) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            out.append(d)
    return sorted(out)


def day_ts(d: date, hour: int = 15) -> datetime:
    return datetime(d.year, d.month, d.day, hour, 0, 0)


def make_filing(
    acc,
    *,
    issuer_cik="1000",
    ticker="AAA",
    form="4",
    accepted=None,
    owners=(("500", "Ann Lee"),),
    txns=(),
    issuer_name="Aaa Inc",
    plan=False,
):
    """Hand-built parsed Filing. txns: (code, shares, price, date[, acquired_disposed]) tuples."""
    from insider_intel.edgar.models import Filing, NonDerivTxn, ReportingOwner

    rows = []
    for i, t in enumerate(txns):
        code, shares, price, d = t[:4]
        ad = t[4] if len(t) > 4 else ("A" if code in ("P", "A", "M") else "D")
        rows.append(
            NonDerivTxn(
                row_idx=i,
                security_title="Common Stock",
                transaction_date=d,
                code=code,
                shares=shares,
                price_per_share=price,
                acquired_disposed=ad,
                post_transaction_shares=10_000.0,
                direct_indirect="D",
            )
        )
    return Filing(
        accession_number=acc,
        form_type=form,
        accepted_at=accepted,
        issuer_cik=issuer_cik,
        issuer_name=issuer_name,
        issuer_ticker=ticker,
        is_10b5_1=plan,
        owners=[ReportingOwner(cik=c, name=n, is_officer=True, officer_title="CEO") for c, n in owners],
        non_derivative=rows,
    )


def make_entry(filing, day: date, form=None):
    from insider_intel.edgar.index import IndexEntry

    return IndexEntry(
        form or filing.form_type,
        filing.issuer_name or "X",
        filing.issuer_cik,
        day,
        f"edgar/data/{filing.issuer_cik}/{filing.accession_number}.txt",
        filing.accession_number,
    )


class FakeSource:
    """Stands in for EdgarSource: serves pre-built Filings per index day; can inject transient failures."""

    def __init__(self, items: list[tuple[date, object]]):
        self.items = list(items)  # (index day, Filing)
        self.fail: set[str] = set()
        self.blocked_days: set[date] = set()
        self.load_calls: list[str] = []
        self.index_calls: list[date] = []

    def add(self, day, filing):
        self.items.append((day, filing))

    def entries(self, day):
        from insider_intel.edgar.client import EdgarHTTPError

        self.index_calls.append(day)
        if day in self.blocked_days:
            raise EdgarHTTPError("503", 503)
        return [make_entry(f, d) for d, f in self.items if d == day]

    def load(self, entry):
        from insider_intel.edgar.client import EdgarHTTPError

        self.load_calls.append(entry.accession)
        if entry.accession in self.fail:
            raise EdgarHTTPError("503 for test", 503)
        return next(f for _, f in self.items if f.accession_number == entry.accession)

    def close(self):
        pass
