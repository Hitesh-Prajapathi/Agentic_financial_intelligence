"""Build a tiny official-style quarterly zip (real SEC column names) for the eval CLI tests."""

import zipfile
from pathlib import Path

SUB = [
    "ACCESSION_NUMBER",
    "FILING_DATE",
    "PERIOD_OF_REPORT",
    "DATE_OF_ORIG_SUB",
    "NO_SECURITIES_OWNED",
    "NOT_SUBJECT_SEC16",
    "FORM3_HOLDINGS_REPORTED",
    "FORM4_TRANS_REPORTED",
    "DOCUMENT_TYPE",
    "ISSUERCIK",
    "ISSUERNAME",
    "ISSUERTRADINGSYMBOL",
    "REMARKS",
]
OWN = ["ACCESSION_NUMBER", "RPTOWNERCIK", "RPTOWNERNAME", "RPTOWNER_RELATIONSHIP", "RPTOWNER_TITLE"]
NDT = [
    "ACCESSION_NUMBER",
    "NONDERIV_TRANS_SK",
    "SECURITY_TITLE",
    "TRANS_DATE",
    "DEEMED_EXECUTION_DATE",
    "TRANS_FORMTYPE",
    "TRANS_CODE",
    "EQUITY_SWAP_INVOLVED",
    "TRANS_TIMELINESS",
    "TRANS_SHARES",
    "TRANS_PRICEPERSHARE",
    "TRANS_ACQUIRED_DISP_CD",
    "SHRS_OWND_FOLWNG_TRANS",
    "VALU_OWND_FOLWNG_TRANS",
    "DIRECT_INDIRECT_OWNERSHIP",
    "NATURE_OF_OWNERSHIP",
]


def _tsv(cols, rows):
    return "\n".join(["\t".join(cols)] + ["\t".join(str(x) for x in r) for r in rows]) + "\n"


def write_quarter_zip(path: Path, filings: list[dict], with_aff: bool = True) -> Path:
    """filings: dict(acc, form='4', cik, ticker, filed='DD-MON-YYYY', aff='', owners=[(cik, title)],
    rows=[(date 'DD-MON-YYYY', code, shares, price, ad[, post_shares])])."""
    sub_cols = SUB + (["AFF10B5ONE"] if with_aff else [])
    sub, own, ndt, sk = [], [], [], 0
    for f in filings:
        r = [
            f["acc"],
            f["filed"],
            "",
            "",
            "",
            "",
            "",
            "",
            f.get("form", "4"),
            f["cik"],
            f"{f['ticker']} Inc",
            f["ticker"],
            "",
        ]
        sub.append(r + ([f.get("aff", "")] if with_aff else []))
        for c, t in f["owners"]:
            own.append([f["acc"], c, f"Owner {c}", "Officer", t])
        for d, code, sh, px, ad, *post in f["rows"]:
            sk += 1
            ndt.append(
                [
                    f["acc"],
                    sk,
                    "Common Stock",
                    d,
                    "",
                    "4",
                    code,
                    "0",
                    "",
                    sh,
                    px,
                    ad,
                    post[0] if post else "1000",
                    "",
                    "D",
                    "",
                ]
            )
    files = {
        "SUBMISSION.tsv": _tsv(sub_cols, sub),
        "REPORTINGOWNER.tsv": _tsv(OWN, own),
        "NONDERIV_TRANS.tsv": _tsv(NDT, ndt),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as z:
        for k, v in files.items():
            z.writestr(k, v)
    return path
