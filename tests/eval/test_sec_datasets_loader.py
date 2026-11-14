import datetime as dt
import zipfile

import pytest

from insider_intel.eval import sec_datasets as sd


def tsv(cols, rows):
    return "\n".join(["\t".join(cols)] + ["\t".join(r) for r in rows]) + "\n"


SUB = ["ACCESSION_NUMBER", "FILING_DATE", "PERIOD_OF_REPORT", "DATE_OF_ORIG_SUB", "NO_SECURITIES_OWNED",
       "NOT_SUBJECT_SEC16", "FORM3_HOLDINGS_REPORTED", "FORM4_TRANS_REPORTED", "DOCUMENT_TYPE", "ISSUERCIK",
       "ISSUERNAME", "ISSUERTRADINGSYMBOL", "REMARKS", "AFF10B5ONE"]
OWN = ["ACCESSION_NUMBER", "RPTOWNERCIK", "RPTOWNERNAME", "RPTOWNER_RELATIONSHIP", "RPTOWNER_TITLE"]
NDT = ["ACCESSION_NUMBER", "NONDERIV_TRANS_SK", "SECURITY_TITLE", "TRANS_DATE", "DEEMED_EXECUTION_DATE",
       "TRANS_FORMTYPE", "TRANS_CODE", "EQUITY_SWAP_INVOLVED", "TRANS_TIMELINESS", "TRANS_SHARES",
       "TRANS_PRICEPERSHARE", "TRANS_ACQUIRED_DISP_CD", "SHRS_OWND_FOLWNG_TRANS", "VALU_OWND_FOLWNG_TRANS",
       "DIRECT_INDIRECT_OWNERSHIP", "NATURE_OF_OWNERSHIP"]


def ndt(acc, sk, date, code, sh, px, ad, post, di="D", title="Common Stock"):
    return [acc, sk, title, date, "", "4", code, "0", "", sh, px, ad, post, "", di, ""]


@pytest.fixture
def quarter_zip(tmp_path):
    files = {
        "SUBMISSION.tsv": tsv(SUB, [
            ["0001-25-000001", "05-JAN-2025", "03-JAN-2025", "", "", "", "", "", "4", "0000320193", "Apple", "aapl ", "", ""],
            ["0001-25-000002", "06-JAN-2025", "03-JAN-2025", "", "", "", "", "", "4/A", "0000789019", "MSFT", "", "", ""],
        ]),
        "REPORTINGOWNER.tsv": tsv(OWN, [
            ["0001-25-000001", "0001214156", "A", "Officer", "CEO"],
            ["0001-25-000001", "0000000099", "B", "Director", ""],
            ["0001-25-000002", "0000555", "C", "Director", ""],
        ]),
        # file order is deliberately not SK order for accession 1
        "NONDERIV_TRANS.tsv": tsv(NDT, [
            ndt("0001-25-000001", "102", "03-JAN-2025", "S", "50", "12.5", "D", "950"),
            ndt("0001-25-000001", "101", "02-JAN-2025", "P", "100", "10", "A", "1000", di="I"),
            ndt("0001-25-000002", "103", "03-JAN-2025", "A", "7", "", "A", ""),
        ]),
        "NONDERIV_HOLDING.tsv": tsv(["ACCESSION_NUMBER", "NONDERIV_HOLDING_SK"], []),
        "FOOTNOTES.tsv": tsv(["ACCESSION_NUMBER", "FOOTNOTE_ID", "FOOTNOTE_TXT"], [["0001-25-000001", "F1", "it's \"quoted\""]]),
        "readme.htm": "<html></html>",
    }
    p = tmp_path / "2025q3_form345.zip"
    with zipfile.ZipFile(p, "w") as z:
        for k, v in files.items():
            z.writestr(("2025q3_form345/" if k == "readme.htm" else "") + k, v)
    return p


def test_normalisers():
    assert sd.norm_cik("0000320193") == "320193" and sd.norm_cik("") is None and sd.norm_cik(None) is None
    assert sd.norm_cik("0000000") == "0" and sd.norm_cik(320193.0) == "320193"
    assert sd.norm_ticker(" aapl ") == "AAPL" and sd.norm_ticker("") is None
    assert sd.norm_owner_ciks("0099|1214156|99") == "1214156|99"
    assert sd.norm_owner_ciks(["10", "0003", "3"]) == "10|3"


def test_quarter_url():
    assert sd.quarter_url(2025, 3).endswith("/insider-transactions-data-sets/2025q3_form345.zip")


def test_load_and_official_rows(quarter_zip):
    con = sd.load_quarter(quarter_zip)
    df = sd.official_rows(con)
    assert list(df.columns) == sd.OFFICIAL_COLUMNS and len(df) == 3
    a1 = df[df.accession_number == "0001-25-000001"].reset_index(drop=True)
    # SK order, not file order
    assert list(a1.row_idx) == [0, 1] and list(a1.code) == ["P", "S"]
    assert a1.loc[0, "transaction_date"] == dt.date(2025, 1, 2)
    assert a1.loc[0, "shares"] == 100.0 and a1.loc[1, "price_per_share"] == 12.5
    assert a1.loc[0, "direct_indirect"] == "I" and a1.loc[0, "post_transaction_shares"] == 1000.0
    assert (a1.issuer_ticker == "AAPL").all() and (a1.issuer_cik == "320193").all()
    assert (a1.owner_ciks == "1214156|99").all() and (a1.form_type == "4").all()
    a2 = df[df.accession_number == "0001-25-000002"].iloc[0]
    assert a2.issuer_ticker is None and a2.form_type == "4/A" and a2.owner_ciks == "555"
    assert a2.price_per_share != a2.price_per_share and a2.post_transaction_shares != a2.post_transaction_shares  # NaN
    assert len(sd.official_rows(con, accessions=["0001-25-000002"])) == 1


def test_download_cached(tmp_path, quarter_zip):
    dest = tmp_path / "ds"
    dest.mkdir()
    (dest / quarter_zip.name).write_bytes(quarter_zip.read_bytes())
    # existing valid zip -> no network
    assert sd.download_quarter(2025, 3, dest) == dest / quarter_zip.name


def test_malformed_rows_are_counted_not_silently_dropped(tmp_path):
    """A line with too many columns must show up in load_rejects, never vanish."""
    import zipfile

    from insider_intel.eval.sec_datasets import load_quarter

    zp = tmp_path / "2099q1_form345.zip"
    with zipfile.ZipFile(zp, "w") as z:
        z.writestr("SUBMISSION.tsv", "ACCESSION_NUMBER\tDOCUMENT_TYPE\tISSUERCIK\tISSUERTRADINGSYMBOL\n"
                                     "0001-26-000001\t4\t123\tABC\n"
                                     "0001-26-000002\t4\t124\tDEF\tEXTRA\tCOLS\n")
    con = load_quarter(zp)
    assert con.execute("SELECT COUNT(*) FROM raw_submission").fetchone()[0] == 1
    assert dict(con.execute("SELECT * FROM load_rejects").fetchall())["SUBMISSION"] == 1
