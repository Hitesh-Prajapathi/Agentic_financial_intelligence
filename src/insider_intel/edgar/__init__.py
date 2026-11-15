"""EDGAR-native Form 4 ingestion: polite client, discovery, parser, quality helpers.

Nothing here touches the network or reads environment variables at import time.
"""

from .form4 import Form4ParseError, filing_to_rows, parse_form4  # noqa: F401
from .models import Filing, NonDerivTxn, ReportingOwner  # noqa: F401
