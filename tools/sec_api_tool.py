from sec_api import InsiderTradingApi
from config.settings import settings
import time
import requests
from datetime import datetime, timedelta, timezone
from telemetry.otel_setup import tracer

class UpstreamAPIError(Exception):
    pass

class ParsingError(Exception):
    pass

class SecApiTool:
    def __init__(self):
        self.api = InsiderTradingApi(api_key=settings.sec_api_key)
        self.last_request_time = 0.0

    def fetch_recent_filings(self, hours: int = 24) -> list[dict]:
        """Fetch Form 4 filings from the last N hours."""
        with tracer.start_as_current_span("sec_api.fetch_filings") as span:
            span.set_attribute("sec.query_type", "Form4")
            span.set_attribute("sec.hours_lookback", hours)
            span.set_attribute("sec.api_endpoint", "InsiderTradingApi")
            
            now = datetime.now(timezone.utc)
            start_time = now - timedelta(hours=hours)
            
            start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
            end_str = now.strftime("%Y-%m-%dT%H:%M:%S")
            
            query = {
              "query": {
                "query_string": {
                  "query": f"formType:\"4\" AND filedAt:[{start_str} TO {end_str}]"
                }
              },
              "size": "50", # Fetch up to 50 recent filings
              "sort": [{"filedAt": {"order": "desc"}}]
            }
            
            retries = 3
            delays = [2, 4, 8]
            
            for attempt in range(retries):
                try:
                    self._rate_limit()
                    response = self.api.get_data(query)
                    filings = response.get('filings', [])
                    span.set_attribute("sec.results_count", len(filings))
                    return filings
                except requests.exceptions.HTTPError as e:
                    if attempt < retries - 1:
                        time.sleep(delays[attempt])
                    else:
                        raise UpstreamAPIError(f"SEC API failed after {retries} retries: {e}")
                except Exception as e:
                    if attempt < retries - 1:
                        time.sleep(delays[attempt])
                    else:
                        raise UpstreamAPIError(f"SEC API encountered an error: {e}")
            return []
            
    def _rate_limit(self):
        """Enforce max 10 requests/second"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < 0.1:
            time.sleep(0.1 - elapsed)
        self.last_request_time = time.time()

    def parse_filing(self, raw: dict):
        """Extract filing dict and list of transaction dicts."""
        try:
            filing_dict = {
                'accession_number': raw.get('accessionNo'),
                'filing_date': raw.get('filedAt'),
                'issuer_name': raw.get('issuer', {}).get('name'),
                'issuer_ticker': raw.get('issuer', {}).get('ticker'),
                'insider_name': raw.get('reportingOwner', {}).get('name'),
                'insider_title': raw.get('reportingOwner', {}).get('title'),
                'form_type': raw.get('formType', '4'),
                'raw_json': raw
            }

            transactions = []
            non_deriv_table = raw.get('nonDerivativeTable', {})
            trans_list = non_deriv_table.get('transactions', [])
            
            for txn in trans_list:
                transactions.append({
                    'accession_number': filing_dict['accession_number'],
                    'transaction_date': txn.get('transactionDate'),
                    'security_title': txn.get('securityTitle'),
                    'transaction_type': txn.get('acquiredDisposedCode', txn.get('transactionCode')),
                    'shares': txn.get('shares'),
                    'price_per_share': txn.get('pricePerShare'),
                    'ownership_nature': txn.get('directOrIndirectOwnership'),
                    'post_transaction_shares': txn.get('sharesOwnedFollowingTransaction')
                })
                     
            return filing_dict, transactions
        except Exception as e:
            raise ParsingError(f"Error parsing filing {raw.get('accessionNo')}: {e}")
