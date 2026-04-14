from tools.sec_api_tool import SecApiTool, ParsingError, UpstreamAPIError
from telemetry.otel_setup import tracer
import logging

class DBWriteError(Exception):
    pass

class SecDataAgent:
    def __init__(self, sec_tool, db_tool, settings):
        self.sec_tool = sec_tool
        self.db_tool = db_tool
        self.settings = settings
        self.run_id = None
        self.logger = logging.getLogger(__name__)

    def run(self) -> dict:
        with tracer.start_as_current_span("sec_data_agent.run") as span:
            span.set_attribute("agent.name", "SecDataAgent")
            if self.run_id:
                span.set_attribute("agent.run_id", self.run_id)
            
            results = {
                "filings_ingested": 0,
                "transactions_found": 0,
                "duplicates_skipped": 0,
                "raw_filings": [],
                "db_filings": [],
                "raw_transactions": []
            }
            
            try:
                raw_filings = self.sec_tool.fetch_recent_filings(hours=self.settings.filing_lookback_hours)
                results["raw_filings"] = raw_filings
                
                if raw_filings:
                    placeholders = ','.join(['?'] * len(raw_filings))
                    acc_nos = [f.get('accessionNo') for f in raw_filings if f.get('accessionNo')]
                    
                    if acc_nos:
                        existing = self.db_tool.query(f"SELECT accession_number FROM filings WHERE accession_number IN ({placeholders})", acc_nos)
                        existing_set = {row[0] for row in existing}
                        
                        new_filings = []
                        new_txns = []
                        
                        for raw in raw_filings:
                            acc_no = raw.get('accessionNo')
                            if not acc_no or acc_no in existing_set:
                                results["duplicates_skipped"] += 1
                                continue
                                
                            try:
                                filing, txns = self.sec_tool.parse_filing(raw)
                                new_filings.append(filing)
                                new_txns.extend(txns)
                                results["db_filings"].append(filing)
                            except ParsingError as e:
                                self.logger.error(f"Parsing error: {e}")
                                
                        if new_filings:
                            try:
                                self.db_tool.insert_filings(new_filings)
                                results["filings_ingested"] = len(new_filings)
                                
                                self.db_tool.insert_transactions(new_txns)
                                results["transactions_found"] = len(new_txns)
                                results["raw_transactions"] = new_txns
                            except Exception as e:
                                raise DBWriteError(f"Database insert failed: {e}")
                                
            except UpstreamAPIError as e:
                self.logger.error(f"Upstream API Error: {e}")
                span.set_attribute("error", str(e))
                raise
                
            span.set_attribute("filings.ingested", results["filings_ingested"])
            span.set_attribute("filings.duplicates_skipped", results["duplicates_skipped"])
            
            return results
