from datetime import datetime, timezone
import logging
from telemetry.otel_setup import tracer
from agents.symbolic_validation import validate_and_rank

class RankingAgent:
    def __init__(self, db_tool, settings):
        self.db_tool = db_tool
        self.settings = settings
        self.run_id = None
        self.logger = logging.getLogger(__name__)

    def run(self) -> dict:
        """
        Queries recent transactions from the database, runs the Symbolic Validation Gate 
        deterministically, and stores the ranked trades to top_trades table.
        """
        with tracer.start_as_current_span("ranking_agent.run") as span:
            span.set_attribute("agent.name", "RankingAgent")
            if self.run_id:
                span.set_attribute("agent.run_id", self.run_id)
            
            # Use current UTC date as logic bounds
            today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            
            query = """
            SELECT t.*, f.filing_date, f.issuer_ticker, f.insider_name 
            FROM transactions t 
            JOIN filings f ON t.accession_number = f.accession_number 
            WHERE DATE(f.filing_date) >= CURRENT_DATE - INTERVAL 1 DAY
            """
            
            try:
                df_txns = self.db_tool.query_df(query)
                transactions = df_txns.to_dict('records')
                
                if not transactions:
                    span.set_attribute("trades.ranked_count", 0)
                    return {"top_trades": [], "checksum": "empty"}

                # Execute deterministic Symbolic Validation Gate (ZERO LLM usage)
                ranked_df, checksum = validate_and_rank(transactions, top_n=self.settings.top_n_trades)
                
                if ranked_df.empty:
                    span.set_attribute("trades.ranked_count", 0)
                    return {"top_trades": [], "checksum": checksum}

                top_trades_records = ranked_df.to_dict('records')
                
                # Normalize schema for insertion
                insert_payload = []
                for t in top_trades_records:
                    insert_payload.append({
                        'run_date': today_str,
                        'rank': t['rank'],
                        'issuer_ticker': t['issuer_ticker'],
                        'insider_name': t.get('insider_name', 'Unknown'),
                        'net_dollar_value': t['net_dollar_value'],
                        'transaction_count': t['transaction_count'],
                        'dominant_direction': t['dominant_direction'],
                        'validation_checksum': checksum
                    })
                    
                # Insert into DB
                self.db_tool.insert_top_trades(insert_payload)
                
                span.set_attribute("trades.ranked_count", len(insert_payload))
                span.set_attribute("trades.validation_checksum", checksum)
                
                return {
                    "top_trades": insert_payload,
                    "checksum": checksum,
                    "ranked_df": ranked_df   # exposed for Governance Gate checksum re-verification
                }

            except Exception as e:
                self.logger.error(f"Ranking Agent Error: {e}")
                span.set_attribute("error", str(e))
                raise
