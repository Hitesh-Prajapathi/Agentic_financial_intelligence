"""Chat quality/latency stats from `query_log` (Health tab): question count, latency, 👍/👎 and confidence mix."""


def chat_metrics(db) -> dict:
    up, down = db.query("SELECT SUM(CASE WHEN feedback > 0 THEN 1 ELSE 0 END), "
                        "SUM(CASE WHEN feedback < 0 THEN 1 ELSE 0 END) FROM query_log")[0]
    n, avg_ms, p90_ms = db.query("SELECT COUNT(*), AVG(latency_ms), quantile_cont(latency_ms, 0.9) FROM query_log")[0]
    mix = dict(db.query("SELECT confidence, COUNT(*) FROM query_log GROUP BY confidence"))
    return {"questions": int(n or 0), "avg_latency_ms": float(avg_ms or 0), "p90_latency_ms": float(p90_ms or 0),
            "thumbs_up": int(up or 0), "thumbs_down": int(down or 0), "retrieval_confidence_mix": mix}
