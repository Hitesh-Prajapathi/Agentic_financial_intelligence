# Skill: ranking insider trades
Deterministic, no LLM (`agents/symbolic_validation.py`). Only open-market **P** (buy) and **S** (sell) count.
Per ticker: `net = bought − sold`; ranked by `|net|`. Joint-filer double reports are removed; rows whose code and
acquired/disposed flag disagree are excluded. Each ranked ticker carries secondary signals: insider count (cluster
buying/selling), officer/director role, size relative to the insider's position, and the share traded under a 10b5-1 plan.
