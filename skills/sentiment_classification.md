# Skill: tweet sentiment
Batches of up to 30 tweets → one JSON-mode LLM call (free-model chain). Output is validated strictly: only
`bullish|bearish|neutral` labels with confidence clamped to 0–1 are stored; anything missing stays unclassified
(no fabricated defaults). Low-confidence labels get a second opinion from a different model (`skills/learning_loop.py`).
Tweets are treated as untrusted data — instructions inside them are ignored.
