# Targets assume the active Python env already has the project installed (`pip install -e ".[dev]"`),
# or at least its dependencies: PYTHONPATH=src makes the package importable either way.
PY ?= python
export PYTHONPATH := src

.PHONY: test lint fmt app run migrate snapshot freshness seed-demo check-keys

test:        ## run the test suite
	$(PY) -m pytest -q

lint:        ## static checks (ruff) + formatting check
	ruff check .
	ruff format --check .

fmt:         ## auto-format and apply safe lint fixes
	ruff check --fix .
	ruff format .

app:         ## launch the Streamlit UI
	streamlit run app/streamlit_app.py

run:         ## run the ingestion pipeline once against the live database
	$(PY) -m insider_intel run

migrate:     ## apply pending schema migrations to the live database
	$(PY) -m insider_intel migrate

snapshot:    ## export derived tables to snapshot/ (Parquet + manifest.json)
	$(PY) -m insider_intel publish-snapshot --out snapshot/

freshness:   ## fail if the live data is stale or the last run failed
	$(PY) -m insider_intel freshness --max-business-days 5

seed-demo:   ## (re)build the fictional demo database
	$(PY) -m insider_intel seed-demo

check-keys:  ## one tiny live call per external service
	$(PY) -m insider_intel check-keys
