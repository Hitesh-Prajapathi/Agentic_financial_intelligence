"""Run the ingestion pipeline once from the terminal:  python scripts/run_pipeline.py"""
import logging
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.system import build_system  # noqa: E402

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    system = build_system("live")
    out = system.supervisor.run_ingestion_pipeline(trigger="manual")
    print(f"\nRun {out['run_id']} → {out['status']}")
    for name, st in system.supervisor.progress.snapshot()["stages"].items():
        print(f"  {name:<10} {st['status']:<8} {st['seconds'] and round(st['seconds'], 1)}s  {st['detail'] if st['status'] not in ('ok',) else ''}")
    if out.get("error"):
        print("error:", out["error"])
    sys.exit(0 if out["status"] in ("success", "partial") else 1)
