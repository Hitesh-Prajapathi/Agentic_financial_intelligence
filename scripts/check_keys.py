"""Verify every API key with one tiny live call each:  python scripts/check_keys.py"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.health import check_all  # noqa: E402
from core.system import build_system  # noqa: E402

if __name__ == "__main__":
    bad = 0
    for r in check_all(build_system("live")):
        print(f"{'✅' if r['ok'] else '❌'} {r['service']:<18} {r['detail']}")
        bad += not r["ok"]
    sys.exit(1 if bad else 0)
