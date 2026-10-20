"""Shared helpers for benchmark scripts (run on the host against published ports)."""

from __future__ import annotations

import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results"

# Windows consoles default to cp1252; Vietnamese output would raise UnicodeEncodeError.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")


def load_env() -> None:
    """Load ../.env into os.environ without overriding existing variables."""
    path = ROOT / ".env"
    if not path.exists():
        path = ROOT / ".env.example"
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def pg_dsn(port_var: str = "POSTGRES_PORT") -> str:
    load_env()
    return (f"host=localhost port={os.environ.get(port_var, '5432')} "
            f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} "
            f"password={os.environ['POSTGRES_PASSWORD']}")


def es_url() -> str:
    load_env()
    return f"http://localhost:{os.environ.get('ELASTICSEARCH_PORT', '9200')}"


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile (p in 0..100)."""
    if not values:
        return math.nan
    ordered = sorted(values)
    rank = max(1, math.ceil(p / 100 * len(ordered)))
    return ordered[rank - 1]


def summarize(values: Iterable[float]) -> dict[str, float]:
    vals = list(values)
    return {
        "count": len(vals),
        "p50": percentile(vals, 50),
        "p95": percentile(vals, 95),
        "p99": percentile(vals, 99),
        "max": max(vals) if vals else math.nan,
        "mean": sum(vals) / len(vals) if vals else math.nan,
    }


def write_result(kind: str, payload: dict) -> Path:
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / f"{time.strftime('%Y%m%d-%H%M%S')}-{kind}.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return path


def fmt_ms(summary: dict[str, float]) -> str:
    return " ".join(f"{k}={v:.0f}ms" if k != "count" else f"n={v}" for k, v in summary.items())
