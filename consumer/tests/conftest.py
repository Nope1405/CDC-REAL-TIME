import json
from pathlib import Path

import pytest

from app.events import RawMessage

FIXTURES = Path(__file__).parent / "fixtures"


def load_raw(name: str) -> RawMessage:
    data = json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))

    def enc(v):
        return None if v is None else json.dumps(v, ensure_ascii=False).encode()

    return RawMessage(
        topic=data["topic"],
        partition=data["partition"],
        offset=data["offset"],
        key=enc(data["key"]),
        value=enc(data["value"]),
        headers={k: v.encode() for k, v in data.get("headers", {}).items()},
        timestamp_ms=data.get("timestamp_ms"),
    )


@pytest.fixture
def raw():
    return load_raw
