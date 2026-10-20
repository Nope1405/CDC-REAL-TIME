"""Elasticsearch bulk writer with per-item classification and backoff."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Callable

import structlog
from elasticsearch import ApiError, Elasticsearch
from elasticsearch import ConnectionError as EsConnectionError
from elasticsearch import ConnectionTimeout

from . import metrics
from .config import Settings
from .es_ops import classify_item, to_bulk_ops
from .events import IndexAction

log = structlog.get_logger(__name__)


class ShutdownRequested(Exception):
    """Stop was requested while waiting for Elasticsearch; nothing is committed."""


class RetryDeadlineExceeded(Exception):
    """Elasticsearch did not recover before the caller's deadline."""


@dataclass
class WriteResult:
    failed: list[tuple[IndexAction, str]] = field(default_factory=list)  # permanent -> DLQ
    indexed_at_ms: int = 0


class EsSink:
    def __init__(self, settings: Settings, client: Elasticsearch | None = None) -> None:
        self._s = settings
        self._es = client or Elasticsearch(
            settings.es_url, request_timeout=settings.es_request_timeout_s,
            max_retries=0, retry_on_timeout=False)

    def ping(self) -> bool:
        try:
            return bool(self._es.ping())
        except Exception:  # noqa: BLE001 - diagnostics only
            return False

    def write(
        self,
        actions: list[IndexAction],
        *,
        should_stop: Callable[[], bool],
        on_wait: Callable[[], None] | None = None,
        on_recover: Callable[[], None] | None = None,
        deadline: float | None = None,
    ) -> WriteResult:
        """Write all actions. Retries transient errors until they succeed.

        ``on_wait`` is called during every backoff sleep (the consumer uses it to
        pause partitions and keep polling so it is not kicked out of the group).
        """
        result = WriteResult()
        pending = list(actions)
        backoff_ms = self._s.retry_initial_ms
        waited = False
        started = time.perf_counter()

        while pending:
            indexed_at = int(time.time() * 1000)
            ops: list[dict] = []
            for action in pending:
                ops.extend(to_bulk_ops(action, index=self._s.index_for(action.entity),
                                       delete_mode=self._s.delete_mode, indexed_at=indexed_at))
            retry: list[IndexAction] = []
            try:
                resp = self._es.bulk(operations=ops)
                for action, item in zip(pending, resp["items"]):
                    op_type, body = next(iter(item.items()))
                    verdict = classify_item(op_type, body, action.kind)
                    if verdict == "ok":
                        metrics.DOCS_INDEXED.labels(action.entity, body.get("result", str(body.get("status")))).inc()
                    elif verdict == "retry":
                        metrics.BULK_ERRORS.labels(f"item_{body.get('status')}").inc()
                        retry.append(action)
                    else:
                        error = body.get("error", {})
                        reason = f"{error.get('type', body.get('status'))}: {error.get('reason', '')}"
                        metrics.BULK_ERRORS.labels("item_rejected").inc()
                        result.failed.append((action, reason))
                result.indexed_at_ms = int(time.time() * 1000)
            except (EsConnectionError, ConnectionTimeout) as exc:
                metrics.BULK_ERRORS.labels("connection").inc()
                log.warning("es_unavailable", error=str(exc), pending=len(pending))
                retry = pending
            except ApiError as exc:
                status = exc.meta.status
                if status == 429 or status >= 500:
                    metrics.BULK_ERRORS.labels(f"request_{status}").inc()
                    log.warning("es_request_retryable", status=status, pending=len(pending))
                    retry = pending
                else:
                    raise  # malformed bulk request = programming error; crash without committing

            pending = retry
            if not pending:
                break

            # ---- backoff with full jitter, keeping the consumer alive
            if should_stop():
                raise ShutdownRequested()
            if deadline is not None and time.monotonic() >= deadline:
                raise RetryDeadlineExceeded()
            sleep_s = random.uniform(0, backoff_ms) / 1000
            log.info("es_backoff", sleep_ms=int(sleep_s * 1000), pending=len(pending))
            end = time.monotonic() + sleep_s
            while time.monotonic() < end:
                if on_wait is not None:
                    on_wait()
                    waited = True
                if should_stop():
                    raise ShutdownRequested()
                time.sleep(min(0.1, max(0.0, end - time.monotonic())))
            backoff_ms = min(backoff_ms * 2, self._s.retry_max_ms)

        if waited and on_recover is not None:
            on_recover()
        metrics.BULK_DURATION.observe(time.perf_counter() - started)
        return result
