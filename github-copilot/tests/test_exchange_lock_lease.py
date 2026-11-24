"""#107: pruning must not drop a lock a caller has looked up but not yet released."""

import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from copilot import credentials as c


class _GateLock:
    """A real Lock that pauses between lookup and acquire, where the race lives."""

    def __init__(self):
        self._lock = threading.Lock()
        self.looked_up = threading.Event()
        self.go = threading.Event()

    def acquire(self, *a, **kw):
        self.looked_up.set()
        self.go.wait(5)
        return self._lock.acquire(*a, **kw)

    def release(self):
        self._lock.release()

    def locked(self):
        return self._lock.locked()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()


def _reset():
    c._jwt_cache.clear()
    c._exchange_failure_cache.clear()
    c._exchange_locks.clear()
    getattr(c, "_exchange_lock_leases", {}).clear()


def _flood_and_prune():
    for i in range(c._EXCHANGE_CACHE_MAX_ENTRIES + 8):
        c._exchange_locks[f"idle{i}"] = threading.Lock()
    c._prune_exchange_caches()


def test_caller_between_lookup_and_acquire_keeps_its_lock(monkeypatch):
    _reset()
    raw = "gho_fixture_token"
    fp = c._token_fingerprint(raw)
    gate = c._exchange_locks[fp] = _GateLock()
    monkeypatch.setattr(c, "_exchange_copilot_token_locked",
                        lambda raw_token, fp, *, timeout: ("jwt", time.time() + 1800, None))
    worker = threading.Thread(target=c.exchange_copilot_token, args=(raw,), daemon=True)
    worker.start()
    try:
        assert gate.looked_up.wait(5)
        _flood_and_prune()
        # The next caller for the same token must queue on the SAME lock (single-flight).
        assert c._exchange_locks.get(fp) is gate
    finally:
        gate.go.set()
        worker.join(5)
    assert not worker.is_alive()


def test_lease_released_after_success_and_failure(monkeypatch):
    _reset()
    raw = "gho_fixture_token"
    fp = c._token_fingerprint(raw)
    monkeypatch.setattr(c, "_exchange_copilot_token_locked",
                        lambda raw_token, fp, *, timeout: ("jwt", time.time() + 1800, None))
    c.exchange_copilot_token(raw)
    assert fp not in c._exchange_lock_leases

    def boom(raw_token, fp, *, timeout):
        raise ValueError("exchange failed")

    c._jwt_cache.clear()
    monkeypatch.setattr(c, "_exchange_copilot_token_locked", boom)
    with pytest.raises(ValueError):
        c.exchange_copilot_token(raw)
    assert fp not in c._exchange_lock_leases
    # With no lease left, the idle entry is prunable again (the table stays bounded).
    _flood_and_prune()
    assert len(c._exchange_locks) <= c._EXCHANGE_CACHE_MAX_ENTRIES
