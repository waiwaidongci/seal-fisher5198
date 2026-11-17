"""Shared test harness: SQLite database, in-memory queue, temp archive.

The fakes preserve the semantics that matter for the scenarios:
* the fake queue is durable across "process restarts" (module-level list);
* the fake clock moves at the test's command, so leases expire on demand.
"""
import os
import sys
import tempfile
import time
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

TMP = tempfile.mkdtemp(prefix='seal-fisher-tests-')
os.environ['DATABASE_URL'] = 'sqlite:///' + os.path.join(TMP, 'db.sqlite3')
os.environ['REDIS_URL'] = 'redis://127.0.0.1:1/0'
os.environ['JWT_SECRET'] = 'test-secret'
os.environ['ARTIFACTS_DIR'] = os.path.join(TMP, 'artifacts')
os.environ['RUN_LEASE_SECONDS'] = '30'
os.environ['NOTIFY_GRACE_SECONDS'] = '2'
os.environ.setdefault('SEED_DEMO', 'false')

from src.config.database import Base, Session, engine  # noqa: E402
from src.models import audit, observation, outbox, review, run, study, user  # noqa: E402

Base.metadata.create_all(engine)


class FakeRedis:
    """Durable message list plus a switch that can 'lose' a push."""

    def __init__(self):
        self.lists = {}
        self.drop = False
        self.fail = False
        self.pushes = []

    def rpush(self, key, value):
        if self.fail:
            raise ConnectionError('redis unavailable')
        self.pushes.append((key, value))
        if self.drop:
            self.drop = False
            return 0
        self.lists.setdefault(key, []).append(int(value))
        return len(self.lists[key])

    def blpop(self, key, timeout=0):
        values = self.lists.get(key) or []
        if not values:
            return None
        return key, str(values.pop(0))

    def flush(self):
        self.lists.clear()
        self.pushes.clear()


class FakeClock:
    def __init__(self):
        self.offset = 0.0

    def advance(self, seconds):
        self.offset += seconds

    def now(self):
        return time.time() + self.offset


class Store:
    """Mirrors the real ArtifactStore with failure injection."""

    def __init__(self, directory):
        from src.services.artifacts import ArtifactStore
        self.real = ArtifactStore(directory)
        self.fail_write = False

    def __getattr__(self, name):
        if name == 'write_atomic':
            return self._write
        return getattr(self.real, name)

    def _write(self, name, data):
        if self.fail_write:
            raise OSError('disk full (injected)')
        return self.real.write_atomic(name, data)


@pytest.fixture()
def env(monkeypatch):
    """Fresh DB, queue, archive and controllable clock for one test."""
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    fake_redis = FakeRedis()
    clock = FakeClock()
    store = Store(os.environ['ARTIFACTS_DIR'])
    import shutil
    shutil.rmtree(store.real.directory, ignore_errors=True)
    store.real.ensure_dir()

    # Demo accounts for HTTP-level tests.
    from src.models.user import User
    from src.utils.security import hash_password
    with Session() as db:
        for role in ('admin', 'analyst', 'viewer'):
            db.add(User(email=f'{role}@demo.local', role=role,
                        password_hash=hash_password('Research2026!')))
        db.commit()

    from src.services import queue
    from src.utils import clock as clock_module
    monkeypatch.setattr(queue, 'redis_client', lambda: fake_redis)
    monkeypatch.setattr(queue, '_REDIS', fake_redis)
    monkeypatch.setattr(clock_module, 'now_iso', lambda: clock.now() and _iso(clock.now()))
    monkeypatch.setattr(clock_module, 'after', lambda seconds: _iso(clock.now() + seconds))
    monkeypatch.setattr('src.services.artifacts.default_store', lambda: store)
    monkeypatch.setattr('src.services.artifacts._STORE', store)
    # worker module resolves default_store at call time through artifacts
    yield Env(fake_redis, clock, store)


def _iso(ts):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')


class Env:
    def __init__(self, redis, clock, store):
        self.redis = redis
        self.clock = clock
        self.store = store

    def db(self):
        return Session()

    def worker(self, index=0, concurrency=1):
        from src.worker import Worker
        return Worker(index=index, concurrency=concurrency, store=self.store)

    def drain(self, worker, limit=50):
        """Run delivery hints until the queue is empty."""
        done = 0
        while done < limit:
            run_id = _consume(self.redis)
            if run_id is None:
                break
            worker.handle(run_id)
            done += 1
        return done


def _consume(fake_redis):
    item = fake_redis.blpop('analysis-queue', timeout=0)
    return int(item[1]) if item else None
