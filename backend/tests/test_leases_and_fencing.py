"""Leases, takeover of lost workers, and fencing of late results."""
from sqlalchemy import select

from src.models.run import Run
from src.services import artifacts, queue, runs


class User:
    role = 'admin'
    email = 'admin@demo.local'


class Slow:
    """Marks that calculation started, then blocks until released."""

    def __init__(self):
        self.entered = None
        self.gate = None

    def __call__(self, snapshot, params):
        self.entered.set()
        self.gate.wait(10)
        from src.services.algorithm import calculate as real
        return real(snapshot, params)


def make_run(db):
    from src.models.study import Study
    from src.models.observation import Observation
    study = Study(name='T', source_filename='t.csv', description='')
    db.add(study)
    db.flush()
    db.add_all([
        Observation(study_id=study.id, source_key='A',
                    values={'group': 'A', 'success': 87, 'failure': 13}),
        Observation(study_id=study.id, source_key='B',
                    values={'group': 'B', 'success': 96, 'failure': 4}),
    ])
    db.commit()

    class Data:
        study_id = study.id
        name = '并发研究'
        parameters = {'alpha': 0.05}
    return runs.create(db, Data(), User())['id']


def test_active_research_is_not_stolen_while_lease_holds(env):
    with env.db() as db:
        run_id = make_run(db)
    worker_a = env.worker()
    worker_b = env.worker(index=1)

    with env.db() as db:
        assert queue.claim(db, run_id, worker_a.owner) is not None
        # The lease is valid: even an expired-looking attempt is rejected.
        assert queue.claim(db, run_id, worker_b.owner) is None
        run = db.get(Run, run_id)
        assert run.status == 'running' and run.attempt == 1
        assert run.owner_id == worker_a.owner
        # Heartbeat extends the lease and only its owner may extend it.
        assert queue.heartbeat(db, run_id, worker_a.owner, 1)
        assert not queue.heartbeat(db, run_id, worker_b.owner, 1)


def test_lost_worker_is_taken_over_and_late_result_is_discarded(env, monkeypatch):
    with env.db() as db:
        run_id = make_run(db)

    import src.worker as worker_module
    import src.services.algorithm as algorithm
    slow = Slow()
    import threading
    slow.entered = threading.Event()
    slow.gate = threading.Event()
    worker_module.calculate = slow
    try:
        old = env.worker(index=0)
        # The old worker starts computing and then 'dies' (stops heartbeating).
        thread = threading.Thread(target=old.handle, args=(run_id,))
        thread.start()
        assert slow.entered.wait(5)
        attempt_old = _attempt(run_id)

        # The lease expires; the relay makes the run deliverable again and a
        # different live worker takes it over by consuming the new hint.
        env.clock.advance(31)
        env.clock.advance(3)  # deliveries older than the grace window are stale
        with env.db() as db:
            assert queue.relay(db) == 1
        new = env.worker(index=1)
        worker_module.calculate = algorithm.calculate
        assert env.drain(new) >= 1
        with env.db() as db:
            done = db.get(Run, run_id)
            assert done.status == 'completed'
            assert done.attempt == attempt_old + 1
            assert done.owner_id is None
            authoritative = done.result
            name = done.artifact_name

        # Now the old worker wakes up with its stale result.
        slow.gate.set()
        thread.join(10)
        with env.db() as db:
            after = db.get(Run, run_id)
            assert after.status == 'completed'
            assert after.result == authoritative  # not overwritten
            assert after.attempt == attempt_old + 1
            assert artifacts.digest(after.result) == after.result_sha256
        # The late worker's archive file was discarded; the canonical file is
        # still the one recorded by the winning attempt.
        assert env.store.read(name) == artifacts.canonical(
            artifacts.payload(run_id, after.checksum, authoritative))
    finally:
        worker_module.calculate = algorithm.calculate


def test_repeated_crash_during_recovery_still_converges(env):
    with env.db() as db:
        run_id = make_run(db)
    # Crash 1: claimed, then process dies.
    a = env.worker(index=0)
    with env.db() as db:
        queue.claim(db, run_id, a.owner)
    env.clock.advance(31)
    # Crash 2: taken over, dies again before finishing.
    with env.db() as db:
        assert queue.relay(db) == 1
    b = env.worker(index=1)
    with env.db() as db:
        queue.claim(db, run_id, b.owner)
    env.clock.advance(31)
    env.clock.advance(3)
    with env.db() as db:
        assert queue.relay(db) == 1
    c = env.worker(index=2)
    env.drain(c)
    with env.db() as db:
        run = db.get(Run, run_id)
        assert run.status == 'completed'
        assert run.attempt == 3
        assert run.owner_id is None
        assert run.integrity == 'ok'
        # Only one outbox registration per attempt, and all delivered.
        from src.models.outbox import Outbox
        rows = db.scalars(select(Outbox).where(Outbox.run_id == run_id)).all()
        assert [r.attempt for r in rows] == [1, 2, 3]
        assert all(r.sent_at for r in rows)


def test_lost_notification_and_recovery_are_idempotent(env):
    with env.db() as db:
        run_id = make_run(db)
    env.redis.flush()  # everything pushed so far is lost
    env.clock.advance(3)  # beyond the notification grace window
    with env.db() as db:
        assert queue.relay(db) == 1
    second = None
    with env.db() as db:
        worker = env.worker()
        worker.handle(run_id)
        run = db.get(Run, run_id)
        assert run.status == 'completed'
        second = run.result
    # A duplicate stale notification arriving later cannot recompute.
    env.redis.rpush('analysis-queue', run_id)
    env.drain(env.worker(index=5))
    with env.db() as db:
        assert db.get(Run, run_id).result == second
        assert db.get(Run, run_id).attempt == 1


def _db():
    from src.config.database import Session
    return Session()


def _attempt(run_id):
    with _db() as db:
        return db.get(Run, run_id).attempt
