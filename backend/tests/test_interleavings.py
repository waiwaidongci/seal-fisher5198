"""Interleavings from the brief: lost notification, failed archive write,
replaced archive, and a slow old calculation returning late."""
import threading

from sqlalchemy import select

from src.models.run import Run
from src.models.review import Review
from src.services import artifacts, queue, runs


class Admin:
    role = 'admin'
    email = 'admin@demo.local'


class Analyst:
    role = 'analyst'
    email = 'analyst@demo.local'


class Note:
    note = '复核：独立试件、固定方案；建议复核后进入复试预算。'


class Data:
    def __init__(self, study_id, name='交错研究'):
        self.study_id = study_id
        self.name = name
        self.parameters = {'alpha': 0.05}


def make_run(env, name='交错研究'):
    from src.models.study import Study
    from src.models.observation import Observation
    with env.db() as db:
        study = Study(name=name, source_filename='t.csv', description='')
        db.add(study)
        db.flush()
        db.add_all([
            Observation(study_id=study.id, source_key='A',
                        values={'group': 'A', 'success': 87, 'failure': 13}),
            Observation(study_id=study.id, source_key='B',
                        values={'group': 'B', 'success': 96, 'failure': 4}),
        ])
        db.commit()
        return runs.create(db, Data(study.id, name), Analyst())['id']


def test_notification_lost_then_archive_write_fails_then_recovers(env):
    env.redis.fail = True
    run_id = make_run(env)
    env.redis.fail = False
    assert env.redis.lists.get('analysis-queue') in (None, [])

    # Archive writing fails on the first attempt: no result is published, the
    # run is failed with a clear reason, and the gates stay closed.
    env.store.fail_write = True
    env.clock.advance(3)
    with env.db() as db:
        queue.relay(db)
    env.drain(env.worker())
    with env.db() as db:
        failed = db.get(Run, run_id)
        assert failed.status == 'failed'
        assert '留档写入失败' in failed.error
        gate = runs.get(db, run_id)['gate']
        assert not gate['view'] and not gate['download'] and not gate['approve']
        assert '失败' in gate['reason']

    # Retry reuses the number; the archive write succeeds and gates open.
    env.store.fail_write = False
    with env.db() as db:
        again = runs.retry(db, run_id, Analyst())
        assert again['id'] == run_id and again['status'] == 'queued'
    env.drain(env.worker(index=1))
    with env.db() as db:
        done = db.get(Run, run_id)
        assert done.status == 'completed' and done.attempt == 2
        assert done.integrity == 'restored' or done.integrity == 'ok'
        data = runs.get(db, run_id)
        assert data['gate']['view'] and data['gate']['approve'] and data['gate']['download']
        assert db.scalar(select(Review).where(Review.run_id == run_id)) is None


def test_replaced_archive_while_approved_keeps_record_and_reopens(env):
    run_id = make_run(env, '替换研究')
    env.drain(env.worker())
    with env.db() as db:
        done = db.get(Run, run_id)
        name = done.artifact_name
        result = done.result
        runs.approve(db, run_id, Note(), Admin())
    # Someone replaces the archive with another run's bytes.
    env.store.real.path(name).write_bytes(b'{"format":"other-run"}')
    with env.db() as db:
        data = runs.get(db, run_id)
        run = db.get(Run, run_id)
        assert run.integrity == 'restored'
        assert run.result == result
        assert data['review'] and data['review']['note'] == Note.note
        assert data['gate']['view'] and data['gate']['download']
        assert '恢复' in data['gate']['reason']
        # Export still serves the approved database result.
        assert 'Fisher' in runs.export(db, run_id, Admin())


def test_slow_old_calculation_cannot_overwrite_after_recovery(env):
    """Old attempt returns after recovery; its result and file are discarded."""
    import src.worker as worker_module
    import src.services.algorithm as algorithm

    run_id = make_run(env, '迟归研究')
    entered = threading.Event()
    release = threading.Event()

    def slow(snapshot, params):
        entered.set()
        release.wait(10)
        out = algorithm.calculate(snapshot, params)
        out['interpretation'] = '迟归进程的结果'
        return out

    worker_module.calculate = slow
    try:
        old = env.worker(index=0)
        thread = threading.Thread(target=old.handle, args=(run_id,))
        thread.start()
        assert entered.wait(5)
        with env.db() as db:
            assert db.get(Run, run_id).attempt == 1
    finally:
        pass

    # Old worker loses its lease; a live worker takes over and completes.
    env.clock.advance(31)
    env.clock.advance(3)
    with env.db() as db:
        assert queue.relay(db) == 1
    worker_module.calculate = algorithm.calculate
    env.drain(env.worker(index=1))
    with env.db() as db:
        done = db.get(Run, run_id)
        assert done.status == 'completed' and done.attempt == 2
        authoritative = done.result
        assert '迟归进程的结果' not in authoritative['interpretation']
        name = done.artifact_name

    # Now the late worker returns and must not write anything.
    release.set()
    thread.join(10)
    with env.db() as db:
        after = db.get(Run, run_id)
        assert after.result == authoritative
        assert after.attempt == 2
        assert after.status == 'completed'
        assert artifacts.check_and_repair(db, after)[0] == 'ok'
        assert artifacts.digest(after.result) == after.result_sha256
    assert env.store.real.path('run-%d-a1.json' % run_id).exists() is False
    assert env.store.read(name) == artifacts.canonical(
        artifacts.payload(run_id, after.checksum, authoritative))


def test_recovery_interrupted_twice_still_converges(env):
    """Process exits again during recovery: relay + claim are idempotent."""
    run_id = make_run(env, '反复恢复研究')
    # Interrupt 1: lost notification.
    env.redis.flush()
    env.clock.advance(3)
    with env.db() as db:
        queue.relay(db)
    # Interrupt 2: a claim happens, then the process dies again.
    worker_a = env.worker(index=0)
    with env.db() as db:
        assert queue.claim(db, run_id, worker_a.owner) is not None
    env.clock.advance(31)
    env.clock.advance(3)
    with env.db() as db:
        assert queue.relay(db) >= 1
    env.drain(env.worker(index=1))
    with env.db() as db:
        run = db.get(Run, run_id)
        assert run.status == 'completed'
        assert run.attempt == 2
        gate = runs.get(db, run_id)['gate']
        assert gate['view'] and gate['download']
        # Page-level stage text explains the recovery.
        assert '接管' in run.status_detail or '第2次' in run.status_detail
