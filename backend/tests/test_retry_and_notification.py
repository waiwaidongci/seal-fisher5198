"""Retry keeps the original run number and never recomputes saved results."""
import pytest
from sqlalchemy import select

from src.models.run import Run
from src.services import queue, runs


class User:
    role = 'analyst'
    email = 'analyst@demo.local'


def seed_study(db):
    from src.models.study import Study
    from src.models.observation import Observation
    study = Study(name='T', source_filename='t.csv', description='')
    db.add(study)
    db.flush()
    db.add_all([
        Observation(study_id=study.id, source_key='A', values={'group': 'A', 'success': 87, 'failure': 13}),
        Observation(study_id=study.id, source_key='B', values={'group': 'B', 'success': 96, 'failure': 4}),
    ])
    db.commit()
    return study


class Data:
    def __init__(self, study_id):
        self.study_id = study_id
        self.name = '本轮研究'
        self.parameters = {'alpha': 0.05}


def test_retry_reuses_number_and_skips_saved_result(env):
    # Interrupt between registration and notification: Redis is unreachable
    # at push time, so the run is committed but nothing was delivered.
    env.redis.fail = True
    with env.db() as db:
        study = seed_study(db)
        run = runs.create(db, Data(study.id), User())
        run_id = run['id']
    env.redis.fail = False
    assert env.redis.lists.get('analysis-queue') in (None, [])
    with env.db() as db:
        assert db.scalar(select(__import__('src.models.outbox', fromlist=['Outbox']).Outbox).where(__import__('src.models.outbox', fromlist=['Outbox']).Outbox.run_id == run_id)).sent_at is None

    # Registration survived the gap; the relay re-sends it.
    with env.db() as db:
        assert queue.relay(db) == 1
    # Duplicate notification is harmless.
    with env.db() as db:
        queue.relay(db, grace=0)
        queue.relay(db, grace=0)
    with env.db() as db:
        assert len(db.scalars(select(Run).where(Run.id == run_id)).all()) == 1

    worker = env.worker()
    assert env.drain(worker) >= 1
    with env.db() as db:
        saved = db.get(Run, run_id)
        assert saved.status == 'completed'
        assert saved.attempt == 1
        first = saved.result

    # Re-delivery of a completed run must not recompute.
    env.redis.rpush('analysis-queue', run_id)
    env.drain(worker)
    with env.db() as db:
        assert db.get(Run, run_id).result == first
        assert db.get(Run, run_id).attempt == 1

    # Retry of a completed run is refused outright.
    from fastapi import HTTPException
    with env.db() as db:
        with pytest.raises(HTTPException) as err:
            runs.retry(db, run_id, User())
        assert err.value.status_code == 409

    # A failed run can be retried under the same number.
    with env.db() as db:
        r = db.get(Run, run_id)
        r.status = 'failed'
        r.error = '模拟计算失败'
        r.result = None
        r.owner_id = None
        r.lease_expires_at = None
        db.commit()
    with env.db() as db:
        again = runs.retry(db, run_id, User())
        assert again['id'] == run_id
        assert again['status'] == 'queued'
        assert again['attempt'] == 1  # attempt increments on claim, not retry
    env.drain(worker)
    with env.db() as db:
        final = db.get(Run, run_id)
        assert final.status == 'completed'
        assert final.attempt == 2


def test_failed_run_is_recorded_then_retry_completes(env):
    import src.services.algorithm as algorithm

    def boom(snapshot, params):
        raise ValueError('模拟分析错误')

    original = algorithm.calculate
    import src.worker as worker_module
    worker_module.calculate = boom
    try:
        with env.db() as db:
            study = seed_study(db)
            run_id = runs.create(db, Data(study.id), User())['id']
        env.drain(env.worker())
        with env.db() as db:
            failed = db.get(Run, run_id)
            assert failed.status == 'failed'
            assert '模拟分析错误' in (failed.error or '')
            assert failed.attempt == 1
        with env.db() as db:
            again = runs.retry(db, run_id, User())
            assert again['id'] == run_id and again['status'] == 'queued'
    finally:
        worker_module.calculate = original
    env.drain(env.worker())
    with env.db() as db:
        final = db.get(Run, run_id)
        assert final.status == 'completed'
        assert final.attempt == 2
        assert final.integrity == 'ok'
