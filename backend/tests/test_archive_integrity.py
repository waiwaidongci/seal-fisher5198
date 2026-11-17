"""Database/archive verification gates view, approval and download.

The database result is authoritative; the archived original is restored if
missing, damaged or swapped. Approved research is never recalculated.
"""
import json

import pytest
from sqlalchemy import select

from src.models.review import Review
from src.models.run import Run
from src.services import artifacts, queue, runs


class Admin:
    role = 'admin'
    email = 'admin@demo.local'


class Analyst:
    role = 'analyst'
    email = 'analyst@demo.local'


class Viewer:
    role = 'viewer'
    email = 'viewer@demo.local'


class Note:
    def __init__(self, note='复核通过：限定于本轮独立试件，建议复试复核。'):
        self.note = note


def completed_run(env, study_name='留档校验研究'):
    from src.models.study import Study
    from src.models.observation import Observation
    from src.services import runs as runs_module

    class Data:
        def __init__(self, study_id):
            self.study_id = study_id
            self.name = study_name + '·运行'
            self.parameters = {'alpha': 0.05}

    with env.db() as db:
        study = Study(name=study_name, source_filename='t.csv', description='')
        db.add(study)
        db.flush()
        db.add_all([
            Observation(study_id=study.id, source_key='A',
                        values={'group': 'A', 'success': 87, 'failure': 13}),
            Observation(study_id=study.id, source_key='B',
                        values={'group': 'B', 'success': 96, 'failure': 4}),
        ])
        db.commit()
        run_id = runs_module.create(db, Data(study.id), Analyst())['id']
    env.drain(env.worker())
    with env.db() as db:
        run = db.get(Run, run_id)
        assert run.status == 'completed' and run.integrity == 'ok'
        return run_id, run.artifact_name


def test_intact_archive_opens_every_gate(env):
    run_id, name = completed_run(env)
    with env.db() as db:
        data = runs.get(db, run_id)
        assert data['gate'] == {'view': True, 'approve': True, 'download': True,
                                'stage': 'completed',
                                'reason': '数据库结果与留档校验一致'}
        assert runs.export(db, run_id, Analyst()) is not None


def test_missing_archive_is_restored_and_gates_reopen(env):
    run_id, name = completed_run(env)
    env.store.real.path(name).unlink()
    with env.db() as db:
        state, reason = artifacts.check_and_repair(db, db.get(Run, run_id))
        assert state == 'restored'
        assert '缺失' in reason
        run = db.get(Run, run_id)
        assert run.integrity == 'restored'
        data = runs.get(db, run_id)
        assert data['gate']['view'] and data['gate']['approve'] and data['gate']['download']
        assert '恢复' in data['gate']['reason']
    # The restored file is byte-identical to the canonical payload.
    with env.db() as db:
        run = db.get(Run, run_id)
        expected = artifacts.canonical(artifacts.payload(run.id, run.checksum, run.result))
        assert env.store.read(name) == expected


def test_corrupt_archive_is_restored(env):
    run_id, name = completed_run(env)
    env.store.real.path(name).write_bytes(b'{"broken": ')
    with env.db() as db:
        state, _ = artifacts.check_and_repair(db, db.get(Run, run_id))
        assert state == 'restored'
        assert runs.get(db, run_id)['gate']['download']


def test_swapped_archive_from_another_run_is_restored(env):
    first, first_name = completed_run(env)
    second, second_name = completed_run(env, '第二个研究')
    # "串档": run #2's archive is overwritten by run #1's file.
    env.store.real.path(second_name).write_bytes(env.store.read(first_name))
    with env.db() as db:
        state, reason = artifacts.check_and_repair(db, db.get(Run, second))
        assert state == 'restored'
        assert '串档' in reason or '属于运行' in reason
        assert runs.get(db, second)['gate']['view']
        # The other run's original is untouched.
        assert artifacts.check_and_repair(db, db.get(Run, first))[0] == 'ok'


def test_database_result_tampering_blocks_gates(env):
    run_id, name = completed_run(env)
    with env.db() as db:
        run = db.get(Run, run_id)
        tampered = json.loads(json.dumps(run.result))
        tampered['metrics']['Fisher双侧p'] = 0.0001
        run.result = tampered
        db.commit()
    # The recorded result hash exposes the tampering; the archive is not
    # rewritten from the inconsistent row.
    original_file = env.store.read(name)
    with env.db() as db:
        state, reason = artifacts.check_and_repair(db, db.get(Run, run_id))
        assert state == 'bad'
        data = runs.get(db, run_id)
        assert not data['gate']['view']
        assert not data['gate']['approve']
        assert not data['gate']['download']
        assert '不一致' in data['gate']['reason']
        assert data['result'] is None and data['result_redacted']
    assert env.store.read(name) == original_file


def test_missing_archive_and_unwritable_store_keeps_gates_closed(env):
    run_id, name = completed_run(env)
    env.store.real.path(name).unlink()
    env.store.fail_write = True
    with env.db() as db:
        state, reason = artifacts.check_and_repair(db, db.get(Run, run_id))
        assert state == 'bad'
        assert '写档失败' in reason
        gate = runs.get(db, run_id)['gate']
        assert not gate['view'] and not gate['download'] and not gate['approve']
    env.store.fail_write = False
    with env.db() as db:
        assert artifacts.check_and_repair(db, db.get(Run, run_id))[0] == 'restored'


def test_approve_and_export_are_refused_while_integrity_bad(env):
    from fastapi import HTTPException

    run_id, name = completed_run(env)
    with env.db() as db:
        run = db.get(Run, run_id)
        run.artifact_name = 'gone.json'
        run.integrity = 'bad'
        run.integrity_reason = '留档文件缺失'
        db.commit()
    env.store.fail_write = True  # repair cannot complete
    with env.db() as db:
        with pytest.raises(HTTPException) as err:
            runs.approve(db, run_id, Note(), Admin())
        assert err.value.status_code == 409
        with pytest.raises(HTTPException) as err2:
            runs.export(db, run_id, Admin())
        assert err2.value.status_code == 409
        assert db.scalar(select(Review).where(Review.run_id == run_id)) is None
    env.store.fail_write = False
    with env.db() as db:
        # Recovery reopens the gates without recomputing the study.
        assert runs.approve(db, run_id, Note(), Admin())['run_id'] == run_id
        assert runs.export(db, run_id, Viewer()) is not None


def test_approved_research_is_never_recomputed(env):
    run_id, name = completed_run(env)
    with env.db() as db:
        runs.approve(db, run_id, Note(), Admin())
        result = db.get(Run, run_id).result
        attempt = db.get(Run, run_id).attempt
    # Damage the archive: the approved record stays approved, result unchanged.
    env.store.real.path(name).write_bytes(b'not json')
    queue.promote.__globals__  # noqa: B018 - keep imports explicit
    with env.db() as db:
        data = runs.get(db, run_id)
        assert data['gate']['download'] and data['gate']['approve']
        assert data['review'] is not None
        run = db.get(Run, run_id)
        assert run.result == result
        assert run.attempt == attempt
        assert run.integrity == 'restored'
    # A stale notification cannot turn an approved run back into work.
    env.redis.rpush('analysis-queue', run_id)
    env.drain(env.worker(index=9))
    with env.db() as db:
        run = db.get(Run, run_id)
        assert run.result == result and run.attempt == attempt
        assert run.status == 'completed'


def test_legacy_completed_run_without_digest_is_closed(env):
    """A completed row created outside the service cannot be trusted."""
    run_id, name = completed_run(env)
    with env.db() as db:
        run = db.get(Run, run_id)
        run.result_sha256 = None
        db.commit()
    with env.db() as db:
        data = runs.get(db, run_id)
        assert not data['gate']['view']
        assert '缺少登记哈希' in data['gate']['reason']
        assert data['result'] is None
