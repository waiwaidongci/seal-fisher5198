"""Several concurrent workers share one queue without duplicating work."""
import threading
import time
from collections import Counter

from src.models.run import Run
from src.services import queue, runs


class Analyst:
    role = 'analyst'
    email = 'analyst@demo.local'


class Data:
    def __init__(self, study_id, name):
        self.study_id = study_id
        self.name = name
        self.parameters = {'alpha': 0.05}


def test_concurrent_workers_claim_each_run_exactly_once(env, monkeypatch):
    from src.models.study import Study
    from src.models.observation import Observation
    import src.worker as worker_module
    import src.services.algorithm as algorithm

    calls = Counter()
    lock = threading.Lock()
    original = algorithm.calculate

    def counting(snapshot, params):
        with lock:
            calls['total'] += 1
        time.sleep(0.02)
        return original(snapshot, params)

    worker_module.calculate = counting
    try:
        with env.db() as db:
            study = Study(name='并发批次', source_filename='c.csv', description='')
            db.add(study)
            db.flush()
            db.add_all([
                Observation(study_id=study.id, source_key='A',
                            values={'group': 'A', 'success': 87, 'failure': 13}),
                Observation(study_id=study.id, source_key='B',
                            values={'group': 'B', 'success': 96, 'failure': 4}),
            ])
            db.commit()
            ids = [runs.create(db, Data(study.id, f'并发{i}'), Analyst())['id']
                   for i in range(12)]

        # Eight workers consume the same queue concurrently.
        workers = [env.worker(index=i) for i in range(8)]
        threads = [threading.Thread(target=env.drain, args=(w,)) for w in workers]
        for t in threads:
            t.start()
        for t in threads:
            t.join(20)
        assert calls['total'] == len(ids)

        with env.db() as db:
            for run_id in ids:
                run = db.get(Run, run_id)
                assert run.status == 'completed', (run_id, run.status, run.error)
                assert run.attempt == 1
                assert run.integrity == 'ok'
                assert runs.get(db, run_id)['gate']['download']
                # Exactly one archive per run, matching its payload.
                from src.services import artifacts
                blob = env.store.read(run.artifact_name)
                assert blob == artifacts.canonical(
                    artifacts.payload(run.id, run.checksum, run.result))
    finally:
        worker_module.calculate = original


def test_two_workers_racing_for_one_claim(env):
    """The conditional UPDATE is the single point of serialization."""
    with env.db() as db:
        from src.models.study import Study
        from src.models.observation import Observation
        study = Study(name='争用', source_filename='c.csv', description='')
        db.add(study)
        db.flush()
        db.add_all([
            Observation(study_id=study.id, source_key='A',
                        values={'group': 'A', 'success': 87, 'failure': 13}),
            Observation(study_id=study.id, source_key='B',
                        values={'group': 'B', 'success': 96, 'failure': 4}),
        ])
        db.commit()
        run_id = runs.create(db, Data(study.id, '争用运行'), Analyst())['id']

    won = []
    barrier = threading.Barrier(2)

    def race(owner):
        from src.config.database import Session
        with Session() as db:
            barrier.wait(5)
            claimed = queue.claim(db, run_id, owner)
            if claimed is not None:
                won.append(owner)

    threads = [threading.Thread(target=race, args=(f'w{i}',)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert len(won) == 1
    with env.db() as db:
        assert db.get(Run, run_id).attempt == 1


def test_worker_serve_loop_consumes_and_recovers(env):
    """The real delivery loop starts, consumes hints and relays lost ones."""
    from src.models.study import Study
    from src.models.observation import Observation
    with env.db() as db:
        study = Study(name='循环批次', source_filename='l.csv', description='')
        db.add(study)
        db.flush()
        db.add_all([
            Observation(study_id=study.id, source_key='A',
                        values={'group': 'A', 'success': 87, 'failure': 13}),
            Observation(study_id=study.id, source_key='B',
                        values={'group': 'B', 'success': 96, 'failure': 4}),
        ])
        db.commit()
        run_id = runs.create(db, Data(study.id, '循环运行'), Analyst())['id']

    env.redis.flush()  # the initial hint is lost
    env.clock.advance(3)
    worker = env.worker(index=0)
    thread = threading.Thread(target=worker.serve, daemon=True)
    thread.start()
    deadline = time.time() + 8
    status = None
    while time.time() < deadline:
        with env.db() as db:
            status = db.get(Run, run_id).status
        if status == 'completed':
            break
        time.sleep(0.1)
    worker.stop.set()
    thread.join(10)
    assert status == 'completed'
    with env.db() as db:
        run = db.get(Run, run_id)
        assert run.attempt == 1 and run.integrity == 'ok'
