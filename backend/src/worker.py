"""Independent calculation workers.

Several processes (and threads inside each process) may run concurrently.
Every claim is atomic and fenced by (owner_id, attempt); a worker whose lease
expired and was taken over can no longer write a result. Already saved work is
never recalculated: completed runs are skipped and retries reuse the row.
"""
import logging
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.config.database import Session
# Imported so Base.metadata is complete before create_all.
from src.models import audit, observation, outbox, review, study, user  # noqa: F401
from src.models.run import Run
from src.services import queue
from src.services.algorithm import calculate
from src.services.artifacts import (artifact_name, canonical, default_store,
                                    digest_bytes, payload)
from src.utils.serializers import checksum
from src.config.database import Base, engine

LOG = logging.getLogger('worker')
HEARTBEAT = Path(os.environ.get('WORKER_HEARTBEAT', '/tmp/worker-heartbeat'))
CONCURRENCY = max(1, int(os.environ.get('WORKER_CONCURRENCY', '2')))


class Worker:
    def __init__(self, index=0, concurrency=None, store=None):
        self.owner = f'{socket.gethostname()}:{os.getpid()}:{index}'
        self.concurrency = concurrency or CONCURRENCY
        self.store = store or default_store()
        self.stop = threading.Event()
        self._keepers = {}
        self._lock = threading.Lock()

    # -- lease keeper ---------------------------------------------------
    def _start_keeper(self, run_id, attempt):
        done = threading.Event()
        with self._lock:
            self._keepers[(run_id, attempt)] = done

        def run():
            interval = max(1, queue.lease_seconds() // 3)
            while not done.wait(interval):
                try:
                    with Session() as db:
                        if not queue.heartbeat(db, run_id, self.owner, attempt):
                            LOG.warning('run=%s attempt=%s lease lost', run_id, attempt)
                            return
                except Exception:  # noqa: BLE001 - transient; retry next tick
                    LOG.exception('heartbeat failed for run=%s', run_id)

        thread = threading.Thread(target=run, daemon=True,
                                  name=f'lease-{run_id}-{attempt}')
        thread.start()
        return done

    def _stop_keeper(self, run_id, attempt, done):
        done.set()
        with self._lock:
            self._keepers.pop((run_id, attempt), None)

    # -- one delivery ---------------------------------------------------
    def handle(self, run_id):
        with Session() as db:
            run = queue.claim(db, run_id, self.owner)
            if run is None:
                LOG.info('skip run=%s (finished, owned elsewhere or missing)', run_id)
                return False
            attempt = run.attempt
            snapshot = run.snapshot
            params = run.parameters
            frozen = run.checksum
            LOG.info('claimed run=%s attempt=%s', run_id, attempt)

        keeper = self._start_keeper(run_id, attempt)
        result = None
        error = None
        try:
            if checksum(snapshot) != frozen:
                raise ValueError('输入快照校验失败')
            result = calculate(snapshot, params)
            result['input_sha256'] = frozen
        except Exception as exc:  # noqa: BLE001 - failure is a recorded state
            LOG.exception('run=%s failed', run_id)
            error = exc
        finally:
            self._stop_keeper(run_id, attempt, keeper)

        if error is not None:
            with Session() as db:
                won = queue.release(db, run_id, self.owner, attempt, error)
            if not won:
                LOG.warning('run=%s attempt=%s fenced out; failure not recorded',
                            run_id, attempt)
            return False

        # Write the archive first, then publish it conditionally on ownership.
        name = artifact_name(run_id, attempt)
        blob = canonical(payload(run_id, frozen, result))
        blob_sha = digest_bytes(blob)
        try:
            self.store.write_atomic(name, blob)
        except OSError as exc:
            LOG.exception('run=%s archive write failed', run_id)
            with Session() as db:
                queue.release(db, run_id, self.owner, attempt,
                              f'结果留档写入失败：{exc}')
            return False

        with Session() as db:
            won = queue.complete(db, run_id, self.owner, attempt, result,
                                 name, blob_sha)
        if not won:
            # Late worker: a newer attempt owns the run. Never let this file
            # masquerade as the canonical archive.
            self.store.remove(name)
            LOG.warning('run=%s attempt=%s finished late and was fenced out; '
                        'result discarded', run_id, attempt)
            return False
        LOG.info('run=%s attempt=%s completed', run_id, attempt)
        return True

    # -- delivery loop --------------------------------------------------
    def serve(self):
        Base.metadata.create_all(engine)
        HEARTBEAT.parent.mkdir(parents=True, exist_ok=True)
        HEARTBEAT.touch()
        pool = ThreadPoolExecutor(max_workers=self.concurrency,
                                  thread_name_prefix='calc')
        LOG.info('worker %s serving with concurrency=%s', self.owner, self.concurrency)
        last_relay = 0.0
        last_beat = 0.0
        try:
            while not self.stop.is_set():
                now = time.time()
                if now - last_beat >= 2:
                    HEARTBEAT.touch()
                    last_beat = now
                if now - last_relay >= max(2, queue.grace_seconds()):
                    self._relay()
                    last_relay = now
                run_id = queue.consume(timeout=1)
                if run_id is None:
                    continue
                pool.submit(self._guarded, run_id)
        except KeyboardInterrupt:
            LOG.info('worker stopping')
        finally:
            self.stop.set()
            pool.shutdown(wait=True)

    def _relay(self):
        try:
            with Session() as db:
                queue.relay(db)
        except Exception:  # noqa: BLE001 - relay retries next tick
            LOG.exception('relay failed')

    def _guarded(self, run_id):
        try:
            self.handle(run_id)
        except Exception:  # noqa: BLE001
            LOG.exception('delivery crashed for run=%s', run_id)


def main():
    logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'),
                        format='%(asctime)s %(levelname)s %(name)s %(message)s')
    worker = Worker(index=int(os.environ.get('WORKER_INDEX', '0')))
    worker.serve()


if __name__ == '__main__':
    main()
