"""Durable queue registration, run leases and fencing.

Concurrency model
-----------------
* A run row is the queue record. Registering work commits the run and an
  outbox row in one transaction; Redis is only a delivery hint.
* A worker claims work with one conditional UPDATE: it may only start while
  the run is queued, or running with an expired/absent lease. The claim
  increments ``attempt``, which doubles as the fencing token.
* Every write to a claimed run (heartbeat, failure, completion) is conditional
  on (owner_id, attempt, status='running'). A worker that comes back after its
  lease was taken over cannot overwrite the newer result.
* The outbox relay re-delivers registrations lost between DB commit and Redis
  push (and re-notifies runs that are still orphaned after a grace period).
"""
import os

from redis import Redis
from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError

from src.models.outbox import Outbox
from src.models.run import Run
from src.utils import clock

QUEUE = 'analysis-queue'
DEFAULT_LEASE = 30
DEFAULT_GRACE = 5

_REDIS = None


def redis_client():
    global _REDIS
    if _REDIS is None:
        _REDIS = Redis.from_url(os.environ['REDIS_URL'])
    return _REDIS


def lease_seconds():
    return int(os.environ.get('RUN_LEASE_SECONDS', DEFAULT_LEASE))


def grace_seconds():
    return int(os.environ.get('NOTIFY_GRACE_SECONDS', DEFAULT_GRACE))


def queue_name():
    return os.environ.get('RUN_QUEUE', QUEUE)


# ---------------------------------------------------------------- claiming


def claim(db, run_id, owner, lease=None):
    """Atomically take ownership. Returns the refreshed row or None."""
    now = clock.now_iso()
    expiry = clock.after(lease if lease is not None else lease_seconds())
    result = db.execute(
        update(Run)
        .where(
            Run.id == run_id,
            Run.status.in_(('queued', 'running')),
            or_(Run.owner_id.is_(None), Run.lease_expires_at.is_(None),
                Run.lease_expires_at < now),
        )
        .values(status='running', attempt=Run.attempt + 1, owner_id=owner,
                heartbeat_at=now, lease_expires_at=expiry,
                status_detail='计算进行中')
        .execution_options(synchronize_session=False)
    )
    db.commit()
    if result.rowcount != 1:
        db.rollback()
        return None
    # synchronize_session=False means any previously loaded Run object is
    # stale; force a fresh read before returning it to the caller.
    db.expire_all()
    run = db.get(Run, run_id)
    if run is not None and run.attempt > 1:
        run.status_detail = f'计算进行中（第{run.attempt}次接取；此前接取失联后自动接管）'
        db.commit()
    return run


def owns(db, run_id, owner, attempt):
    run = db.get(Run, run_id)
    return bool(run and run.status == 'running' and run.owner_id == owner
                and run.attempt == attempt)


def heartbeat(db, run_id, owner, attempt, lease=None):
    """Extend a lease. Returns False once this worker has been fenced out."""
    now = clock.now_iso()
    expiry = clock.after(lease if lease is not None else lease_seconds())
    result = db.execute(
        update(Run)
        .where(Run.id == run_id, Run.owner_id == owner, Run.attempt == attempt,
               Run.status == 'running')
        .values(heartbeat_at=now, lease_expires_at=expiry)
        .execution_options(synchronize_session=False)
    )
    db.commit()
    db.expire_all()
    return result.rowcount == 1


def release(db, run_id, owner, attempt, error, cancelled=False):
    """Conditionally record failure. A fenced worker cannot change the row."""
    detail = ('计算已中止：接取已被接管，本次结果作废' if cancelled
              else '计算失败，可沿用原编号重试')
    result = db.execute(
        update(Run)
        .where(Run.id == run_id, Run.owner_id == owner, Run.attempt == attempt,
               Run.status == 'running')
        .values(status='failed', error=str(error)[:2000], owner_id=None,
                lease_expires_at=None, heartbeat_at=clock.now_iso(),
                status_detail=detail)
        .execution_options(synchronize_session=False)
    )
    db.commit()
    db.expire_all()
    return result.rowcount == 1


def complete(db, run_id, owner, attempt, result, artifact, artifact_sha):
    """Publish a result only while this exact attempt still owns the run.

    Returns False for a late worker whose lease was taken over; the caller
    must discard its file and leave the newer attempt untouched.
    """
    from src.services.artifacts import digest
    values = {
        'status': 'completed',
        'result': result,
        'error': '',
        'result_sha256': digest(result),
        'artifact_name': artifact,
        'artifact_sha256': artifact_sha,
        'integrity': 'ok',
        'integrity_reason': '',
        'owner_id': None,
        'lease_expires_at': None,
        'heartbeat_at': clock.now_iso(),
        'status_detail': '计算完成；数据库结果与留档校验一致，已开放查看、复核与下载'
                         if attempt == 1 else
                         f'计算完成（第{attempt}次接取）；数据库结果与留档校验一致，已开放查看、复核与下载',
    }
    result_row = db.execute(
        update(Run)
        .where(Run.id == run_id, Run.owner_id == owner, Run.attempt == attempt,
               Run.status == 'running', Run.result.is_(None))
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    db.commit()
    db.expire_all()
    return result_row.rowcount == 1


# ---------------------------------------------------------------- outbox


def notify(db, run_id, target_attempt):
    """Idempotently register a delivery for a specific upcoming attempt.

    A crash between the run commit and the Redis push leaves this row pending;
    a crash after the push only produces a harmless duplicate message.
    """
    exists = db.scalar(select(Outbox).where(Outbox.run_id == run_id,
                                            Outbox.attempt == target_attempt))
    if exists:
        return exists
    row = Outbox(run_id=run_id, attempt=target_attempt,
                 created_at=clock.now_iso())
    db.add(row)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return db.scalar(select(Outbox).where(Outbox.run_id == run_id,
                                              Outbox.attempt == target_attempt))
    return row


def orphan_runs(db, now=None):
    """Runs that need a (new) delivery: queued, or running with a dead lease."""
    now = now or clock.now_iso()
    return list(db.scalars(
        select(Run).where(
            or_(
                Run.status == 'queued',
                (Run.status == 'running')
                & or_(Run.owner_id.is_(None), Run.lease_expires_at.is_(None),
                      Run.lease_expires_at < now),
            )
        ).order_by(Run.id)
    ))


def relay(db, redis=None, now=None, grace=None):
    """Make orphaned runs deliverable, then push due messages.

    Returns the number of messages pushed. Safe to call repeatedly and from
    several processes: claims remain the single point of contention.
    """
    redis = redis or redis_client()
    now = now or clock.now_iso()
    grace = grace_seconds() if grace is None else grace
    cutoff = clock.after(-grace)

    orphans = orphan_runs(db, now)
    pending = []
    for run in orphans:
        target = (run.attempt or 0) + 1
        row = db.scalar(select(Outbox).where(Outbox.run_id == run.id,
                                             Outbox.attempt == target))
        if row is None:
            row = Outbox(run_id=run.id, attempt=target, created_at=now)
            db.add(row)
            try:
                db.flush()
            except IntegrityError:
                db.rollback()
                row = db.scalar(select(Outbox).where(Outbox.run_id == run.id,
                                                     Outbox.attempt == target))
        pending.append((run, row))
    db.commit()

    pushed = 0
    for run, row in pending:
        if row.sent_at is not None and row.sent_at >= cutoff:
            continue
        # Re-check the run immediately before pushing: another worker may
        # have claimed it since the orphan scan.
        fresh = db.get(Run, run.id)
        if not fresh or fresh.status not in ('queued', 'running'):
            continue
        if fresh.status == 'running' and fresh.lease_expires_at and fresh.lease_expires_at >= now:
            continue
        try:
            redis.rpush(queue_name(), fresh.id)
        except Exception:  # noqa: BLE001 - keep the row unsent for next relay
            db.rollback()
            continue
        row.sent_at = clock.now_iso()
        row.attempts = (row.attempts or 0) + 1
        pushed += 1
        db.commit()
    db.commit()

    # Cancel stale pending rows whose run already finished.
    stale = db.scalars(select(Outbox).where(Outbox.sent_at.is_(None))).all()
    for row in stale:
        fresh = db.get(Run, row.run_id)
        if fresh is None or fresh.status not in ('queued', 'running'):
            row.sent_at = clock.now_iso()
    db.commit()
    return pushed


def register(db, run_id):
    """Commit-safe registration used right after creating/retrying a run.

    Always leaves a durable outbox row even if Redis is unavailable; the
    caller may attempt an immediate best-effort relay.
    """
    run = db.get(Run, run_id)
    return notify(db, run_id, (run.attempt or 0) + 1)


def kick(db, redis=None):
    """Best-effort immediate delivery; never fails the caller."""
    try:
        return relay(db, redis)
    except Exception:
        db.rollback()
        return 0


def consume(redis=None, timeout=2):
    """Pop one delivery hint. Returns a run id or None."""
    redis = redis or redis_client()
    item = redis.blpop(queue_name(), timeout=timeout)
    if not item:
        return None
    try:
        return int(item[1])
    except (TypeError, ValueError):
        return None


def promote(db, run_id):
    """Turn a queued run back into a deliverable state; used by retry."""
    run = db.get(Run, run_id)
    if run is None:
        return None
    run.status = 'queued'
    run.owner_id = None
    run.lease_expires_at = None
    run.heartbeat_at = None
    run.error = ''
    # A retryable run has no usable saved result; a completed run is refused
    # before reaching here, so clearing stale partial fields is safe.
    run.result = None
    run.result_sha256 = None
    run.artifact_name = None
    run.artifact_sha256 = None
    run.integrity = 'pending'
    run.integrity_reason = ''
    run.status_detail = '已登记重试（沿用原运行编号），等待空闲计算进程接取'
    db.commit()
    return run
