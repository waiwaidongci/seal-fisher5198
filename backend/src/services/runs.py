import csv
import io
import json

from fastapi import HTTPException
from sqlalchemy import select

from src.models.review import Review
from src.models.run import Run
from src.services import artifacts, queue, studies
from src.services.audits import log
from src.services.domain_validation import validate_parameters
from src.utils import clock
from src.utils.serializers import checksum, now, record

# Reasons shown when an integrity check blocks an entry point.
BLOCK_VIEW = '结果与留档校验未通过，暂不开放查看'


def _queue_enqueue(db, run_id):
    """Best-effort immediate delivery; durable outbox row already committed."""
    queue.kick(db)


def create(db, data, user):
    studies.require(db, data.study_id)
    rows = studies.observations(db, data.study_id)
    if not rows:
        raise HTTPException(422, '先录入至少一条业务观测')
    try:
        params = validate_parameters(data.parameters)
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, str(exc))
    snapshot = [{'source_key': x['source_key'], 'values': x['values']} for x in rows]
    run = Run(study_id=data.study_id, name=data.name, parameters=params,
              snapshot=snapshot, checksum=checksum(snapshot), created_at=now(),
              status='queued', attempt=0, integrity='pending',
              status_detail='排队登记已保存，等待传输到计算队列')
    db.add(run)
    db.flush()
    log(db, user, 'run.queue', str(run.id))
    # Registration and notification are two steps; the outbox row makes the
    # gap recoverable and makes a duplicate notification harmless.
    queue.notify(db, run.id, 1)
    db.commit()
    _queue_enqueue(db, run.id)
    return record(run)


def retry(db, run_id, user):
    """Reuse the original run number; never recalculate a saved result."""
    run = db.get(Run, run_id)
    if not run:
        raise HTTPException(404, '运行不存在')
    if run.status == 'completed':
        raise HTTPException(409, '已完成的研究保留原结果，不重算；如需新口径请新建运行')
    if run.status == 'running' and run.lease_expires_at and run.lease_expires_at >= clock.now_iso():
        raise HTTPException(409, '该研究正在被活跃计算进程接取，无需重试')
    queue.promote(db, run_id)
    log(db, user, 'run.retry', f'{run_id}（沿用原编号）')
    queue.notify(db, run_id, (run.attempt or 0) + 1)
    db.commit()
    _queue_enqueue(db, run_id)
    return get(db, run_id)


def reconcile(db):
    """Recover registrations whose delivery was interrupted and reconcile
    finished runs whose archive changed while nobody was looking."""
    try:
        queue.kick(db)
    except Exception:  # noqa: BLE001 - page still renders; retry next poll
        db.rollback()
    for run in db.scalars(select(Run).where(Run.status == 'completed')):
        try:
            artifacts.check_and_repair(db, run)
        except Exception:  # noqa: BLE001 - a damaged archive is reported below
            db.rollback()
            run.integrity = 'bad'
            run.integrity_reason = '留档校验过程异常，入口保持关闭'
            db.commit()


def _shape(run):
    data = record(run)
    data.pop('owner_id', None)
    data.setdefault('attempt', 0)
    return data


def _present(db, run, check=True):
    if check and run.status == 'completed':
        artifacts.check_and_repair(db, run)
        db.refresh(run)
    data = _shape(run)
    review = db.scalar(select(Review).where(Review.run_id == run.id))
    data['review'] = record(review) if review else None
    data['gate'] = gate(db, run)
    # The gate must actually close viewing: do not leak result content when
    # verification failed, even through the JSON payload.
    if run.status == 'completed' and not data['gate']['view']:
        data['result'] = None
        data['result_redacted'] = True
    return data


def get(db, run_id):
    run = db.get(Run, run_id)
    if not run:
        raise HTTPException(404, '运行不存在')
    return _present(db, run)


def list_runs(db):
    data = []
    for run_id in db.scalars(select(Run.id).order_by(Run.id.desc())):
        run = db.get(Run, run_id)
        if run is None:
            continue
        data.append(_present(db, run, check=run.status == 'completed' and
                             run.integrity not in ('ok', 'restored')))
    return data


def gate(db, run):
    """Explain whether view/approve/export are currently open, and why."""
    if run.status != 'completed':
        return {'view': False, 'approve': False, 'download': False,
                'stage': run.status,
                'reason': run.status_detail or '计算尚未完成'}
    if run.integrity == 'ok':
        return {'view': True, 'approve': True, 'download': True, 'stage': 'completed',
                'reason': '数据库结果与留档校验一致'}
    if run.integrity == 'restored':
        return {'view': True, 'approve': True, 'download': True, 'stage': 'completed',
                'reason': run.integrity_reason or '留档已由数据库原件恢复并通过校验'}
    return {'view': False, 'approve': False, 'download': False,
            'stage': 'integrity',
            'reason': run.integrity_reason or '结果与留档校验未通过，已关闭查看、复核与下载'}


def _require_integrity(db, run):
    state, reason = artifacts.check_and_repair(db, run)
    db.refresh(run)
    if state not in ('ok', 'restored'):
        raise HTTPException(409, f'结果与留档校验未通过，已关闭查看、复核与下载：{reason}')


def approve(db, run_id, data, user):
    run = db.get(Run, run_id)
    if not run:
        raise HTTPException(404, '运行不存在')
    if run.status != 'completed':
        raise HTTPException(409, '只能复核已完成运行')
    _require_integrity(db, run)
    if db.scalar(select(Review).where(Review.run_id == run_id)):
        raise HTTPException(409, '已复核，记录不可覆盖')
    obj = Review(run_id=run_id, note=data.note, author=user.email, created_at=now())
    db.add(obj)
    log(db, user, 'review.approve', f'{run_id}: {data.note}')
    db.commit()
    return record(obj)


def export(db, run_id, user):
    run = db.get(Run, run_id)
    if not run:
        raise HTTPException(404, '运行不存在')
    if run.status != 'completed':
        raise HTTPException(409, '尚无完成结果')
    _require_integrity(db, run)
    review = db.scalar(select(Review).where(Review.run_id == run_id))
    if user.role == 'viewer' and not review:
        raise HTTPException(403, '观察员只能导出已复核结果')
    data = _shape(run)
    stream = io.StringIO()
    writer = csv.writer(stream)
    writer.writerow(['运行编号', '输入校验SHA256', '指标', '值'])
    for key, value in data['result']['metrics'].items():
        writer.writerow([run_id, run.checksum, key,
                         json.dumps(value, ensure_ascii=False)
                         if isinstance(value, (dict, list)) else value])
    writer.writerow([run_id, run.checksum, '解释', data['result']['interpretation']])
    log(db, user, 'run.export', run_id)
    db.commit()
    return '﻿' + stream.getvalue()
