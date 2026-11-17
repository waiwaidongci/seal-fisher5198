"""Run archival files.

The database row is the research record. The archived JSON file is the
immutable original used for cross-checking; it is only considered usable when
its bytes match both the recorded artifact hash and a canonical rendering of
the database result. If the file is missing, corrupt or belongs to another run
("串档"), it is restored from the database original.
"""
import hashlib
import json
import os
from pathlib import Path


FORMAT = 'seal-fisher/run-artifact/v1'


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(',', ':')).encode('utf-8')


def digest_bytes(value):
    return hashlib.sha256(value).hexdigest()


def digest(value):
    return digest_bytes(canonical(value))


def payload(run_id, input_sha256, result):
    return {'format': FORMAT, 'run_id': int(run_id), 'input_sha256': input_sha256,
            'result': result}


def artifact_name(run_id, attempt):
    return f'run-{int(run_id)}-a{int(attempt or 0)}.json'


class ArtifactStore:
    def __init__(self, directory):
        self.directory = Path(directory)

    def ensure_dir(self):
        self.directory.mkdir(parents=True, exist_ok=True)

    def path(self, name):
        return self.directory / name

    def write_atomic(self, name, data):
        self.ensure_dir()
        target = self.path(name)
        temp = target.with_name(target.name + f'.{os.getpid()}.{id(data)}.part')
        try:
            temp.write_bytes(data)
            os.replace(temp, target)
        finally:
            if temp.exists():
                try:
                    temp.unlink()
                except OSError:
                    pass
        return target

    def read(self, name):
        return self.path(name).read_bytes()

    def exists(self, name):
        return bool(name) and self.path(name).exists()

    def remove(self, name):
        try:
            self.path(name).unlink()
        except (OSError, TypeError):
            pass


_STORE = None


def default_store():
    global _STORE
    if _STORE is None:
        _STORE = ArtifactStore(os.environ.get('ARTIFACTS_DIR', '/app/artifacts'))
    return _STORE


def _mark(run, state, reason):
    run.integrity = state
    run.integrity_reason = reason or ''


def check_and_repair(db, run, store=None, repair=True):
    """Verify DB result against archive; restore archive when possible.

    Returns (state, reason) where state is one of ok/restored/bad. The caller
    must hold a session; no recomputation of the study is ever performed.
    """
    store = store or default_store()
    if run.status != 'completed':
        return run.integrity, run.integrity_reason
    if run.result is None:
        _mark(run, 'bad', '数据库缺少计算结果，无法开放查看')
        db.commit()
        return 'bad', run.integrity_reason

    db_digest = digest(run.result)
    if not run.result_sha256:
        # A completed run is only ever written together with this digest; a
        # missing value means the row was altered outside the service. Treat
        # it as unverifiable rather than trusting the database blindly.
        _mark(run, 'bad', '数据库结果缺少登记哈希，无法校验，已关闭查看、复核与下载')
        db.commit()
        return 'bad', run.integrity_reason
    if run.result_sha256 != db_digest:
        _mark(run, 'bad', '数据库结果与登记哈希不一致，已关闭查看、复核与下载')
        db.commit()
        return 'bad', run.integrity_reason

    embedded = run.result.get('input_sha256') if isinstance(run.result, dict) else None
    if embedded != run.checksum:
        _mark(run, 'bad', '结果内输入校验与冻结快照不一致，已关闭查看、复核与下载')
        db.commit()
        return 'bad', run.integrity_reason

    name = run.artifact_name or artifact_name(run.id, run.attempt)
    expected = canonical(payload(run.id, run.checksum, run.result))
    expected_digest = digest_bytes(expected)

    cause = None
    try:
        raw = store.read(name)
    except FileNotFoundError:
        raw, cause = None, '留档文件缺失'
    except OSError as exc:
        raw, cause = None, f'留档文件无法读取（{exc}）'
    if raw is not None and raw != expected:
        recorded = run.artifact_sha256
        if recorded and digest_bytes(raw) != recorded:
            cause = '留档文件哈希与数据库登记不一致'
        else:
            cause = '留档文件内容与本运行不匹配（可能串档）'
        try:
            parsed = json.loads(raw.decode('utf-8'))
            if isinstance(parsed, dict) and parsed.get('run_id') not in (None, run.id):
                cause = f'留档文件属于运行 #{parsed.get("run_id")}（串档）'
            elif isinstance(parsed, dict) and parsed.get('input_sha256') not in (None, run.checksum):
                cause = '留档文件输入校验与本次冻结输入不一致（串档）'
        except (ValueError, UnicodeDecodeError):
            cause = cause + '，且文件无法解析'

    if raw is not None and raw == expected:
        changed = False
        if run.artifact_name != name:
            run.artifact_name = name
            changed = True
        if run.artifact_sha256 != expected_digest:
            run.artifact_sha256 = expected_digest
            changed = True
        # Keep the 'restored' note so the page can explain what happened;
        # a run whose archive never failed is plainly 'ok'.
        if run.integrity not in ('ok', 'restored'):
            _mark(run, 'ok', '')
            changed = True
        if changed:
            db.commit()
        return 'ok', ''

    if not repair:
        _mark(run, 'bad', (cause or '留档校验失败') + '，已关闭查看、复核与下载')
        db.commit()
        return 'bad', run.integrity_reason

    try:
        store.write_atomic(name, expected)
    except OSError as exc:
        _mark(run, 'bad', f'{cause or "留档校验失败"}，恢复原件写档失败：{exc}；入口保持关闭')
        db.commit()
        return 'bad', run.integrity_reason

    run.artifact_name = name
    run.artifact_sha256 = expected_digest
    _mark(run, 'restored', f'{cause or "留档异常"}；已由数据库原件恢复并重新校验')
    db.commit()
    return 'restored', run.integrity_reason

