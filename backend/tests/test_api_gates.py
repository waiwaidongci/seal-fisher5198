"""HTTP-level checks: gates are enforced by the API, not just the UI."""
import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(env):
    from src.main import app
    with TestClient(app) as c:
        yield c


def token(client, email='admin@demo.local'):
    r = client.post('/auth/login', json={'email': email, 'password': 'Research2026!'})
    assert r.status_code == 200, r.text
    return {'Authorization': 'Bearer ' + r.json()['token']}


@pytest.fixture()
def setup(client, env):
    admin = token(client)
    r = client.post('/studies', headers=admin,
                    json={'name': '接口研究', 'source_filename': 'x.csv', 'description': ''})
    assert r.status_code == 200, r.text
    study_id = r.json()['id']
    for key, group, ok, bad in [('A', 'A', 87, 13), ('B', 'B', 96, 4)]:
        rr = client.post(f'/studies/{study_id}/observations', headers=admin,
                         json={'source_key': key,
                               'values': {'group': group, 'success': ok, 'failure': bad}})
        assert rr.status_code == 200, rr.text
    return client, env, admin, study_id


def test_api_flow_and_gates(setup):
    client, env, admin, study_id = setup
    r = client.post('/runs', headers=admin,
                    json={'study_id': study_id, 'name': '接口运行', 'parameters': {'alpha': 0.05}})
    assert r.status_code == 200, r.text
    run_id = r.json()['id']
    assert r.json()['status'] == 'queued'

    detail = client.get(f'/runs/{run_id}', headers=admin).json()
    assert detail['gate']['view'] is False
    assert detail['gate']['stage'] == 'queued'
    assert detail['gate']['reason']

    env.drain(env.worker())
    detail = client.get(f'/runs/{run_id}', headers=admin).json()
    assert detail['status'] == 'completed'
    assert detail['gate'] == {'view': True, 'approve': True, 'download': True,
                              'stage': 'completed',
                              'reason': '数据库结果与留档校验一致'}
    assert detail['result']['metrics']['Fisher双侧p'] > 0

    export = client.get(f'/runs/{run_id}/export', headers=admin)
    assert export.status_code == 200 and 'SHA256' in export.text

    viewer = token(client, 'viewer@demo.local')
    assert client.get(f'/runs/{run_id}/export', headers=viewer).status_code == 403

    rv = client.post(f'/runs/{run_id}/review', headers=admin,
                     json={'note': '复核：固定方案，独立试件，进入复试预算。'})
    assert rv.status_code == 200, rv.text
    assert client.get(f'/runs/{run_id}/export', headers=viewer).status_code == 200

    # Retrying a completed study is refused; the number never changes.
    assert client.post(f'/runs/{run_id}/retry', headers=admin, json={}).status_code == 409


def test_api_closes_view_when_archive_damaged_and_reports_why(setup):
    client, env, admin, study_id = setup
    run_id = client.post('/runs', headers=admin,
                         json={'study_id': study_id, 'name': '留档接口', 'parameters': {'alpha': 0.05}}).json()['id']
    env.drain(env.worker())
    with env.db() as db:
        from src.models.run import Run
        name = db.get(Run, run_id).artifact_name
    env.store.real.path(name).write_bytes(b'corrupt')
    env.store.fail_write = True
    detail = client.get(f'/runs/{run_id}', headers=admin).json()
    assert detail['gate']['view'] is False
    assert detail['result'] is None and detail['result_redacted']
    assert client.get(f'/runs/{run_id}/export', headers=admin).status_code == 409
    assert client.post(f'/runs/{run_id}/review', headers=admin,
                       json={'note': '本应在留档关闭时被拒绝。'}).status_code == 409
    # Once the archive can be restored, the API reopens all gates.
    env.store.fail_write = False
    detail = client.get(f'/runs/{run_id}', headers=admin).json()
    assert detail['gate']['view'] is True and detail['result'] is not None
    assert client.get(f'/runs/{run_id}/export', headers=admin).status_code == 200


def test_api_recovery_listing_explains_stage(setup):
    client, env, admin, study_id = setup
    env.redis.fail = True
    run_id = client.post('/runs', headers=admin,
                         json={'study_id': study_id, 'name': '中断接口', 'parameters': {'alpha': 0.05}}).json()['id']
    env.redis.fail = False
    listed = client.get('/runs', headers=admin).json()
    entry = next(x for x in listed if x['id'] == run_id)
    assert entry['status'] == 'queued'
    assert entry['gate']['stage'] == 'queued'
    assert '接取' in entry['gate']['reason'] or '排队' in entry['gate']['reason']
    env.clock.advance(3)
    # Reading /runs triggers a relay which repairs the lost notification.
    client.get('/runs', headers=admin)
    env.drain(env.worker())
    entry = next(x for x in client.get('/runs', headers=admin).json() if x['id'] == run_id)
    assert entry['status'] == 'completed'
    assert '校验一致' in entry['gate']['reason']
