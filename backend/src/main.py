import os,logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from redis import Redis
from src.config.database import Base,engine,Session
# Imported so Base.metadata is complete before create_all.
from src.models import user,study,observation,run,review,audit,outbox  # noqa: F401
from src.routes import auth,research
from src.middlewares.errors import ErrorMiddleware
from src.middlewares.rate_limit import RateLimitMiddleware
from src.services import queue
@asynccontextmanager
async def lifespan(app):
    Base.metadata.create_all(engine)
    if os.environ.get('SEED_DEMO')=='true':
        try:
            from scripts.seed_demo import seed
            seed()
        except Exception: logging.exception('seed failed; manual seed available')
    # Recover registrations interrupted between commit and Redis delivery.
    try:
        with Session() as db: queue.relay(db)
    except Exception: logging.exception('initial queue relay failed; workers retry')
    yield
    try:
        # Best-effort final relay so a run queued just before shutdown is not
        # left only in the database.
        with Session() as db: queue.relay(db)
    except Exception: logging.exception('shutdown relay failed; workers retry')
app=FastAPI(lifespan=lifespan,title='Research Application')
app.add_middleware(ErrorMiddleware)
app.add_middleware(RateLimitMiddleware)
app.include_router(auth.router);app.include_router(research.router)
@app.get('/health')
def health():
    with engine.connect() as conn: conn.execute(text('SELECT 1'))
    Redis.from_url(os.environ['REDIS_URL']).ping()
    return {'status':'healthy'}
