import os
from sqlalchemy import create_engine,event
from sqlalchemy.orm import DeclarativeBase, sessionmaker
class Base(DeclarativeBase):
    pass
_url=os.environ['DATABASE_URL']
_extra={'pool_pre_ping':True}
if _url.startswith('sqlite'):
    # Local test/dev database: allow worker threads to share the file.
    _extra['connect_args']={'check_same_thread':False,'timeout':30}
engine = create_engine(_url,**_extra)
if _url.startswith('sqlite'):
    @event.listens_for(engine,'connect')
    def _sqlite_pragmas(conn,record):
        cur=conn.cursor();cur.execute('PRAGMA journal_mode=WAL');cur.execute('PRAGMA busy_timeout=30000');cur.close()
Session = sessionmaker(bind=engine, expire_on_commit=False)
def get_db():
    with Session() as db:
        yield db
