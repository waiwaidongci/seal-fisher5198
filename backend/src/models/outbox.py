from sqlalchemy import Column,Integer,String,UniqueConstraint
from src.config.database import Base
class Outbox(Base):
    """Durable registration for queue delivery (transactional outbox)."""
    __tablename__='outbox'
    __table_args__=(UniqueConstraint('run_id','attempt'),)
    id=Column(Integer,primary_key=True)
    run_id=Column(Integer,nullable=False,index=True)
    attempt=Column(Integer,nullable=False,default=0)
    created_at=Column(String(40),nullable=False)
    sent_at=Column(String(40),nullable=True)
    attempts=Column(Integer,nullable=False,default=0)
