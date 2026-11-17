from sqlalchemy import Column,Integer,String,JSON,Text,ForeignKey
from src.config.database import Base
class ExactRun(Base):
    __tablename__='runs'
    id=Column(Integer,primary_key=True)
    study_id=Column(Integer,ForeignKey('studies.id'),nullable=False)
    name=Column(String(120),nullable=False)
    status=Column(String(20),default='queued')
    parameters=Column(JSON,nullable=False)
    snapshot=Column(JSON,nullable=False)
    checksum=Column(String(64),nullable=False)
    result=Column(JSON(none_as_null=True),nullable=True)
    error=Column(Text,default='')
    created_at=Column(String(40),nullable=False)
    # Lease + fencing: attempt is the fencing token. A worker may only publish
    # while it still owns (owner_id, attempt) and the run is running.
    attempt=Column(Integer,nullable=False,default=0,server_default='0')
    owner_id=Column(String(160),nullable=True)
    lease_expires_at=Column(String(40),nullable=True)
    heartbeat_at=Column(String(40),nullable=True)
    # Archived original: the artifact file is authoritative only when it
    # matches the database result (bytes, run id and input checksum).
    artifact_name=Column(String(200),nullable=True)
    artifact_sha256=Column(String(64),nullable=True)
    result_sha256=Column(String(64),nullable=True)
    integrity=Column(String(20),nullable=False,default='pending',server_default='pending')
    integrity_reason=Column(String(500),nullable=False,default='',server_default='')
    status_detail=Column(String(500),nullable=False,default='',server_default='')
Run=ExactRun
