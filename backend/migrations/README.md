Schema history
==============

Schema v1 (original): users, studies, observations, runs, reviews, audits.
Created at startup via ``Base.metadata.create_all``. No destructive migration
is applied; back up ``db-data`` before changing schemas.

Schema v2 (concurrent calculation and archive verification)
------------------------------------------------------------

``create_all`` adds new tables but does **not** alter existing ones, so a
database created with v1 needs the following statements once before starting
the new code. They are additive and safe to re-run (MySQL 8 checks columns
explicitly; the ``outbox`` table is created by ``create_all``).

.. code-block:: sql

   ALTER TABLE runs
     ADD COLUMN attempt INT NOT NULL DEFAULT 0,
     ADD COLUMN owner_id VARCHAR(160) NULL,
     ADD COLUMN lease_expires_at VARCHAR(40) NULL,
     ADD COLUMN heartbeat_at VARCHAR(40) NULL,
     ADD COLUMN artifact_name VARCHAR(200) NULL,
     ADD COLUMN artifact_sha256 VARCHAR(64) NULL,
     ADD COLUMN result_sha256 VARCHAR(64) NULL,
     ADD COLUMN integrity VARCHAR(20) NOT NULL DEFAULT 'pending',
     ADD COLUMN integrity_reason VARCHAR(500) NOT NULL DEFAULT '',
     ADD COLUMN status_detail VARCHAR(500) NOT NULL DEFAULT '';

Completed runs from v1 have no ``result_sha256`` and therefore show as
"database result has no registered digest" with view/approval/download
closed. This is deliberate: the archive cannot be proven consistent with the
database. A fresh ``docker compose down -v`` demo dataset, or re-running the
study under a new run number, restores normal operation. Approved v1 runs are
never recomputed automatically.
