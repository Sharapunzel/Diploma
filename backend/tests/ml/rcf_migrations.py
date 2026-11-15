"""Migration reversal is only exercised on the runner's disposable database."""

import os
import subprocess
import sys

from sqlalchemy import create_engine, text


def test_detector_upgrade_check_downgrade_upgrade():
    url = os.environ["TEST_DATABASE_URL"]
    assert "/diploma_test_review_" in url
    env = {**os.environ, "DATABASE_URL": url}
    env.pop("DATABASE_URL_FILE", None)
    subprocess.run([sys.executable,"-B","-m","alembic","upgrade","head"],env=env,check=True)
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            before = connection.execute(text("SELECT id,checkpoint,active_attempt_id,consumed_index,execution_state,execution_generation FROM logs.detectors ORDER BY id")).all()
            counts = connection.execute(text("SELECT (SELECT count(*) FROM logs.detector_warmups),(SELECT count(*) FROM logs.detector_outcomes),(SELECT count(*) FROM logs.findings)")).one()
            aggregates = connection.scalar(text("SELECT count(*) FROM logs.aggregates"))
        if aggregates:
            refused=subprocess.run([sys.executable,"-B","-m","alembic","downgrade","0018_detector_core"],env=env,capture_output=True,text=True,check=False)
            assert refused.returncode!=0 and "explicit data-preserving migration" in refused.stderr
        else:
            for command in (["downgrade","0017_capture_writer_guard"],["upgrade","head"]):
                subprocess.run([sys.executable,"-B","-m","alembic",*command],env=env,check=True)
        with engine.connect() as connection:
            assert connection.execute(text("SELECT id,checkpoint,active_attempt_id,consumed_index,execution_state,execution_generation FROM logs.detectors ORDER BY id")).all()==before
            assert connection.execute(text("SELECT (SELECT count(*) FROM logs.detector_warmups),(SELECT count(*) FROM logs.detector_outcomes),(SELECT count(*) FROM logs.findings)")).one()==counts
            assert connection.scalar(text("SELECT version_num FROM alembic_version"))=="0021_event_time"
    finally:
        engine.dispose()
    subprocess.run([sys.executable,"-B","-m","alembic","check"],env=env,check=True)
