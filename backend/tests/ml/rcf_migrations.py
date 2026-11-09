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
    engine = create_engine(url)
    with engine.connect() as connection:
        before = connection.execute(text("SELECT id,checkpoint,active_attempt_id,consumed_index FROM logs.detectors ORDER BY id")).all()
        counts = connection.execute(text("SELECT (SELECT count(*) FROM logs.detector_warmups),(SELECT count(*) FROM logs.detector_outcomes),(SELECT count(*) FROM logs.findings)")).one()
    for command in (["downgrade", "0018_detector_core"], ["upgrade", "head"]):
        subprocess.run([sys.executable, "-m", "alembic", *command], env=env, check=True)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT id,checkpoint,active_attempt_id,consumed_index FROM logs.detectors ORDER BY id")).all() == before
        assert connection.execute(text("SELECT (SELECT count(*) FROM logs.detector_warmups),(SELECT count(*) FROM logs.detector_outcomes),(SELECT count(*) FROM logs.findings)")).one() == counts
        assert not connection.execute(text("SELECT 1 FROM logs.detectors WHERE execution_state NOT IN ('created','terminated')")).first()
    engine.dispose()
    for command in (["upgrade", "head"], ["check"], ["downgrade", "0017_capture_writer_guard"],
                    ["upgrade", "head"], ["check"]):
        subprocess.run([sys.executable, "-m", "alembic", *command], env=env, check=True)
