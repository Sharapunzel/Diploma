"""Migration reversal is only exercised on the runner's disposable database."""

import os
import subprocess
import sys


def test_detector_upgrade_check_downgrade_upgrade():
    url = os.environ["TEST_DATABASE_URL"]
    assert "/diploma_test_review_" in url
    env = {**os.environ, "DATABASE_URL": url}
    env.pop("DATABASE_URL_FILE", None)
    for command in (["upgrade", "head"], ["check"], ["downgrade", "0017_capture_writer_guard"],
                    ["upgrade", "head"], ["check"]):
        subprocess.run([sys.executable, "-m", "alembic", *command], env=env, check=True)
