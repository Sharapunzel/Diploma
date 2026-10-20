"""Keep harness pytest artifacts inside the isolated run directory."""

import importlib.util
import shlex
from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest

PROFILE_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "agent_test_profiles.py"
SPEC = importlib.util.spec_from_file_location("agent_test_profiles", PROFILE_SCRIPT)
assert SPEC is not None and SPEC.loader is not None
PROFILE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROFILE)


@pytest.mark.parametrize(
    "run",
    [
        PureWindowsPath(r"C:\Users\Lenovo\Projects\Diploma\.harness\runs\run with spaces"),
        PurePosixPath("/workspace/project/.harness/runs/run with spaces"),
    ],
)
def test_pytest_basetemp_keeps_absolute_run_path(run):
    args = shlex.split(PROFILE.pytest_addopts(run))

    assert args == [
        "-p",
        "no:cacheprovider",
        f"--basetemp={(run / 'pytest').as_posix()}",
    ]
