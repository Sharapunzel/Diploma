"""Permanent selector, fixture and early-failure checks for smoke browser mode."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from scripts import browser_test_scenarios as browser


def arguments(**changes):
    return SimpleNamespace(**{"profile":"smoke-development", "browser_only":True, "browser_spec":None,
        "detector_only":False,"aggregate_only":False,"rcf":False,"wal":False,"pytest":None, **changes})


def test_mode_selection_is_explicit_and_includes_all_standard_families():
    dev = browser.validate_browser_arguments(arguments())
    assert {"auth.spec.ts","events.spec.ts","resources.spec.ts","normalizers.spec.ts",
            "aggregates.live.spec.ts","aggregates.ui.spec.ts","detectors.ui.spec.ts","detectors.live.spec.ts"} <= set(dev)
    for family in ("resources", "normalizers"):
        assert {p.name for p in (browser.ROOT / "frontend/browser").glob(f"{family}.*.spec.ts")} <= set(dev)
    assert browser.validate_browser_arguments(arguments(profile="smoke-production")) == list(browser.PRODUCTION)
    assert "aggregates.live.spec.ts" in browser.PRODUCTION
    assert "external.live.spec.ts" not in dev
    assert "agents.tls.spec.ts" not in dev


@pytest.mark.parametrize("changes", [{"browser_only":False,"browser_spec":["auth.spec.ts"]},
    {"profile":"frontend"},{"detector_only":True},{"aggregate_only":True},{"rcf":True},{"wal":True},{"pytest":[]},
    {"browser_spec":["../auth.spec.ts"]},{"browser_spec":["external.live.spec.ts"]},
    {"profile":"smoke-production","browser_spec":["auth.spec.ts"]},{"browser_spec":[]}])
def test_incompatible_selection_fails_before_preparation(changes):
    with pytest.raises(ValueError):
        browser.validate_browser_arguments(arguments(**changes))


def test_required_fixture_is_error_not_skip(tmp_path):
    path = tmp_path / "fixture.json"
    with pytest.raises(FileNotFoundError):
        browser.require_fixture(path,["aggregate"])
    path.write_text('{"aggregate":null}',encoding="utf-8")
    with pytest.raises(RuntimeError):
        browser.require_fixture(path,["aggregate"])


def test_history_readiness_requires_recent_complete_working_windows():
    points=[{"kind":"working","end_utc":f"{i:03}","completeness":"complete"} for i in range(16)]
    assert browser.ready_history(points)
    assert not browser.ready_history(points[:15])
    assert not browser.ready_history([*points,{"kind":"working","end_utc":"017","completeness":"unknown"}])
    assert not browser.ready_history([{**p,"kind":"historical"} for p in points])


def test_hmr_verification_preserves_unrelated_changes(tmp_path):
    css = tmp_path / "styles.css"
    original = b"--color-page: #eef5f2;\n/* user's existing style */\n"
    css.write_bytes(original)
    browser.verify_hmr_restore(css, original)
    css.write_bytes(original.replace(b"#eef5f2", b"#e4f7e4"))
    with pytest.raises(RuntimeError, match="HMR"):
        browser.verify_hmr_restore(css, original)
    assert css.read_bytes() == original
    foreign = original + b"/* concurrent user edit */\n"
    css.write_bytes(foreign)
    with pytest.raises(RuntimeError, match="preserved"):
        browser.verify_hmr_restore(css, original)
    assert css.read_bytes() == foreign


def test_missing_mandatory_spec_is_not_silently_omitted(tmp_path,monkeypatch):
    monkeypatch.setattr(browser,"ROOT",tmp_path)
    with pytest.raises(ValueError,match="Missing mandatory"):
        browser.browser_specs(True)


def test_production_browser_container_removed_after_failed_run(tmp_path):
    source = tmp_path / "frontend"
    (source / "browser").mkdir(parents=True)
    for name in ("package.json","package-lock.json","playwright.config.ts","browser/detectors.live.spec.ts"):
        (source / name).write_text("{}",encoding="utf-8")
    fixture = tmp_path / "fixture.json"
    fixture.write_text("{}",encoding="utf-8")
    command = Mock(side_effect=["proxy-id", "browser-id", "", RuntimeError("browser failed")])
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(browser,"ROOT",tmp_path)
        cleanup = Mock()
        patch.setattr(browser.subprocess,"run",cleanup)
        with pytest.raises(RuntimeError,match="browser failed"):
            browser.production_browsers([], {"DETECTOR_BROWSER_CONFIG":str(fixture),"KAFKA_CA_FILE":str(fixture),"TASK21_ADMIN_PASSWORD":"synthetic","HTTPS_BIND":"127.0.0.1:55443:443","PUBLIC_ORIGIN":"https://localhost:55443"}, "owned",tmp_path,["detectors.live.spec.ts"],command)
        assert cleanup.call_args.args[0] == ["docker","rm","-f","owned-detector-browser"]
        browser_command = command.call_args_list[1].args[0]
        assert "PLAYWRIGHT_BASE_URL=https://localhost:55443" in browser_command
        assert "NODE_EXTRA_CA_CERTS=/tmp/proxy-ca.pem" in browser_command
        assert not any("NODE_TLS_REJECT_UNAUTHORIZED" in arg for arg in browser_command)
        test_script = command.call_args.args[0][-1]
        assert "TCP-LISTEN:55443" in test_script
        assert "TCP:127.0.0.1:443" in test_script
        assert "ignoreHTTPSErrors" not in test_script


def test_browser_modules_copy_local_dependency_tree_and_reject_escape(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "one.spec.ts").write_text('import { helper } from "./nested/helper";', encoding="utf-8")
    (source / "nested").mkdir()
    (source / "nested/helper.ts").write_text('export { value } from "../value";', encoding="utf-8")
    (source / "value.ts").write_text("export const value = 1;", encoding="utf-8")
    destination = tmp_path / "dest"
    browser.copy_browser_modules(source, destination, ["one.spec.ts"])
    assert (destination / "nested/helper.ts").read_bytes() == (source / "nested/helper.ts").read_bytes()
    assert (destination / "value.ts").is_file()
    (source / "bad.spec.ts").write_text('import "../outside";', encoding="utf-8")
    (tmp_path / "outside.ts").write_text("", encoding="utf-8")
    with pytest.raises(RuntimeError, match="unsafe"):
        browser.copy_browser_modules(source, destination, ["bad.spec.ts"])


@pytest.mark.parametrize("failed", [False, True])
def test_production_aggregate_config_paths_ca_export_and_cleanup(tmp_path, monkeypatch, failed):
    import json
    source = tmp_path / "frontend"
    (source / "browser").mkdir(parents=True)
    for name in ("package.json", "package-lock.json", "playwright.config.ts", "browser/aggregates.live.spec.ts"):
        (source / name).write_text("{}", encoding="utf-8")
    detector = tmp_path / "detector.json"
    detector.write_text("{}", encoding="utf-8")
    aggregate = tmp_path / "aggregate.json"
    exported = tmp_path / "owned.json"
    aggregate.write_text(json.dumps({"username":"test", "password":"synthetic", "sources":["one","two"], "anchor":"2026-01-01T00:00:00Z", "result":str(exported)}), encoding="utf-8")
    calls = []
    def command(args, **kwargs):
        calls.append(args)
        if args[:2] == ["docker", "exec"] and "npx playwright test" in args[-1]:
            (tmp_path / "browser-results/aggregate-browser-owned.json").write_text('["owned-id"]', encoding="utf-8")
            if failed:
                raise RuntimeError("browser failed")
        return "proxy-id" if "ps" in args else ""
    cleanup = Mock()
    monkeypatch.setattr(browser,"ROOT",tmp_path)
    monkeypatch.setattr(browser.subprocess,"run",cleanup)
    env = {"DETECTOR_BROWSER_CONFIG":str(detector), "AGGREGATE_BROWSER_CONFIG":str(aggregate), "KAFKA_CA_FILE":str(detector), "TASK21_ADMIN_PASSWORD":"synthetic", "HTTPS_BIND":"127.0.0.1:55443:443", "PUBLIC_ORIGIN":"https://localhost:55443"}
    if failed:
        with pytest.raises(RuntimeError, match="browser failed"):
            browser.production_browsers([],env,"owned",tmp_path,["aggregates.live.spec.ts"],command)
    else:
        browser.production_browsers([],env,"owned",tmp_path,["aggregates.live.spec.ts"],command)
    config = json.loads((tmp_path / "detector-browser-context/aggregate-fixture.json").read_text(encoding="utf-8"))
    assert config["result"] == "/results/aggregate-browser-owned.json"
    run = next(args for args in calls if args[:2] == ["docker","run"])
    assert "AGGREGATE_BROWSER_CONFIG=/test/aggregate-fixture.json" in run
    assert "DETECTOR_BROWSER_CONFIG=/test/fixture.json" in run
    assert "NODE_EXTRA_CA_CERTS=/tmp/proxy-ca.pem" in run
    assert exported.read_text(encoding="utf-8") == '["owned-id"]'
    assert cleanup.call_args.args[0] == ["docker","rm","-f","owned-detector-browser"]


@pytest.mark.parametrize("failed", [False, True])
def test_browser_working_gate_releases_its_transaction_on_every_exit(failed):
    from unittest.mock import MagicMock

    from scripts.browser_detector_fixture import working_closure_gate
    database = MagicMock()
    transaction = database.session_factory.begin.return_value
    if failed:
        with pytest.raises(RuntimeError, match="proof failed"), working_closure_gate(database):
            raise RuntimeError("proof failed")
        assert transaction.__exit__.call_args.args[0] is RuntimeError
    else:
        with working_closure_gate(database) as proof:
            assert proof is transaction.__enter__.return_value
        assert transaction.__exit__.call_args.args == (None, None, None)
    statements = [str(call.args[0]) for call in transaction.__enter__.return_value.execute.call_args_list]
    assert any("lock_timeout" in statement for statement in statements)
    assert any("statement_timeout" in statement for statement in statements)
