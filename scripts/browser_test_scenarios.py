"""Browser selection and preparation within the existing smoke Compose lifecycle."""
import json
import os
import secrets
import subprocess
import time
from pathlib import Path

if __package__:
    from .browser_profile_health import (
        check_fixture,
        monitored_command,
        wait_current_history,
    )
else:
    from browser_profile_health import (
        check_fixture,
        monitored_command,
        wait_current_history,
    )

ROOT = Path(__file__).resolve().parents[1]
PRODUCTION = ("detectors.live.spec.ts", "proxy.session.spec.ts")


def browser_specs(production, requested=None):
    directory = ROOT / "frontend/browser"
    development = ["auth.spec.ts", "events.spec.ts", "resources.spec.ts", "normalizers.spec.ts",
                   "aggregates.live.spec.ts", "aggregates.ui.spec.ts", "detectors.ui.spec.ts", "detectors.live.spec.ts"]
    development += sorted(p.name for p in directory.glob("resources.*.spec.ts"))
    development += sorted(p.name for p in directory.glob("normalizers.*.spec.ts"))
    allowed = list(PRODUCTION) if production else development
    for name in allowed:
        if not (directory / name).is_file():
            raise ValueError(f"Missing mandatory browser spec: {name}")
    selected = requested if requested is not None else allowed
    if not selected or len(set(selected)) != len(selected) or any(name not in allowed for name in selected):
        raise ValueError("Browser specs must belong to the selected smoke mode without duplicates")
    return selected


def validate_browser_arguments(args):
    if args.browser_spec is not None and not args.browser_only:
        raise ValueError("--browser-spec requires --browser-only")
    if not args.browser_only:
        return None
    if args.profile not in {"smoke-development", "smoke-production"}:
        raise ValueError("--browser-only requires a smoke profile")
    if args.detector_only or args.aggregate_only or args.rcf or args.wal or args.pytest is not None:
        raise ValueError("--browser-only cannot combine with detector/aggregate/rcf/wal/pytest selection")
    return browser_specs(args.profile == "smoke-production", args.browser_spec)


def require_fixture(path, fields):
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or any(not data.get(field) for field in fields):
        raise RuntimeError(f"Required browser fixture fields missing: {fields}")
    return data


def ready_history(items, *, not_before=None):
    working = sorted((p for p in items if p["kind"] == "working"), key=lambda p:p["end_utc"], reverse=True)[:16]
    if len(working) != 16 or any(p["completeness"] != "complete" for p in working):
        return False
    if not_before is not None:
        from datetime import datetime
        return datetime.fromisoformat(working[0]["end_utc"].replace("Z", "+00:00")) >= not_before
    return True


def verify_hmr_restore(css, original):
    current = css.read_bytes()
    if current == original:
        return
    own_change = original.replace(b"--color-page: #eef5f2;", b"--color-page: #e4f7e4;", 1)
    if own_change != original and current == own_change:
        css.write_bytes(original)
    # Preserve any unrelated bytes, including concurrent user changes.
    raise RuntimeError("HMR did not restore the original stylesheet bytes; unrelated changes preserved")


def run_browsers(compose, env, project, run, production, selected, command, oidc_setup):
    from aggregate_test_scenarios import aggregate_runtime_smoke
    print(f"Browser mode={'production' if production else 'development'}; specs={len(selected)}: {', '.join(selected)}", flush=True)
    npm = "npm.cmd" if os.name == "nt" else "npm"
    username, guest, password = "task21admin", "detector-browser-guest", secrets.token_urlsafe(24)
    config = {"username":username,"guest":guest,"password":password}
    aggregate_config = None
    if any(name.startswith("aggregates.") for name in selected):
        aggregate_config = aggregate_runtime_smoke(compose, env, production, command, prepare_only=True)
    need_detectors = any(name.startswith("detectors.") for name in selected)
    if not need_detectors:
        create = "import os,app.cli as cli; cli.getpass.getpass=lambda prompt: os.environ['BROWSER_PASSWORD']; raise SystemExit(cli.create_admin('task21admin','Browser admin'))"
        command([*compose,"exec","-T","-e",f"BROWSER_PASSWORD={password}","api","python","-c",create],env=env)
    if need_detectors:
        command([*compose, "stop", "aggregate-worker"], env=env)
        command([*compose, "cp", str(ROOT / "scripts/browser_detector_fixture.py"), "detector-worker:/tmp/browser_detector_fixture.py"], env=env)
        command([*compose, "exec", "-d", "-e", f"BROWSER_USERNAME={username}", "-e", f"BROWSER_GUEST={guest}", "-e", f"BROWSER_PASSWORD={password}", "detector-worker", "sh", "-c", "python /tmp/browser_detector_fixture.py >/tmp/browser-detector-fixture.log 2>&1"], env=env)
        deadline = time.monotonic() + 60
        startup_deadline = time.monotonic() + 5
        while True:
            result = subprocess.run([*compose, "exec", "-T", "detector-worker", "cat", "/tmp/detector-browser-fixture.json"], env=env, text=True, capture_output=True, check=False, timeout=5)
            if result.returncode == 0:
                config.update(json.loads(result.stdout))
                break
            check_fixture(compose, env, command, allow_starting=time.monotonic() < startup_deadline,
                          allow_capture_starting=True)
            if time.monotonic() >= deadline:
                raise RuntimeError("Detector browser history preparation exceeded its profile budget")
            time.sleep(.5)
        command([*compose, "up", "-d", "--no-deps", "--wait", "aggregate-worker"], env=env)
    fixture = run / "detector-browser.json"
    fixture.write_text(json.dumps(config), encoding="utf-8")
    if need_detectors:
        require_fixture(fixture, ["username", "guest", "password", "aggregate", "source", "anchor"])
    if aggregate_config:
        aggregate_file = run / "aggregate-browser.json"
        aggregate_file.write_text(json.dumps({**aggregate_config,"result":str(run / "aggregate-browser-owned.json")}), encoding="utf-8")
    browser_env = {**env, "DETECTOR_BROWSER_CONFIG":str(fixture), "BROWSER_FIXTURES_REQUIRED":"1", "PLAYWRIGHT_BASE_URL":env["PUBLIC_ORIGIN"], "PLAYWRIGHT_USERNAME":username, "PLAYWRIGHT_PASSWORD":password, "TASK21_ADMIN_PASSWORD":password, "TASK21_SECURE_COOKIE":"true" if production else "false"}
    if aggregate_config:
        browser_env["AGGREGATE_BROWSER_CONFIG"] = str(aggregate_file)
    css = ROOT / "frontend/src/app/styles.css"
    original_css = css.read_bytes()
    check = (lambda: check_fixture(compose, env, command)) if need_detectors else None
    ready = (lambda timeout=60: wait_current_history(env, config, check, ready_history, timeout=timeout)) if need_detectors else None
    try:
        if not production:
            for candidate in (r"C:\Program Files\Google\Chrome\Application\chrome.exe", r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"):
                if Path(candidate).is_file():
                    browser_env["PLAYWRIGHT_CHROME_PATH"] = candidate
                    break
            if "auth.spec.ts" in selected:
                oidc_setup(compose, env, False, keep_active=True)
                browser_env["PLAYWRIGHT_OIDC"] = "1"
            if ready:
                ready()
            execute = monitored_command if check else command
            execute([npm,"run","test:browser","--",*selected,"--workers=1","--output",str(run / "browser-results")], env=browser_env, cwd=ROOT / "frontend", timeout=1200, **({"check":check} if check else {}))
        else:
            production_browsers(compose, browser_env, project, run, selected, command, check=check, ready=ready)
    finally:
        if need_detectors:
            try:
                command([*compose, "cp", str(ROOT / "scripts/browser_history_diagnostics.py"),
                         "detector-worker:/tmp/browser_history_diagnostics.py"], env=env)
                facts = command([*compose, "exec", "-T", "detector-worker", "python",
                                 "/tmp/browser_history_diagnostics.py"], env=env, capture=True, timeout=15)
                print("Detector browser read-only diagnostics: " + facts,flush=True)
            except RuntimeError:
                print("Detector browser diagnostic snapshot unavailable",flush=True)
        # auth.spec's own finally restores its exact input; detect any failure.
        verify_hmr_restore(css, original_css)


def production_browsers(compose, env, project, run, selected, command, *, check=None, ready=None):
    deadline = time.monotonic() + 1200
    context = run / "detector-browser-context"
    context.mkdir()
    source = ROOT / "frontend"
    for name in ("package.json", "package-lock.json", "playwright.config.ts"):
        (context / name).write_bytes((source / name).read_bytes())
    (context / "browser").mkdir()
    for name in selected:
        (context / "browser" / name).write_bytes((source / "browser" / name).read_bytes())
    (context / "fixture.json").write_bytes(Path(env["DETECTOR_BROWSER_CONFIG"]).read_bytes())
    results = run / "browser-results"
    results.mkdir()
    proxy = command([*compose,"ps","-q","proxy"], env=env, capture=True).strip()
    if not proxy:
        raise RuntimeError("Production browser proxy is unavailable")
    container = project + "-detector-browser"
    public_port = env["HTTPS_BIND"].split(":")[1]
    prepare = "set -e; apt-get update >/dev/null; apt-get install -y --no-install-recommends libnss3-tools socat >/dev/null; npm ci --ignore-scripts --no-audit --no-fund; mkdir -p /root/.pki/nssdb; certutil -d sql:/root/.pki/nssdb -N --empty-password; certutil -d sql:/root/.pki/nssdb -A -t 'C,,' -n proxy-test -i /tmp/proxy-ca.pem; cp /tmp/proxy-ca.pem /usr/local/share/ca-certificates/proxy-test.crt; update-ca-certificates >/dev/null"
    script = f"socat TCP-LISTEN:{public_port},bind=127.0.0.1,reuseaddr,fork TCP:127.0.0.1:443 & npx playwright test " + " ".join("browser/"+name for name in selected) + " --workers=1 --output /results"
    try:
        command(["docker","run","-d","--rm","--name",container,"--user","0:0","--memory","2g","--cpus","2","--network",f"container:{proxy}","--label",f"com.docker.compose.project={project}","-w","/test","-v",f"{context.resolve()}:/test","-v",f"{results.resolve()}:/results","-v",f"{Path(env['KAFKA_CA_FILE']).resolve()}:/tmp/proxy-ca.pem:ro","-e","NODE_EXTRA_CA_CERTS=/tmp/proxy-ca.pem","-e","DETECTOR_BROWSER_CONFIG=/test/fixture.json","-e","BROWSER_FIXTURES_REQUIRED=1","-e",f"PLAYWRIGHT_BASE_URL={env['PUBLIC_ORIGIN']}","-e","TASK21_SECURE_COOKIE=true","-e",f"TASK21_ADMIN_PASSWORD={env['TASK21_ADMIN_PASSWORD']}","mcr.microsoft.com/playwright:v1.63.0-noble","sleep","1800"], env=env, timeout=30)
        def execute(script):
            args = ["docker", "exec", container, "sh", "-c", script]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("Production browser preparation/test budget exhausted")
            if check:
                monitored_command(args, env=env, cwd=ROOT, timeout=remaining, check=check)
            else:
                command(args, env=env, timeout=remaining)
        execute(prepare)
        if ready:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("Production browser preparation/test budget exhausted")
            ready(timeout=min(60, remaining))
        execute(script)
    finally:
        subprocess.run(["docker","rm","-f",container],env=env,check=False,capture_output=True)
