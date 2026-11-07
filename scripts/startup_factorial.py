#!/usr/bin/env python3
"""Disposable Chrome startup factorial. No product imports, writes, or global cache deletion.

80 observations = 4 cache/DBus arms x 10 paired initial/reused launches, 8 workers.
Only the private cache is cold; system/ambient caches are measured, never purged.
The 12 s acceptance is recorded independently of the longer diagnostic deadline.
"""
import argparse
import concurrent.futures
import hashlib
import json
import math
import os
from pathlib import Path
import random
import resource
import shutil
import signal
import socket
import statistics
import subprocess
import threading
import time
import urllib.request
from xml.sax.saxutils import escape

PRODUCT_SHA = "997db4ce6c28b47cbe613f3eaa443193e02ad75a"
BROWSER_SHA256 = "760ae6e7b88145a56437378b777a9c434fea1e8da8404fe55d5d80447d56de9c"
SAFE_ENV = ("HOME", "XDG_CACHE_HOME", "XDG_CONFIG_HOME", "XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS", "FONTCONFIG_FILE", "FONTCONFIG_PATH", "DISPLAY", "TZ", "CI", "ImageOS", "ImageVersion", "RUNNER_ARCH", "RUNNER_OS", "ANYAGENTS_CHROMIUM_BIN", "RUST_TEST_THREADS")
ACTIVE = {}
ACTIVE_LOCK = threading.Lock()


def utc():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def read(path, limit=32768):
    try:
        return Path(path).read_text(errors="replace")[:limit]
    except OSError as error:
        return str(error)


def run(cmd, env=None, timeout=15):
    try:
        p = subprocess.run(cmd, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout, text=True)
        return {"command": cmd, "exit_code": p.returncode, "output": p.stdout[:65536]}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"command": cmd, "error": str(error)}


def path_info(path):
    p = Path(path)
    answer = {"path": str(p), "exists": p.exists(), "writable_access": os.access(p, os.W_OK)}
    try:
        s = p.stat()
        answer.update(uid=s.st_uid, gid=s.st_gid, mode=oct(s.st_mode & 0o7777), size=s.st_size)
        if p.is_dir():
            files = [f for f in p.glob("**/*") if f.is_file()]
            answer["file_count"] = len(files)
            answer["files"] = [{"path": str(f), "size": f.stat().st_size} for f in files[:100]]
    except OSError as error:
        answer["error"] = str(error)
    return answer


def resources():
    data = {"utc": utc(), "cpu_count": os.cpu_count(), "affinity": sorted(os.sched_getaffinity(0)), "loadavg": os.getloadavg(), "rlimit_nofile": resource.getrlimit(resource.RLIMIT_NOFILE), "rlimit_nproc": resource.getrlimit(resource.RLIMIT_NPROC)}
    for name in ("/proc/self/status", "/proc/self/cgroup", "/proc/meminfo", "/proc/pressure/cpu", "/proc/pressure/memory", "/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/cpuset.cpus.effective", "/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.events", "/sys/fs/cgroup/pids.max", "/sys/fs/cgroup/pids.current"):
        data[name] = read(name)
    # Also retain the actual process cgroup (root files can describe an ancestor).
    for line in read("/proc/self/cgroup").splitlines():
        if line.startswith("0::"):
            cgroup = Path("/sys/fs/cgroup") / line[3:].lstrip("/")
            for name in ("cpu.max", "cpu.stat", "memory.max", "memory.events", "pids.max"):
                data[str(cgroup / name)] = read(cgroup / name)
    return data


def process_snapshot(pid):
    root = Path(f"/proc/{pid}")
    data = {"pid": pid, "utc": utc(), "status": read(root / "status"), "stat": read(root / "stat"), "wchan": read(root / "wchan"), "threads": []}
    for task in sorted((root / "task").glob("*")):
        data["threads"].append({"tid": task.name, "comm": read(task / "comm"), "wchan": read(task / "wchan"), "children": read(task / "children")})
    return data


def stop(child):
    # Each child starts a new session; never match process names or touch foreign PIDs.
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        child.wait(timeout=1)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        child.wait(timeout=3)
    except subprocess.TimeoutExpired:
        pass
    with ACTIVE_LOCK:
        ACTIVE.pop(child.pid, None)
    return child.returncode


def interruption(signum, _frame):
    with ACTIVE_LOCK:
        children = list(ACTIVE.values())
    for child in children:
        stop(child)
    raise SystemExit(128 + signum)


def chromium_binary():
    custom = os.environ.get("ANYAGENTS_CHROMIUM_BIN", "").strip()
    if custom:
        return custom
    for candidate in ("/usr/bin/google-chrome-stable", "/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser"):
        if Path(candidate).is_file() and run([candidate, "--version"]).get("exit_code") == 0:
            return candidate
    return shutil.which("chromium") or "chromium"


def cache_paths(env):
    home = Path(env.get("HOME", str(Path.home())))
    xdg = Path(env.get("XDG_CACHE_HOME") or home / ".cache")
    return [path_info(p) for p in (Path("/var/cache/fontconfig"), xdg / "fontconfig", home / ".fontconfig")]


def make_pair(args, index, arm):
    pair = args.out / f"pair-{index:02d}-{arm}"
    pair.mkdir()
    env = os.environ.copy()
    env["TZ"] = "Asia/Hong_Kong"
    cache, dbus = arm.split("_")
    if dbus == "disabled":
        env["DBUS_SESSION_BUS_ADDRESS"] = "disabled:"
    # The current arm inherits the original value exactly, including absence.
    if cache == "writable":
        xdg = pair / "cache"
        fc = xdg / "fontconfig"
        fc.mkdir(parents=True)
        config = pair / "fonts.conf"
        config.write_text('<?xml version="1.0"?><!DOCTYPE fontconfig SYSTEM "fonts.dtd"><fontconfig><include ignore_missing="yes">/etc/fonts/fonts.conf</include><cachedir>' + escape(str(fc)) + '</cachedir></fontconfig>\n')
        env["XDG_CACHE_HOME"] = str(xdg)
        env["FONTCONFIG_FILE"] = str(config)
    return {"index": index, "arm": arm, "dir": pair, "env": env, "cache_initial": cache_paths(env), "fc_conflist": run(["fc-conflist"], env)}


def command_for(args, pair, case, port):
    version = args.chrome_version
    user_agent = f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36"
    flags = [f"--user-data-dir={case / 'user-data'}", f"--remote-debugging-port={port}", "--remote-debugging-address=127.0.0.1", "--remote-allow-origins=*", "--disable-blink-features=AutomationControlled", "--disable-infobars", "--no-first-run", "--no-default-browser-check", "--password-store=basic", "--use-mock-keychain", "--disable-background-timer-throttling", "--disable-backgrounding-occluded-windows", "--disable-renderer-backgrounding", "--disable-dev-shm-usage", "--no-sandbox", "--enable-webgl", "--ignore-gpu-blocklist", "--enable-unsafe-swiftshader", "--force-webrtc-ip-handling-policy=disable_non_proxied_udp", f"--user-agent={user_agent}", "--window-size=1920,1080", "--lang=zh-HK", "about:blank"]
    if args.mode == "auto" and pair["env"].get("DISPLAY"):
        return [args.chrome] + flags, "x11-display"
    if args.mode == "auto" and Path("/usr/bin/xvfb-run").is_file():
        return ["/usr/bin/xvfb-run", "-a", "-s", "-screen 0 1920x1080x24 -ac +extension GLX +render -noreset", args.chrome] + flags, "xvfb-headed"
    return [args.chrome, "--headless=new", "--hide-scrollbars", "--disable-gpu", "--no-zygote"] + flags, "headless-new"


def sample(args, pair, phase, barrier):
    case = pair["dir"] / phase
    case.mkdir()
    (case / "user-data").mkdir()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    command, mode = command_for(args, pair, case, port)
    record = {"utc": utc(), "pair": pair["index"], "arm": pair["arm"], "phase": phase, "cache_state": ("private_empty_global_ambient" if phase == "initial" else "private_browser_reused_global_ambient") if pair["arm"].startswith("writable") else "ambient_not_controlled_" + phase, "mode": mode, "command": command, "env": {key: pair["env"].get(key) for key in SAFE_ENV}, "port": port, "cache_before": cache_paths(pair["env"]), "attempts": 0, "ready_ms": None, "ready_at_12s": False, "acceptance_ms": args.acceptance * 1000, "diagnostic_deadline_ms": args.deadline * 1000}
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    log = open(case / "browser.log", "wb")
    barrier.wait(timeout=30)
    started = time.monotonic()
    child = subprocess.Popen(command, env=pair["env"], stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True, close_fds=True)
    with ACTIVE_LOCK:
        ACTIVE[child.pid] = child
    record["pid"] = child.pid
    record["spawn_ms"] = (time.monotonic() - started) * 1000
    try:
        while True:
            elapsed = time.monotonic() - started
            if elapsed >= args.acceptance and "at_12s" not in record:
                record["at_12s"] = {"elapsed_ms": elapsed * 1000, "poll": child.poll(), "process": process_snapshot(child.pid), "resources": resources()}
            if child.poll() is not None:
                record["reason"] = "child_exit_before_readiness"
                break
            remaining = args.deadline - elapsed
            if remaining <= 0:
                record["reason"] = "diagnostic_deadline"
                break
            record["attempts"] += 1
            try:
                with opener.open(f"http://127.0.0.1:{port}/json/version", timeout=min(0.25, remaining)) as response:
                    payload = response.read(16385)
                    if response.status != 200 or len(payload) > 16384:
                        raise ValueError("status/size invalid")
                    version = json.loads(payload)
                if not version.get("Browser") or not str(version.get("webSocketDebuggerUrl", "")).startswith(f"ws://127.0.0.1:{port}/devtools/browser/"):
                    raise ValueError("version JSON not ready")
                record["ready_ms"] = (time.monotonic() - started) * 1000
                record["ready_at_12s"] = record["ready_ms"] <= args.acceptance * 1000
                record["version_response"] = version
                record["reason"] = "ready"
                break
            except Exception as error:
                record["last_probe"] = f"{type(error).__name__}: {error}"
            time.sleep(min(0.08, max(0, args.deadline - (time.monotonic() - started))))
        record["observation_end_ms"] = (time.monotonic() - started) * 1000
        record["before_cleanup"] = child.poll()
        # Keep successful children alive until every launch in this wave has a result.
        barrier.wait(timeout=args.deadline + 30)
    finally:
        record["after_cleanup"] = stop(child)
        record["elapsed_including_cleanup_ms"] = (time.monotonic() - started) * 1000
        log.close()
        record["browser_log_tail"] = read(case / "browser.log", 65536)[-8192:]
        record["cache_after"] = cache_paths(pair["env"])
        (case / "record.json").write_text(json.dumps(record, indent=2) + "\n")
    return record


def summarize(records):
    result = {}
    for arm in sorted({r["arm"] for r in records}):
        selected = [r for r in records if r["arm"] == arm]
        ready = sorted(r["ready_ms"] for r in selected if r["ready_ms"] is not None)
        count = len(selected)
        result[arm] = {"n": count, "ready_by_12s": sum(r["ready_at_12s"] for r in selected), "ready_by_deadline": len(ready), "failures_12s": count - sum(r["ready_at_12s"] for r in selected), "ready_ms_min": min(ready) if ready else None, "ready_ms_median": statistics.median(ready) if ready else None, "ready_ms_p95_nearest_rank": ready[math.ceil(0.95 * len(ready)) - 1] if ready else None, "ready_ms_max": max(ready) if ready else None, "phase": {phase: {"n": sum(r["phase"] == phase for r in selected), "ready_by_12s": sum(r["phase"] == phase and r["ready_at_12s"] for r in selected), "cache_files_after": [r["cache_after"] for r in selected if r["phase"] == phase]} for phase in ("initial", "reused")}}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--pairs-per-arm", type=int, default=10)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--cpu-count", type=int, default=1, help="Pin driver and all its children to first N allowed CPUs; 0 preserves affinity")
    parser.add_argument("--acceptance", type=float, default=12)
    parser.add_argument("--deadline", type=float, default=30)
    parser.add_argument("--mode", choices=("headless", "auto"), default="headless", help="headless matches all four failed tests; auto matches production headed default")
    parser.add_argument("--expected-image", default="20261101.360.1")
    parser.add_argument("--allow-image-mismatch", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    args = parser.parse_args()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    before = resources()
    if args.cpu_count:
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:args.cpu_count])
    metadata = {"started_utc": utc(), "product_sha": PRODUCT_SHA, "browser_source_sha256": BROWSER_SHA256, "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "args": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()}, "uid": os.getuid(), "gid": os.getgid(), "env": {key: os.environ.get(key) for key in SAFE_ENV}, "resources_before_affinity": before, "resources_after_affinity": resources(), "os_release": read("/etc/os-release"), "uname": run(["uname", "-a"]), "fontconfig_version": run(["fc-cache", "--version"]), "fontconfig_config": run(["fc-conflist"]), "cache_paths": cache_paths(os.environ), "fontconfig_config_files": {str(p): read(p) for p in Path("/etc/fonts").glob("fonts.conf")}, "cold_scope": "Only new private XDG/fontconfig cache roots start empty. System and baseline caches are neither purged nor claimed cold. Reused phase has a fresh browser profile and the previous phase's cache; warm state is established by file inventories."}
    (args.out / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    if args.plan_only:
        print(json.dumps({"plan_only": True, "metadata": str(args.out / "metadata.json"), "cases": args.pairs_per_arm * 8}))
        return
    if not args.allow_image_mismatch and os.environ.get("ImageVersion") != args.expected_image:
        raise SystemExit(f"ImageVersion mismatch: {os.environ.get('ImageVersion')!r} != {args.expected_image!r}; metadata retained; no browser launched")
    if args.deadline < args.acceptance or args.workers < 1 or args.pairs_per_arm < 1:
        raise SystemExit("invalid bounds")
    args.chrome = chromium_binary()
    version = run([args.chrome, "--version"])
    metadata["chrome"] = {"selected": args.chrome, "realpath": os.path.realpath(args.chrome), "version": version, "file": path_info(args.chrome)}
    tokens = version.get("output", "").split()
    args.chrome_version = next((v for v in tokens if v[:1].isdigit() and "." in v), "136.0.7103.92")
    (args.out / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    arms = ["current_current", "writable_current", "current_disabled", "writable_disabled"]
    randomizer = random.Random(115)
    pairs = []
    # Each eight-launch wave has two pairs from every arm, avoiding arm/time confounding.
    for _ in range(args.pairs_per_arm):
        block = arms.copy()
        randomizer.shuffle(block)
        for arm in block:
            pairs.append(make_pair(args, len(pairs), arm))
    records = []
    for wave_start in range(0, len(pairs), args.workers):
        wave = pairs[wave_start:wave_start + args.workers]
        for phase in ("initial", "reused"):
            barrier = threading.Barrier(len(wave))
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(wave)) as pool:
                jobs = [pool.submit(sample, args, pair, phase, barrier) for pair in wave]
                for job in jobs:
                    record = job.result()
                    records.append(record)
                    with open(args.out / "records.jsonl", "a") as stream:
                        stream.write(json.dumps(record) + "\n")
                    print(json.dumps({key: record[key] for key in ("utc", "pair", "arm", "phase", "ready_ms", "ready_at_12s", "reason")}), flush=True)
    for pair in pairs:
        # Diagnostics can warm shared ambient caches, so defer them until ALL
        # measured launches finish, not merely this pair or this wave.
        diagnostic = {"fc_match": run(["fc-match", "-v"], pair["env"]), "fc_cache": run(["fc-cache", "-v"], pair["env"]), "cache_after_diagnostics": cache_paths(pair["env"])}
        (pair["dir"] / "fontconfig-after.json").write_text(json.dumps(diagnostic, indent=2) + "\n")
    summary = {"finished_utc": utc(), "observations": len(records), "arms": summarize(records), "resources_end": resources(), "owned_active_children": list(ACTIVE), "interpretation": "Standalone measured startup only, not a Rust suite reproduction. No claimed global-cold state. Success latency distributions exclude nonready right-censored cases; inspect failures_12s and per-case records."}
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({"summary": str(args.out / "summary.json"), "observations": len(records), "active_owned_children": len(ACTIVE)}), flush=True)


if __name__ == "__main__":
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, interruption)
    main()
