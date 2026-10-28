#!/usr/bin/env python3
"""Measure the five JSON handlers with wrk and save a page-readable report."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from urllib.request import urlopen


RUNTIMES = ("rust", "lua", "deno", "php", "python")
SUMMARY = r'''
done = function(summary, latency, requests)
  io.write(string.format(
    '\nPENTAGLOT_RESULT {"duration_us":%.0f,"requests":%.0f,' ..
    '"p99_us":%.0f,"errors":{"connect":%.0f,"read":%.0f,' ..
    '"write":%.0f,"status":%.0f,"timeout":%.0f}}\n',
    summary.duration, summary.requests, latency:percentile(99),
    summary.errors.connect, summary.errors.read, summary.errors.write,
    summary.errors.status, summary.errors.timeout))
end
'''


def server_pids(root_pid, workers):
    if workers is None:
        return {root_pid}
    listing = subprocess.check_output(["ps", "-axo", "pid=,ppid="], text=True)
    children = {int(pid) for pid, parent in (line.split() for line in listing.splitlines())
                if int(parent) == root_pid}
    if len(children) != workers:
        raise RuntimeError(f"expected {workers} workers below the master; found {len(children)}")
    return {root_pid, *children}


def process_identity(pid):
    """Use the kernel birth stamp to distinguish reused Linux PIDs."""
    if platform.system() == "Linux":
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return stat[19]
    return subprocess.check_output(
        ["ps", "-p", str(pid), "-o", "lstart="], text=True
    ).strip()


def process_usage(pids, root_pid, workers):
    if platform.system() == "Linux":
        clock_ticks = os.sysconf("SC_CLK_TCK")
        page_bytes = os.sysconf("SC_PAGE_SIZE")
        samples = []
        for pid in sorted(pids):
            stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            if stat[0] in ("Z", "X"):
                raise RuntimeError("a measured process exited during the benchmark")
            samples.append({
                "pid": pid, "start_ticks": int(stat[19]),
                "rss_bytes": int(stat[21]) * page_bytes,
                "cpu_seconds": (int(stat[11]) + int(stat[12])) / clock_ticks,
                "role": "standalone" if workers is None else ("master" if pid == root_pid else "worker"),
            })
        return samples
    output = subprocess.check_output(
        ["ps", "-p", ",".join(map(str, sorted(pids))), "-o", "pid=,rss=,time="], text=True
    )
    samples = []
    for line in output.splitlines():
        pid, rss, cpu = line.split()
        days, cpu = cpu.split("-", 1) if "-" in cpu else ("0", cpu)
        seconds = int(days) * 86400 + sum(
            float(value) * 60**index for index, value in enumerate(reversed(cpu.split(":")))
        )
        samples.append({
            "pid": int(pid), "rss_bytes": int(rss) * 1024, "cpu_seconds": seconds,
            "role": "standalone" if workers is None else ("master" if int(pid) == root_pid else "worker"),
        })
    if {sample["pid"] for sample in samples} != pids:
        raise RuntimeError("a measured process exited during the benchmark")
    return samples


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="Running isolated Pentaglot URL")
    parser.add_argument("--connections", type=int, required=True)
    parser.add_argument("--threads", type=int, required=True)
    parser.add_argument("--duration", required=True, help="wrk duration, such as 1m")
    parser.add_argument("--runtime", action="append", choices=RUNTIMES, help="Measure only selected runtime(s); default: all five")
    parser.add_argument("--binary", type=Path, required=True, help="Serving release binary")
    parser.add_argument("--expected-master-pid", type=int, help="Reject responses from a different master process")
    parser.add_argument("--native", action="store_true", help="Record a target-cpu=native release build")
    parser.add_argument("--notes", help="Preparation and runtime settings used for this measurement")
    parser.add_argument("--idle-sample-seconds", type=int, nargs="+", default=[],
                        help="Sample the same live processes at these seconds after load (for example 10 30 60 120)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--standalone", action="store_true", help="Measure one serving process (default)")
    mode.add_argument("--workers", type=int, help="Measure the master and this many serving workers")
    parser.add_argument("--label", required=True, help="Public hardware and OS description")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--raw-dir", type=Path, required=True, help="Local wrk output directory")
    args = parser.parse_args()
    selected = args.runtime or list(RUNTIMES)
    if len(set(selected)) != len(selected):
        parser.error("select each runtime at most once")
    if args.threads < 1 or args.connections < args.threads:
        parser.error("connections must be at least threads, and threads must be positive")
    if args.connections % args.threads:
        parser.error("connections must be divisible by threads; wrk otherwise rounds the total down")
    if args.workers is not None and not 1 <= args.workers <= 255:
        parser.error("workers must be between 1 and 255; use --standalone for one serving process")
    if any(seconds <= 0 for seconds in args.idle_sample_seconds) or args.idle_sample_seconds != sorted(set(args.idle_sample_seconds)):
        parser.error("idle sample times must be positive, distinct and increasing")
    if not re.fullmatch(r"[1-9][0-9]*(?:ms|s|m|h)", args.duration):
        parser.error("duration must be a positive integer followed by ms, s, m or h")
    wrk = shutil.which("wrk")
    if wrk is None:
        parser.error("wrk is required")
    if not args.binary.is_file():
        parser.error("the serving release binary must exist")

    base = args.url.rstrip("/")
    process_ids = set()
    for runtime in RUNTIMES:
        with urlopen(f"{base}/{runtime}") as response:
            value = json.load(response)
            if response.status != 200 or value.get("runtime") != runtime:
                raise RuntimeError(f"{runtime}: unexpected preflight response")
            process_ids.add(response.headers.get("x-pentaglot-pid"))
    if None in process_ids:
        raise RuntimeError("the server must identify its Edge PID in every preflight response")
    observed_pids = {int(pid) for pid in process_ids}
    if args.workers is None:
        if len(observed_pids) != 1:
            raise RuntimeError("standalone mode requires one serving process for all five runtimes")
        root_pid = next(iter(observed_pids))
    else:
        parents = subprocess.check_output(
            ["ps", "-p", ",".join(map(str, sorted(observed_pids))), "-o", "ppid="], text=True
        ).split()
        if len(set(parents)) != 1 or int(parents[0]) <= 1:
            raise RuntimeError("worker responses must belong to one running master")
        root_pid = int(parents[0])
    if args.expected_master_pid is not None and root_pid != args.expected_master_pid:
        raise RuntimeError("the responding server is not the expected benchmark process")
    pids = server_pids(root_pid, args.workers)
    identities = {pid: process_identity(pid) for pid in pids}
    if platform.system() == "Linux":
        expected_executable = args.binary.resolve()
        if any(Path(f"/proc/{pid}/exe").resolve() != expected_executable for pid in pids):
            raise RuntimeError("a measured process is not running the supplied binary")
    if not observed_pids <= pids:
        raise RuntimeError("preflight responses came from an unexpected process")

    root = Path(__file__).resolve().parents[1]
    revision = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    version = subprocess.run([wrk, "--version"], capture_output=True, text=True)
    with args.binary.open("rb") as binary:
        binary_hash = hashlib.file_digest(binary, "sha256").hexdigest()
    report = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "hardware": args.label,
        "architecture": platform.machine(),
        "source_revision": revision,
        "binary_sha256": binary_hash,
        "build": "release, target-cpu=native" if args.native else "release",
        "source_modified": bool(subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=normal", "--", "."],
            cwd=root, text=True,
        ).strip()),
        "notes": args.notes,
        "idle_sample_seconds": args.idle_sample_seconds,
        "transport": "HTTP/1.1, loopback, persistent connections",
        "mode": "standalone" if args.workers is None else "workers",
        "serving_processes": args.workers or 1,
        "resource_processes": len(pids),
        "connections": args.connections,
        "generator_threads": args.threads,
        "requested_duration": args.duration,
        "generator": (version.stdout or version.stderr).splitlines()[0],
        "method": "GET JSON; sequential cases; all five runtimes remain loaded",
        "measured_runtimes": selected,
        "preflight": "one untimed GET per runtime before measurement",
        "access_logging": False,
        "memory_metric": "RSS before/after each case; all five runtimes included; worker mode sums master and workers and counts shared pages per process",
        "cpu_metric": "Cumulative process CPU-time delta divided by wall time; 100 percent equals one fully used CPU core",
        "results": [],
    }
    args.raw_dir.mkdir(parents=True, exist_ok=True)
    failed = False
    with tempfile.TemporaryDirectory(prefix="pentaglot-wrk-") as temporary:
        hook = Path(temporary) / "summary.lua"
        hook.write_text(SUMMARY)
        for runtime in selected:
            print(f"Measuring /{runtime}: {args.connections} connections, {args.duration}", flush=True)
            if server_pids(root_pid, args.workers) != pids:
                raise RuntimeError("the measured process set changed")
            usage_before = process_usage(pids, root_pid, args.workers)
            window_start = time.perf_counter()
            completed = subprocess.run(
                [wrk, "-t", str(args.threads), "-c", str(args.connections),
                 "-d", args.duration, "--latency", "-s", str(hook), f"{base}/{runtime}"],
                capture_output=True, text=True,
            )
            usage_after = process_usage(pids, root_pid, args.workers)
            load_finished = time.perf_counter()
            if any(process_identity(pid) != stamp for pid, stamp in identities.items()):
                raise RuntimeError("a measured PID was reused during the benchmark")
            resource_window = time.perf_counter() - window_start
            if server_pids(root_pid, args.workers) != pids:
                raise RuntimeError("the measured process set changed")
            memory_before = sum(sample["rss_bytes"] for sample in usage_before)
            memory_after = sum(sample["rss_bytes"] for sample in usage_after)
            cpu_before = sum(sample["cpu_seconds"] for sample in usage_before)
            cpu_after = sum(sample["cpu_seconds"] for sample in usage_after)
            cpu_used = cpu_after - cpu_before
            output = completed.stdout + completed.stderr
            (args.raw_dir / f"{runtime}.txt").write_text(output)
            match = re.search(r"^PENTAGLOT_RESULT (\{.*\})$", output, re.MULTILINE)
            if completed.returncode != 0 or match is None:
                report["results"].append({"runtime": runtime, "path": f"/{runtime}",
                                          "rss_before_bytes": memory_before,
                                          "rss_after_bytes": memory_after,
                                          "error": "wrk failed; inspect its local output"})
                failed = True
                continue
            measured = json.loads(match.group(1))
            seconds = measured["duration_us"] / 1_000_000
            successful = measured["requests"] - measured["errors"]["status"]
            result = {
                "runtime": runtime, "path": f"/{runtime}",
                "requests_per_second": round(successful / seconds, 2),
                "successful_requests": successful,
                "completed_requests": measured["requests"],
                "duration_seconds": round(seconds, 6),
                "p99_ms": measured["p99_us"] / 1000,
                "rss_before_bytes": memory_before,
                "rss_after_bytes": memory_after,
                "rss_delta_bytes": memory_after - memory_before,
                "cpu_before_seconds": round(cpu_before, 4),
                "cpu_after_seconds": round(cpu_after, 4),
                "cpu_used_seconds": round(cpu_used, 4),
                "cpu_percent": round(cpu_used / resource_window * 100, 2),
                "cpu_us_per_request": round(cpu_used / measured["requests"] * 1_000_000, 4) if measured["requests"] else None,
                "resource_window_seconds": round(resource_window, 6),
                "process_usage_before": usage_before,
                "process_usage_after": usage_after,
                "errors": measured["errors"],
            }
            report["results"].append(result)
            failed |= any(result["errors"].values()) or successful == 0
            print(f"/{runtime}: {result['requests_per_second']:,.2f} successful req/s; "
                  f"p99 {result['p99_ms']:.3f} ms; "
                  f"RSS {memory_before / 2**20:.2f} -> {memory_after / 2**20:.2f} MiB; "
                  f"CPU {result['cpu_percent']:.2f}% ({cpu_used:.2f} CPU s); "
                  f"{result['cpu_us_per_request']} CPU us/request; "
                  f"errors {result['errors']}", flush=True)
            if args.idle_sample_seconds:
                samples = [{
                    "requested_seconds": 0, "elapsed_seconds": 0,
                    "rss_bytes": memory_after, "cpu_seconds": round(cpu_after, 4),
                    "process_usage": usage_after,
                }]
                result["post_load_samples"] = samples
                for seconds in args.idle_sample_seconds:
                    # These observation times are supplied by the operator, not a runtime poll policy.
                    remaining = load_finished + seconds - time.perf_counter()
                    if remaining > 0:
                        time.sleep(remaining)
                    if server_pids(root_pid, args.workers) != pids or any(
                        process_identity(pid) != stamp for pid, stamp in identities.items()
                    ):
                        raise RuntimeError("the measured process family changed during idle observation")
                    usage = process_usage(pids, root_pid, args.workers)
                    rss = sum(sample["rss_bytes"] for sample in usage)
                    samples.append({
                        "requested_seconds": seconds,
                        "elapsed_seconds": round(time.perf_counter() - load_finished, 6),
                        "rss_bytes": rss,
                        "cpu_seconds": round(sum(sample["cpu_seconds"] for sample in usage), 4),
                        "process_usage": usage,
                    })
                    (args.raw_dir / f"{runtime}-idle.json").write_text(json.dumps(samples, indent=2) + "\n")
                    print(f"/{runtime}: idle +{seconds}s RSS {rss / 2**20:.2f} MiB; "
                          f"decrease from load end {(memory_after - rss) / 2**20:.2f} MiB", flush=True)
                result["rss_after_idle_bytes"] = samples[-1]["rss_bytes"]
                result["rss_returned_bytes"] = memory_after - samples[-1]["rss_bytes"]
    report["complete"] = not failed and len(report["results"]) == len(selected)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary_output.write_text(json.dumps(report, indent=2) + "\n")
    os.replace(temporary_output, args.output)
    print(f"Report saved to {args.output}", flush=True)
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
