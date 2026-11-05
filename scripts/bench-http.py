#!/usr/bin/env python3
"""Measure dispatch, serial HTTP/1 RTT or concurrent server CPU; retain local results."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
HISTORY = ROOT / ".benchmarks" / "http"
CONCURRENT_TARGETS = {
    "http1-concurrent-hyper": "hyper",
    "http1-concurrent-axum": "axum",
    "http1-concurrent-edge-other": "edge-other",
    "http1-concurrent-edge-axum": "edge-axum",
    "http1-concurrent-edge-routable": "edge-routable",
}
BENCHMARKS = {
    "dispatch-hyper": ("dispatch/native/hyper/json/request", "decoded-request-json-response"),
    "dispatch-axum": ("dispatch/native/axum/json/request", "decoded-request-json-response"),
    "dispatch-edge-other": ("dispatch/edge/other/plain/host", "decoded-request-plain-host-overhead"),
    "dispatch-edge-axum": ("dispatch/edge/axum/plain/host", "decoded-request-plain-host-overhead"),
    "dispatch-edge-pipeline": ("dispatch/edge/pipeline/plain/host", "decoded-request-plain-host-overhead"),
    "dispatch-edge-json": ("dispatch/edge/json/request", "decoded-request-json-response"),
    "http1-hyper": ("transport/http1/native/hyper/json/request", "persistent-loopback-http1-round-trip"),
    "http1-axum": ("transport/http1/native/axum/json/request", "persistent-loopback-http1-round-trip"),
    "http1-plain-hyper": ("transport/http1/native/hyper/plain/request", "native-loopback-http1-plain-round-trip"),
    "http1-plain-axum": ("transport/http1/native/axum/plain/request", "native-loopback-http1-plain-round-trip"),
    "http1-plain-edge-other": ("transport/http1/edge/other/plain/request", "native-loopback-http1-plain-round-trip"),
    "http1-plain-edge-axum": ("transport/http1/edge/axum/plain/request", "native-loopback-http1-plain-round-trip"),
    "http1-plain-edge-routable": ("transport/http1/edge/routable/plain/request", "native-loopback-http1-plain-round-trip"),
}
BENCHMARKS.update({name: (f"transport/http1/concurrent/{owner}/plain", "concurrent-http1-server-process-cpu")
                   for name, owner in CONCURRENT_TARGETS.items()})
PLAIN_TARGETS = {"dispatch-edge-other", "dispatch-edge-axum", "dispatch-edge-pipeline"}
# Keep code generation and optional adapter availability identical across targets.
FEATURES = ["edge", "axum", "tls"]


def command(*args, cwd=ROOT):
    return subprocess.check_output(args, cwd=cwd)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_digest(path):
    return digest(path.read_bytes()) if path.is_file() else None


def save_record(path, record):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(record, indent=2) + "\n")
    temporary.replace(path)


def source_state(root=ROOT):
    change = hashlib.sha256(command("git", "diff", "--no-ext-diff", "--no-textconv", "--binary", "HEAD", cwd=root))
    for name in command("git", "ls-files", "--others", "--exclude-standard", "-z", cwd=root).split(b"\0"):
        if name:
            path = root / os.fsdecode(name)
            change.update(name + b"\0")
            change.update(os.fsencode(os.readlink(path)) if path.is_symlink() else path.read_bytes())
    return {
        "commit": command("git", "rev-parse", "HEAD", cwd=root).decode().strip(),
        "dirty": bool(command("git", "status", "--porcelain", "--untracked-files=all", cwd=root)),
        "diff_sha256": change.hexdigest(),
    }


def path_dependencies():
    metadata = json.loads(command("cargo", "metadata", "--format-version", "1", "--features", ",".join(FEATURES)))
    roots = set()
    for package in metadata["packages"]:
        if package["source"] is None:
            directory = Path(package["manifest_path"]).parent
            root = Path(command("git", "rev-parse", "--show-toplevel", cwd=directory).decode().strip()).resolve()
            if root != ROOT:
                roots.add(root)
    return sorted(roots)


def build(target):
    cargo_target = "http1-concurrent" if target in CONCURRENT_TARGETS else target
    args = ["cargo", "bench", "--bench", cargo_target, "--features", ",".join(FEATURES),
            "--no-run", "--message-format=json-render-diagnostics"]
    executable = None
    with subprocess.Popen(args, cwd=ROOT, stdout=subprocess.PIPE, text=True) as process:
        for line in process.stdout:
            message = json.loads(line)
            if message.get("reason") == "compiler-message":
                rendered = message["message"].get("rendered")
                if rendered:
                    print(rendered, file=sys.stderr, end="")
            if (message.get("reason") == "compiler-artifact"
                    and message["target"]["name"] == cargo_target
                    and "bench" in message["target"]["kind"]):
                executable = message.get("executable")
        if process.wait():
            raise RuntimeError("benchmark build failed")
    if executable is None:
        raise RuntimeError("Cargo did not report a benchmark executable")
    return Path(executable)


def compatible(first, second, execution_change=False):
    before, after = dict(first.get("controls", {})), dict(second.get("controls", {}))
    if (execution_change and before.get("schema") == after.get("schema") == 8
            and before.get("boundary") == after.get("boundary") == "concurrent-http1-server-process-cpu"):
        for key in ("server_execution", "http_threads"):
            before.pop(key, None)
            after.pop(key, None)
    return first.get("status") == "passed" and before == after


def estimate_mean(estimates):
    mean = estimates["mean"]["point_estimate"]
    if not math.isfinite(mean) or mean <= 0:
        raise RuntimeError("measurement must have a finite, positive mean")
    return mean


def mean_time(record):
    if record["target"] in CONCURRENT_TARGETS:
        mean = record["measurements"]["server_cpu_ns_per_request"]
        if not math.isfinite(mean) or mean <= 0:
            raise RuntimeError("server CPU/request must be finite and positive")
        return mean
    return estimate_mean(record["estimates_ns"])


def read_estimates(directory, benchmark):
    matches = [path for path in directory.rglob("base/benchmark.json")
               if json.loads(path.read_text()).get("full_id") == benchmark]
    if len(matches) != 1:
        raise RuntimeError(f"expected one Criterion result for {benchmark}, found {len(matches)}")
    return json.loads(matches[0].with_name("estimates.json").read_text())


def positive(value):
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise argparse.ArgumentTypeError("must be finite and greater than zero")
    return result


def positive_int(value):
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return result


def validate_side(side, target, args):
    """Check one owner's samples against its own totals."""
    if side.get("target") != CONCURRENT_TARGETS[target] or side.get("server_pid", 0) <= 0:
        raise RuntimeError("serving process does not match the requested owner")
    samples = side.get("samples", [])
    if len(samples) != args.samples or side.get("warmup_requests", 0) <= 0:
        raise RuntimeError("incomplete warmup or sample count")
    for sample in samples:
        counts = sample.get("per_connection", [])
        if (len(counts) != args.connections or any(count <= 0 for count in counts)
                or sum(counts) != sample.get("requests")
                or sample.get("wall_ns", 0) <= 0
                or sample.get("user_cpu_ns", -1) < 0 or sample.get("system_cpu_ns", -1) < 0
                or sample.get("server_cpu_ns") != sample["user_cpu_ns"] + sample["system_cpu_ns"]):
            raise RuntimeError("invalid completed-request or CPU sample")
    for key in ("requests", "wall_ns", "server_cpu_ns", "user_cpu_ns", "system_cpu_ns"):
        if side.get(key, -1) != sum(sample[key] for sample in samples):
            raise RuntimeError(f"inconsistent {key} total")
    if side["requests"] <= 0 or side["wall_ns"] <= 0 or side["server_cpu_ns"] <= 0:
        raise RuntimeError("completed-request, wall and server CPU totals must be positive")
    http_threads = 1 if args.server_execution == "current_thread" else args.server_threads
    expected = {
        "requests_per_second": side["requests"] * 1e9 / side["wall_ns"],
        "server_cpu_ns_per_request": side["server_cpu_ns"] / side["requests"],
        "user_cpu_ns_per_request": side["user_cpu_ns"] / side["requests"],
        "system_cpu_ns_per_request": side["system_cpu_ns"] / side["requests"],
        "http_thread_utilization": side["server_cpu_ns"] / (side["wall_ns"] * http_threads),
        "allocations_per_request": side.get("warmup_allocations", -1) / side["warmup_requests"],
        "allocated_bytes_per_request": side.get("warmup_allocated_bytes", -1) / side["warmup_requests"],
    }
    for key, value in expected.items():
        if value < 0 or not math.isclose(side.get(key, math.nan), value):
            raise RuntimeError(f"reported {key} does not match raw samples")


def validate_concurrent(report, args):
    if (report.get("client_pid", 0) <= 0 or report.get("server_pid", 0) <= 0
            or report["client_pid"] == report["server_pid"]):
        raise RuntimeError("client and server must be separate processes")
    expected = {"mode": args.mode, "connections": args.connections,
                "server_threads": args.server_threads, "client_threads": args.client_threads,
                "server_execution": args.server_execution,
                "http_threads": 1 if args.server_execution == "current_thread" else args.server_threads}
    if any(report.get(key) != value for key, value in expected.items()):
        raise RuntimeError("server/client topology does not match requested controls")
    validate_side(report, args.target, args)
    control, paired = report.get("control"), report.get("paired")
    if args.control is None:
        if control is not None or paired is not None:
            raise RuntimeError("an unrequested control was measured")
        return
    if control is None or paired is None:
        raise RuntimeError("the requested control was not measured")
    validate_side(control, args.control, args)
    if control["server_pid"] in (report["server_pid"], report["client_pid"]):
        raise RuntimeError("target, control and client must be separate processes")
    ratios = [target["server_cpu_ns"] / target["requests"] / (other["server_cpu_ns"] / other["requests"])
              for target, other in zip(report["samples"], control["samples"])]
    expected = {
        "server_cpu_ratio": report["server_cpu_ns_per_request"] / control["server_cpu_ns_per_request"],
        "user_cpu_ns_per_request_difference":
            report["user_cpu_ns_per_request"] - control["user_cpu_ns_per_request"],
        "system_cpu_ns_per_request_difference":
            report["system_cpu_ns_per_request"] - control["system_cpu_ns_per_request"],
        "allocations_per_request_difference":
            report["allocations_per_request"] - control["allocations_per_request"],
    }
    recorded = paired.get("server_cpu_ratio_per_sample", [])
    if (len(recorded) != len(ratios)
            or not all(math.isclose(first, second) for first, second in zip(recorded, ratios))
            or not all(math.isclose(paired.get(key, math.nan), value, abs_tol=1e-9)
                       for key, value in expected.items())):
        raise RuntimeError("reported paired comparison does not match raw samples")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", choices=BENCHMARKS)
    parser.add_argument("--warmup-seconds", type=positive, required=True)
    parser.add_argument("--measurement-seconds", type=positive, required=True)
    parser.add_argument("--samples", type=int, required=True)
    parser.add_argument("--connections", type=positive_int)
    parser.add_argument("--server-threads", type=positive_int)
    parser.add_argument("--server-execution", choices=("shared", "current_thread"),
                        help="HTTP execution placement; concurrent targets default to shared")
    parser.add_argument("--client-threads", type=positive_int)
    parser.add_argument("--control", choices=CONCURRENT_TARGETS,
                        help="Measure this owner in alternating windows of the same run.")
    parser.add_argument("--mode", choices=("plain", "tls"),
                        help="What carries HTTP/1; concurrent targets default to plain. "
                             "tls measures Edge Other against Hyper only.")
    parser.add_argument("--compare", help="Compare with this local run ID, including the other target.")
    args = parser.parse_args()
    concurrent = args.target in CONCURRENT_TARGETS
    topology = (args.connections, args.server_threads, args.client_threads)
    if concurrent and not all(topology):
        parser.error("concurrent targets require --connections, --server-threads and --client-threads")
    if not concurrent and (any(value is not None for value in topology) or args.server_execution is not None
                           or args.control is not None or args.mode is not None):
        parser.error("connection/thread/execution/control/mode options apply only to concurrent targets")
    if concurrent:
        args.server_execution = args.server_execution or "shared"
        args.mode = args.mode or "plain"
        served = {"http1-concurrent-hyper", "http1-concurrent-edge-other"}
        if args.mode == "tls" and not {args.target, args.control or args.target} <= served:
            parser.error("--mode tls measures http1-concurrent-edge-other against http1-concurrent-hyper")
    if args.samples <= 0:
        parser.error("--samples must be positive")
    # Only Criterion targets require at least ten statistical samples.
    if not concurrent and args.samples < 10:
        parser.error("Criterion requires --samples >= 10")

    dependencies = path_dependencies()

    def snapshot():
        return dict(source_state(), path_dependencies={str(root): source_state(root) for root in dependencies})

    source = snapshot()
    executable = build(args.target)
    if snapshot() != source:
        raise RuntimeError("source changed during build; no measurement started")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run_id = f"{stamp}-{args.target}-{source['commit'][:12]}"
    if source["dirty"]:
        run_id += f"-dirty-{source['diff_sha256'][:12]}"
    directory = HISTORY / run_id
    directory.mkdir(parents=True)
    record = {
        "run_id": run_id,
        "target": args.target,
        "benchmark": BENCHMARKS[args.target][0],
        "status": "started",
        "source": source,
        "artifact_sha256": file_digest(executable),
        "controls": {
            "schema": 8 if concurrent else 4,
            "boundary": BENCHMARKS[args.target][1],
            "workload_sha256": {
                path.relative_to(ROOT).as_posix(): file_digest(path)
                for path in sorted((ROOT / "benches").rglob("*"))
                if path.is_file() and path.suffix in {".rs", ".yaml"}
            },
            "manifest_sha256": file_digest(ROOT / "Cargo.toml"),
            "lock_sha256": file_digest(ROOT / "Cargo.lock"),
            "cargo_config_sha256": file_digest(ROOT / ".cargo" / "config.toml"),
            "path_dependencies": source["path_dependencies"],
            "rustc": command("rustc", "-vV").decode(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "host": platform.node(),
            "logical_cpus": os.cpu_count(),
            "affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
            "allocator": "system",
            "features": FEATURES,
            "profile": "bench",
            "runtime": "tokio/multi_thread/separate-server-process" if concurrent else "tokio/current_thread",
            "concurrency": args.connections if concurrent else 1,
            "warmup_seconds": args.warmup_seconds,
            "measurement_seconds": args.measurement_seconds,
            "samples": args.samples,
            "environment": {key: value for key, value in os.environ.items()
                            if key in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET",
                                       "LD_PRELOAD", "DYLD_INSERT_LIBRARIES"}
                            or key.startswith(("CARGO_PROFILE_", "CARGO_TARGET_", "MIMALLOC_", "MALLOC_", "Malloc"))},
        },
    }
    if concurrent:
        record["controls"].update(server_threads=args.server_threads, client_threads=args.client_threads,
                                  server_execution=args.server_execution,
                                  http_threads=1 if args.server_execution == "current_thread" else args.server_threads,
                                  control=args.control, mode=args.mode)
    previous = [json.loads(path.read_text()) for path in sorted(HISTORY.glob("*/run.json"))]
    if args.compare:
        baseline = next((old for old in previous if old["run_id"] == args.compare), None)
        if baseline is None or not compatible(baseline, record, execution_change=True):
            raise RuntimeError("requested baseline is missing, incomplete or has different measurement controls")
    else:
        baseline = next((old for old in reversed(previous)
                         if old["target"] == args.target and compatible(old, record)), None)
    if baseline:
        mean_time(baseline)
    metadata = directory / "run.json"
    save_record(metadata, record)
    environment = dict(os.environ, CRITERION_HOME=str(directory / "criterion"))
    if concurrent:
        arguments = [str(executable), "--target", CONCURRENT_TARGETS[args.target],
                     "--mode", args.mode,
                     "--connections", str(args.connections),
                     "--server-threads", str(args.server_threads),
                     "--server-execution", args.server_execution,
                     "--client-threads", str(args.client_threads),
                     "--warmup-seconds", str(args.warmup_seconds),
                     "--measurement-seconds", str(args.measurement_seconds),
                     "--samples", str(args.samples), "--output", str(directory / "measurements.json")]
        if args.control:
            arguments += ["--control", CONCURRENT_TARGETS[args.control]]
    else:
        arguments = [str(executable), "--bench", "--noplot",
                 "--warm-up-time", str(args.warmup_seconds),
                 "--measurement-time", str(args.measurement_seconds),
                 "--sample-size", str(args.samples)]
    result = subprocess.run(arguments, cwd=ROOT, env=environment)
    record["status"] = "failed"
    if snapshot() != source:
        record["status"] = "source_changed"
    elif file_digest(executable) != record["artifact_sha256"]:
        record["status"] = "artifact_changed"
    elif result.returncode == 0:
        save_record(metadata, record)
        if concurrent:
            report = json.loads((directory / "measurements.json").read_text())
            validate_concurrent(report, args)
            record["measurements"] = report
        else:
            record["estimates_ns"] = read_estimates(directory / "criterion", record["benchmark"])
        mean_time(record)
        if args.target in PLAIN_TARGETS:
            group = record["benchmark"].rsplit("/", 1)[0]
            record["components_ns"] = {
                case: read_estimates(directory / "criterion", f"{group}/{case}")
                for case in ("native", "bind")
            }
            for estimates in record["components_ns"].values():
                estimate_mean(estimates)
        record["status"] = "passed"
    save_record(metadata, record)
    if record["status"] != "passed":
        raise RuntimeError(f"run is invalid ({record['status']}); retained at {metadata}")
    mean = mean_time(record)
    metric = "server CPU ns/request" if concurrent else "ns/request"
    if concurrent:
        print(f"HTTP execution: {args.server_execution}; "
              f"HTTP runtime threads: {record['measurements']['http_threads']}; "
              f"main runtime threads: {args.server_threads}")
    print(f"{run_id}: {mean:.3f} {metric}")
    if args.target in PLAIN_TARGETS:
        native = estimate_mean(record["components_ns"]["native"])
        binding = estimate_mean(record["components_ns"]["bind"])
        print(f"Native owner: {native:.3f} ns/request")
        print(f"Request binding, without host layer execution: {binding:.3f} ns/request")
        print(f"Host minus native: {mean - native:+.3f} ns/request")
        print("This delta includes response completion; it is not an exclusive pre-handler timer.")
    if baseline:
        old_mean = mean_time(baseline)
        print(f"Compared with {baseline['run_id']}: {old_mean:.3f} -> {mean:.3f} ns/request "
              f"({mean - old_mean:+.3f} ns, {(mean / old_mean - 1) * 100:+.2f}% mean time)")
        print("This arithmetic difference is not a statistical significance test.")
    print(f"Local record: {metadata}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
