#!/usr/bin/env python3
"""Measure decoded dispatch or persistent HTTP/1 round trips; retain local results."""

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
BENCHMARKS = {
    "dispatch-hyper": ("dispatch/hyper/request", "decoded-request-to-consumed-response"),
    "dispatch-axum": ("dispatch/axum/request", "decoded-request-to-consumed-response"),
    "dispatch-edge-other": ("dispatch/edge/other/request", "decoded-request-to-consumed-response"),
    "dispatch-edge-axum": ("dispatch/edge/axum/request", "decoded-request-to-consumed-response"),
    "dispatch-edge-pipeline": ("dispatch/edge/pipeline/request", "decoded-request-to-consumed-response"),
    "http1-hyper": ("transport/http1/hyper/request", "persistent-loopback-http1-round-trip"),
    "http1-axum": ("transport/http1/axum/request", "persistent-loopback-http1-round-trip"),
    "http1-edge": ("transport/http1/edge/request", "persistent-loopback-http1-round-trip"),
}
# Keep code generation and optional adapter availability identical across targets.
FEATURES = ["edge", "axum"]


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
    args = ["cargo", "bench", "--bench", target, "--features", ",".join(FEATURES),
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
                    and message["target"]["name"] == target
                    and "bench" in message["target"]["kind"]):
                executable = message.get("executable")
        if process.wait():
            raise RuntimeError("benchmark build failed")
    if executable is None:
        raise RuntimeError("Cargo did not report a benchmark executable")
    return Path(executable)


def compatible(first, second):
    return (first.get("status") == "passed"
            and first.get("controls") == second.get("controls"))


def mean_time(record):
    mean = record["estimates_ns"]["mean"]["point_estimate"]
    if not math.isfinite(mean) or mean <= 0:
        raise RuntimeError("measurement must have a finite, positive mean")
    return mean


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", choices=BENCHMARKS)
    parser.add_argument("--warmup-seconds", type=positive, required=True)
    parser.add_argument("--measurement-seconds", type=positive, required=True)
    parser.add_argument("--samples", type=int, required=True)
    parser.add_argument("--compare", help="Compare with this local run ID, including the other target.")
    args = parser.parse_args()
    # Criterion requires at least ten samples for its statistical analysis.
    if args.samples < 10:
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
            "schema": 3,
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
            "runtime": "tokio/current_thread",
            "concurrency": 1,
            "warmup_seconds": args.warmup_seconds,
            "measurement_seconds": args.measurement_seconds,
            "samples": args.samples,
            "environment": {key: value for key, value in os.environ.items()
                            if key in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET",
                                       "LD_PRELOAD", "DYLD_INSERT_LIBRARIES"}
                            or key.startswith(("CARGO_PROFILE_", "CARGO_TARGET_", "MIMALLOC_", "MALLOC_", "Malloc"))},
        },
    }
    previous = [json.loads(path.read_text()) for path in sorted(HISTORY.glob("*/run.json"))]
    if args.compare:
        baseline = next((old for old in previous if old["run_id"] == args.compare), None)
        if baseline is None or not compatible(baseline, record):
            raise RuntimeError("requested baseline is missing, incomplete or has different measurement controls")
    else:
        baseline = next((old for old in reversed(previous)
                         if old["target"] == args.target and compatible(old, record)), None)
    if baseline:
        mean_time(baseline)
    metadata = directory / "run.json"
    save_record(metadata, record)
    environment = dict(os.environ, CRITERION_HOME=str(directory / "criterion"))
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
        record["estimates_ns"] = read_estimates(directory / "criterion", record["benchmark"])
        mean_time(record)
        record["status"] = "passed"
    save_record(metadata, record)
    if record["status"] != "passed":
        raise RuntimeError(f"run is invalid ({record['status']}); retained at {metadata}")
    mean = mean_time(record)
    print(f"{run_id}: {mean:.3f} ns/request")
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
