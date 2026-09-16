"""Run a campaign with private loopback and clean up its remaining processes."""

import argparse
import fcntl
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
ENGINES = ("luxir", "opensearch", "elasticsearch")


def network_namespace(pid="self"):
    try:
        return os.readlink(f"/proc/{pid}/ns/net")
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None


def namespace_processes(namespace):
    return {int(path.name) for path in Path("/proc").iterdir()
            if path.name.isdigit() and int(path.name) != os.getpid()
            and network_namespace(path.name) == namespace}


def signal_processes(pids, namespace, signum):
    for pid in pids:
        try:
            fd = os.pidfd_open(pid)
        except ProcessLookupError:
            continue
        try:
            # Pin the process identity before checking membership: a PID
            # reused by a host process must never receive our cleanup signal.
            if network_namespace(pid) == namespace:
                signal.pidfd_send_signal(fd, signum)
        except ProcessLookupError:
            pass
        finally:
            os.close(fd)


def cleanup(namespace):
    # Namespace membership also finds daemonized engines and their helpers.
    # Only this invocation creates processes in this namespace. Host PIDs and
    # /proc are retained, so the ordinary engine/sampler scripts still work.
    pids = namespace_processes(namespace)
    signal_processes(pids, namespace, signal.SIGTERM)
    deadline = time.monotonic() + 10
    while pids and time.monotonic() < deadline:
        time.sleep(0.1)
        pids = namespace_processes(namespace)
    signal_processes(pids, namespace, signal.SIGKILL)
    deadline = time.monotonic() + 5
    while namespace_processes(namespace):
        if time.monotonic() >= deadline:
            raise RuntimeError(f"processes remain in {namespace} after SIGKILL")
        time.sleep(0.1)
    for engine in ENGINES:
        path = ROOT / "run" / f"{engine}.pid"
        if path.exists():
            value = path.read_text().strip()
            if not value or network_namespace(int(value)) in (None, namespace):
                path.unlink()


class Interrupted(Exception):
    def __init__(self, signum):
        self.signum = signum


def interrupt(signum, _frame):
    raise Interrupted(signum)


def supervise(command, parent_namespace):
    namespace = network_namespace()
    if (not namespace or namespace == parent_namespace
            or namespace == network_namespace(os.getppid())):
        raise RuntimeError("network namespace was not isolated")
    os.environ["SEARCHBENCH_NETWORK_MODE"] = "isolated-loopback"
    os.environ["SEARCHBENCH_NETWORK_NAMESPACE"] = namespace
    run = ROOT / "run"
    run.mkdir(exist_ok=True)
    with (run / "isolated.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another isolated Searchbench command is running")
        for engine in ENGINES:
            path = run / f"{engine}.pid"
            if path.exists():
                raise RuntimeError(f"stop the existing engine first: {path}")
        print(f"Searchbench network: {namespace}, isolated loopback, UID {os.getuid()}",
              file=sys.stderr, flush=True)
        child = None
        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, interrupt)
        try:
            child = subprocess.Popen(command)
            status = child.wait()
            return status if status >= 0 else 128 - status
        except Interrupted as error:
            return 128 + error.signum
        finally:
            for signum in (signal.SIGINT, signal.SIGTERM):
                signal.signal(signum, signal.SIG_IGN)
            cleanup(namespace)
            if child is not None:
                child.wait()


def main():
    parser = argparse.ArgumentParser(
        description="Run a complete Searchbench command with isolated loopback. "
                    "Prepare corpora/downloads and set CPU policy beforehand.")
    parser.add_argument("--inside", help=argparse.SUPPRESS)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("a command is required (for example scripts/run-baseline.sh)")
    if args.inside:
        return supervise(command, args.inside)
    # Map the caller to its own UID, not root: ES and OS reject UID 0. Keep
    # namespace capabilities just long enough to raise lo, then drop them.
    setup = 'ip link set lo up; exec setpriv --inh-caps=-all --ambient-caps=-all "$@"'
    argv = ["unshare", "--user", "--map-current-user", "--net", "--keep-caps",
            "sh", "-eu", "-c", setup, "searchbench-netns", sys.executable,
            str(Path(__file__).resolve()), "--inside", network_namespace(),
            "--", *command]
    os.execvp(argv[0], argv)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, ValueError) as error:
        print(f"Searchbench network setup failed: {error}", file=sys.stderr)
        sys.exit(1)
