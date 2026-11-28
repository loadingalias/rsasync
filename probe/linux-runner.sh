#!/usr/bin/env bash
# The runner of the `aarch64-unknown-linux-musl` tests (.cargo/config.toml). On Linux it runs the
# test binary itself. Elsewhere it runs the binary in the Docker VM, in the image of
# `probe/Dockerfile`, with the repository and the directory of the binary at their physical
# paths: `Scratch::new` makes its directory next to the binary, so that mount is writable. Build
# the image one time:
#
#   docker build -t rsasync-linux probe
set -euo pipefail
[ "$(uname -s)" = Linux ] && exec "$@"
root="$(cd "$(dirname "$0")/.." && pwd -P)"
binary="$(realpath "$1")"
shift
directory="$(dirname "$binary")"
# The test writes its output and then its exit status to files in a mounted directory. Under load,
# the `docker run` client of the Docker VM can stall after the test ended, so the status file, not
# the client, gives the end of the test.
run="$(mktemp -d "$directory/.run.XXXXXX")"
# `--init`: the test binary is no PID 1, which ignores `SIGTERM`, so the signal of a timeout of
# nextest ends the test, and `--rm` then removes its container.
docker run --rm --init --security-opt seccomp=unconfined --cap-add SYS_PTRACE \
    --ulimit nofile=65536:65536 -v "$root:$root" -v "$directory:$directory" -w "$(pwd -P)" \
    rsasync-linux sh -c 'code=0; "$@" > "$0/out" 2> "$0/err" || code=$?
        echo "$code" > "$0/status.part" && mv "$0/status.part" "$0/status"' "$run" "$binary" "$@" &
client=$!
# A client that outlives the status of its test by 1 s stalled: end it. The watchdog stops after
# 120 s, past the limit of nextest. It holds no pipe of nextest, which waits for each to close.
(
    for _ in $(seq 240); do
        kill -0 "$client" 2> /dev/null || exit 0
        if [ -e "$run/status" ]; then
            sleep 1
            kill -KILL "$client" 2> /dev/null || true
            exit 0
        fi
        sleep 0.5
    done
    kill -KILL "$client" 2> /dev/null || true
) > /dev/null 2>&1 &
watchdog=$!
finish() {
    kill "$watchdog" 2> /dev/null || true
    cat "$run/out" 2> /dev/null || true
    cat "$run/err" >&2 2> /dev/null || true
    rm -rf "$run"
}
# The timeout of nextest: the client passes the signal to the test, and its output so far prints.
trap 'kill -TERM "$client" 2> /dev/null || true; finish; exit 143' TERM INT
code=0
# The redirection keeps the notice of the shell about a client that the watchdog ended.
wait "$client" 2> /dev/null || code=$?
# With no status, the test did not end: the client gives the failure of Docker.
[ -e "$run/status" ] && code="$(cat "$run/status")"
finish
exit "$code"
