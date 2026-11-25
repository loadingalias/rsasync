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
# `--init`: the test binary is no PID 1, which ignores `SIGTERM`, so the signal of a timeout of
# nextest ends the test, and `--rm` then removes its container.
exec docker run --rm --init --security-opt seccomp=unconfined --cap-add SYS_PTRACE \
    --ulimit nofile=65536:65536 -v "$root:$root" -v "$directory:$directory" -w "$(pwd -P)" \
    rsasync-linux "$binary" "$@"
