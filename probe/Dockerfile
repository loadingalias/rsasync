# The Linux image of `probe/linux-runner.sh`, and of Callgrind runs on this Mac.
FROM rust:1.100.0-trixie
RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -qq -y --no-install-recommends valgrind > /dev/null && rm -rf /var/lib/apt/lists/*
