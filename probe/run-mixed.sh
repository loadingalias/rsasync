#!/usr/bin/env bash
# The mixed-mode isolation probe on a pair of hosts. Node 1 runs `probe server` with local workers
# for the echo path and movable workers that get bursts of work, in one runtime. Node 2 runs the
# open-loop client (`probe client`), which measures the echo path. The traffic uses the private
# addresses of the pair, and SSH over the tailnet only controls the hosts. The security group of
# the pair must let node 2 reach TCP port 7000 of node 1.
#
# Run it from the checkout as the local recipe of an ephemeral pair target, for example:
#
#   PROBE_TARGET=aws-pair-arm64 DEV_MACHINE_AWS_PLACEMENT_GROUP=<cluster group> \
#     ~/dev-machines/dev-machine run rsasync aws-pair-arm64 --ephemeral --execute --local \
#     bash probe/run-mixed.sh
#
# The phases: `none` (the echo path alone, with idle movable workers: the floor), `cpu` (each
# movable worker uses its CPU for 100 ms of each 200 ms), `io` (32 movable tasks send 128 KiB
# blocks over loopback TCP for 100 ms of each 200 ms), and `both`. Each round runs each phase,
# and the next round runs them in the opposite order. Each phase starts a new server.
#
# Each phase prints one JSON line: the line of the client, with the latency from the scheduled
# instant of each request for the whole window and the 99.9th percentile of each slice of 20 ms,
# and the CPU of the server in the window: `server_cpu_s` for its process, which the scheduler
# measures, and `host_cpu_s` for the busy time of each CPU of the host from `/proc/stat`, which
# the kernel samples at its tick. The summary gives each phase against the floor.
# `PROBE_RATE`, `PROBE_CONNECTIONS`, `PROBE_LOCAL`, `PROBE_MOVABLE`, `PROBE_WINDOW`, and
# `PROBE_ROUNDS` change the load, the workers, and the time.
set -euo pipefail

target="${PROBE_TARGET:?set PROBE_TARGET to the pair target of the run}"
port=7000
rate="${PROBE_RATE:-60000}"
connections="${PROBE_CONNECTIONS:-1000}"
workers="${PROBE_LOCAL:-1}"
movable="${PROBE_MOVABLE:-8}"
window="${PROBE_WINDOW:-120}"
rounds="${PROBE_ROUNDS:-2}"
warmup=5
client_workers=15

hosts="$(~/dev-machines/dev-machine nodes rsasync "$target" --live | sed -n 's/^[0-9][0-9]*=//p')"
server="$(sed -n 1p <<<"$hosts")"
client="$(sed -n 2p <<<"$hosts")"
[ -n "$server" ] && [ -n "$client" ] || { echo "the pair has no two live nodes" >&2; exit 1; }
# Runs the command `$2` on host `$1`. A command over SSH has no login shell, so it gets the path of
# the toolchain here.
on() {
    ~/dev-machines/ssh-bin/ssh "ubuntu@$1" "PATH=\$HOME/.cargo/bin:\$PATH; $2"
}

echo "== hosts"
for host in "$server" "$client"; do on "$host" 'uname -rv; nproc; lscpu | grep -m1 "Model name"'; done
address="$(on "$server" "ip -4 -o addr show scope global | awk '!/tailscale/ { split(\$4, a, \"/\"); print a[1]; exit }'")"
echo "server address: $address"
# The CPU of each interrupt of the network device of the server.
on "$server" 'for irq in $(awk -F: "/ens|eth|ena/ { print \$1 }" /proc/interrupts); do echo "irq $irq cpus=$(cat /proc/irq/$irq/effective_affinity_list)"; done | sort -u -k3 | head -40'
# The client closes first, so each closed connection keeps its local port for 60 s (TIME_WAIT).
on "$client" 'sudo sysctl -q -w net.ipv4.tcp_tw_reuse=1 net.ipv4.ip_local_port_range="1024 65535"'

echo "== build"
for host in "$server" "$client"; do on "$host" 'cd ~/rsasync && cargo build --quiet --release --locked -p probe'; done

# Runs phase `$1` in round `$2`, and prints its JSON line.
phase() {
    local bursts="$1" round="$2" pid probe result cpu ticks hz busy
    pid="$(on "$server" "ulimit -n 65536; cd ~; setsid nohup rsasync/target/release/probe server --workers $workers \
        --port $port --completions 64 --movable $movable --bursts $bursts > ~/server.log 2>&1 < /dev/null & echo \$!")"
    sleep 2
    [ "$round" != 1 ] || on "$server" 'cat ~/server.log' >&2
    # The schedule starts 3 s after the client starts, and the window starts after the warm-up.
    # The busy time of the host: user, nice, system, irq, softirq, and steal of all CPUs.
    busy="awk '/^cpu / { print \$2 + \$3 + \$4 + \$7 + \$8 + \$9 }' /proc/stat"
    probe="sleep $((3 + warmup)); a=\$(awk '{ print \$14 + \$15 }' /proc/$pid/stat); c=\$($busy); sleep $window"
    probe="$probe; b=\$(awk '{ print \$14 + \$15 }' /proc/$pid/stat); d=\$($busy); echo \"\$(( b - a )) \$(( d - c )) \$(getconf CLK_TCK)\""
    on "$server" "$probe" > "/tmp/probe-mixed.$$" &
    result="$(on "$client" "ulimit -n 65536; ~/rsasync/target/release/probe client --target $address:$port \
        --connections $connections --rate $rate --workers $client_workers --warmup $warmup --window $window \
        --seed $round --slice 20")"
    wait
    read -r ticks cpu hz < "/tmp/probe-mixed.$$"
    rm -f "/tmp/probe-mixed.$$"
    on "$server" "kill $pid"
    sleep 2
    echo "${result%\}},\"phase\":\"$bursts\",\"round\":$round,\"window_s\":$window,\"server_cpu_s\":$(awk -v t="$ticks" -v h="$hz" 'BEGIN { printf "%.2f", t / h }'),\"host_cpu_s\":$(awk -v t="$cpu" -v h="$hz" 'BEGIN { printf "%.2f", t / h }')}"
}

echo "== phases: $workers local and $movable movable workers, $connections connections, $rate requests/s, $window s"
lines="$(mktemp)"
trap 'rm -f "$lines"' EXIT
for round in $(seq 1 "$rounds"); do
    order="none cpu io both"
    [ $((round % 2)) = 1 ] || order="both io cpu none"
    for bursts in $order; do
        phase "$bursts" "$round" | tee -a "$lines"
    done
done

echo "== summary"
python3 -I - "$lines" <<'PY'
import collections, json, sys

runs = [json.loads(line) for line in open(sys.argv[1]) if line.startswith("{")]
phases = collections.defaultdict(list)
for run in runs:
    phases[run["phase"]].append(run)
mean = lambda group, key: sum(key(run) for run in group) / len(group)
rows = {}
print("phase rounds  achieved/s   p50 us   p99 us  p999 us  p999 of a 20 ms slice us (p50, p99, max)"
      "  lag p99 us  server CPUs  host CPUs")
for phase in ("none", "cpu", "io", "both"):
    group = phases.get(phase)
    if not group:
        continue
    row = {"p999": mean(group, lambda run: run["latency_ns"]["p999"]) / 1e3,
           "slice": mean(group, lambda run: run["slices"]["p999_ns"]["p99"]) / 1e3,
           "cpu": mean(group, lambda run: run["server_cpu_s"] / run["window_s"])}
    rows[phase] = row
    print(f"{phase:5} {len(group):6} {mean(group, lambda run: run['achieved']):11.0f}"
          f" {mean(group, lambda run: run['latency_ns']['p50']) / 1e3:8.1f}"
          f" {mean(group, lambda run: run['latency_ns']['p99']) / 1e3:8.1f} {row['p999']:8.1f}"
          f" {mean(group, lambda run: run['slices']['p999_ns']['p50']) / 1e3:10.1f} {row['slice']:10.1f}"
          f" {max(run['slices']['p999_ns']['max'] for run in group) / 1e3:10.1f}"
          f" {max(run['lag_ns']['p99'] for run in group) / 1e3:11.1f} {row['cpu']:12.2f}"
          f" {mean(group, lambda run: run['host_cpu_s'] / run['window_s']):10.2f}")
floor = rows.get("none")
if floor:
    print("phase  R p999  R p999 of a slice (p99)  server CPUs above the floor")
    for phase, row in rows.items():
        if phase != "none":
            print(f"{phase:5} {row['p999'] / floor['p999']:7.2f} {row['slice'] / floor['slice']:24.2f}"
                  f" {row['cpu'] - floor['cpu']:28.2f}")
PY
