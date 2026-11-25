#!/usr/bin/env bash
# The file probe on one host: random reads and durable writes of one block at queue depth 1 on a
# direct-I/O file on the instance NVMe, from `probe file` and from the io_uring engine of fio, on
# one pinned CPU. Each case runs each operation in each round. The order of the cases rotates from
# round to round, so a slow drift of the device spreads over all cases.
#
# Run it from the checkout as the local recipe of an ephemeral target, for example:
#
#   ~/dev-machines/dev-machine run rsasync aws-linux-x64-h1 --ephemeral --execute --local \
#     bash probe/run-file.sh
#
# The cases:
# - `fio`: fio with no registered files or buffers, with each fio process on the CPU. `fioalone`
#   puts only the job of fio on the CPU, and `fioqos` runs `fio` with the latency limit below.
# - `turn`, `spin`: `probe file` of the checkout. `turn` is the loop of driver-only.md, and `spin`
#   turns and never waits. `qos` runs `turn` with a limit of 20 µs on the wake latency of the CPU
#   (`pm_qos_resume_latency_us`).
# - `before`: `probe file --drive turn` of a copy of the checkout whose waits have no bound while a
#   file operation runs (`FILE_WAIT` in src/driver.rs), as before R230.
#
# Each run prints one JSON line, with the change of the use count and the time of each idle state
# of the CPU. Then the 4 KiB read of `PROBE_EVENTS` runs one more time under `perf stat`, for the
# scheduler, interrupt, idle, and io_uring events of the CPU. `PROBE_CASES`, `PROBE_OPS`,
# `PROBE_ROUNDS`, and `PROBE_COUNT` select a partial rerun.
set -euo pipefail

target="${PROBE_TARGET:-aws-linux-x64-h1}"
# `--live` prints the state of each node before the `<node>=<host>` lines.
host="$(~/dev-machines/dev-machine nodes rsasync "$target" --live | sed -n 's/^1=//p')"
[ -n "$host" ] || { echo "the target has no live node" >&2; exit 1; }

# A command over SSH has no login shell, so the script gets the path of the toolchain here.
~/dev-machines/ssh-bin/ssh "ubuntu@$host" \
    "PATH=\$HOME/.cargo/bin:\$PATH CPU=${PROBE_CPU:-1} COUNT=${PROBE_COUNT:-40000} \
     ROUNDS=${PROBE_ROUNDS:-5} CASES='${PROBE_CASES:-fio turn before}' \
     EVENTS='${PROBE_EVENTS:-fio turn before}' \
     OPS='${PROBE_OPS:-read:4096 read:65536 write:4096 write:65536}' bash -s" <<'EOF'
set -euo pipefail
size=$((4 << 30))
file=/mnt/probe/data
# The trees and their builds. The `perf` runs below run `run` as root, whose `~` is another home.
home=$HOME

echo "== host"
uname -rv
nproc
lscpu | grep -m1 'Model name'
cat /sys/devices/system/clocksource/clocksource0/current_clocksource
cat /sys/devices/system/cpu/cpuidle/current_driver 2> /dev/null || echo 'no cpuidle driver'
cat /sys/devices/system/cpu/cpuidle/current_governor_ro 2> /dev/null || true
for state in /sys/devices/system/cpu/cpu"$CPU"/cpuidle/state*; do
    [ -d "$state" ] || continue
    echo "$(basename "$state") $(cat "$state/name") latency_us=$(cat "$state/latency")" \
        "residency_us=$(cat "$state/residency") disabled=$(cat "$state/disable")"
done
grep -H . /sys/module/haltpoll/parameters/* 2> /dev/null || true

echo "== tools"
sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y fio linux-tools-aws > /dev/null 2>&1
fio --version
sudo perf version || true

echo "== device"
device="$(lsblk -dpno NAME,MODEL | awk '/Instance Storage/ { print $1; exit }')"
[ -n "$device" ] || { echo "no instance NVMe" >&2; exit 1; }
if ! findmnt -n /mnt/probe > /dev/null; then
    # Format only an empty device.
    if sudo blkid -p "$device" > /dev/null 2>&1; then echo "$device is not empty" >&2; exit 1; fi
    sudo mkfs.ext4 -q -F -m 0 "$device"
    sudo mkdir -p /mnt/probe
    sudo mount -o noatime "$device" /mnt/probe
    sudo chown ubuntu:ubuntu /mnt/probe
fi
name="$(basename "$device")"
echo "$device $(cat /sys/block/"$name"/device/model) write_cache=$(cat /sys/block/"$name"/queue/write_cache)" \
    "fua=$(cat /sys/block/"$name"/queue/fua) scheduler=$(cat /sys/block/"$name"/queue/scheduler)"
for irq in $(awk -F: '/nvme/ { print $1 }' /proc/interrupts); do
    echo "irq $irq $(awk -v irq="$irq:" '$1 == irq { print $NF }' /proc/interrupts) cpus=$(cat /proc/irq/"$irq"/effective_affinity_list)"
done
fio --name=layout --filename="$file" --size="$size" --rw=write --bs=1m --direct=1 --ioengine=io_uring \
    --iodepth=8 > /dev/null
ls -l "$file"

echo "== trees"
trees=""
for case in $CASES; do
    case "$case" in before) trees="$trees $case" ;; esac
done
# Each copy has one change, and the check of the change fails the run if the source moved.
for tree in $trees; do
    rm -rf ~/"$tree"
    mkdir ~/"$tree"
    tar -C ~/rsasync --exclude=./target --exclude=./.git --exclude=./docs -cf - . | tar -C ~/"$tree" -xf -
done
if [ -d ~/before ]; then
    sed -i 's/^        if self\.resources\.file_operations > 0$/        if false/' ~/before/src/driver.rs
    [ "$(grep -c '^        if false$' ~/before/src/driver.rs)" = 1 ]
fi
# The copies need the lints capped, and each tree builds with the same flags.
for tree in rsasync $trees; do
    (cd ~/"$tree" && RUSTFLAGS=--cap-lints=warn cargo build --quiet --release --locked -p probe --target-dir ~/target-"$tree")
done

# The use count and the time of each idle state of the CPU.
idle() {
    for state in /sys/devices/system/cpu/cpu"$CPU"/cpuidle/state*; do
        [ -d "$state" ] && echo "$(cat "$state/usage") $(cat "$state/time")"
    done
    true
}

# Runs fio for operation `$2` and block size `$3`. With `$1` of `pinned`, each fio process runs on
# the CPU. With `alone`, only the job runs there, and the other processes run on CPU 0.
fio_on() {
    local rw=randread sync="" main="$CPU"
    [ "$2" = write ] && { rw=randwrite; sync=--sync=dsync; }
    [ "$1" = alone ] && main=0
    taskset -c "$main" fio --name=probe --filename="$file" --size="$size" --ioengine=io_uring \
        --cpus_allowed="$CPU" --direct=1 --rw="$rw" $sync --bs="$3" --iodepth=1 --number_ios="$COUNT" \
        --norandommap --randrepeat=0 --lat_percentiles=1 --clat_percentiles=0 --registerfiles=0 \
        --fixedbufs=0 --output-format=json |
        python3 -c '
import json, sys
job = json.load(sys.stdin)["jobs"][0]
side = job["read"] if job["read"]["total_ios"] else job["write"]
lat = side["lat_ns"]
p = lat["percentile"]
print(json.dumps({"op": sys.argv[1], "block": int(sys.argv[2]), "drive": "fio", "count": side["total_ios"],
    "latency_ns": {"p50": p["50.000000"], "p99": p["99.000000"], "p999": p["99.900000"], "max": lat["max"]}},
    separators=(",", ":")))' "$2" "$3"
}

# Runs `probe file` of tree `$1` with the loop `$2`, for operation `$3` and block size `$4`.
probe_on() {
    taskset -c "$CPU" "$home"/target-"$1"/release/probe file --path "$file" --op "$3" --block "$4" \
        --size "$size" --count "$COUNT" --seed "$RANDOM" --drive "$2"
}

# Runs the command with a limit of 20 µs on the wake latency of the CPU, which keeps the CPU out of
# each idle state with a longer exit, and then gives the CPU its earlier limit.
with_qos() {
    local limit=/sys/devices/system/cpu/cpu"$CPU"/power/pm_qos_resume_latency_us earlier status=0
    earlier="$(cat "$limit")"
    echo 20 | sudo tee "$limit" > /dev/null
    "$@" || status=$?
    echo "$earlier" | sudo tee "$limit" > /dev/null
    return "$status"
}

# Runs case `$1` for operation `$2` and block size `$3`, and prints its JSON line.
run() {
    case "$1" in
        fio) fio_on pinned "$2" "$3" ;;
        fioalone) fio_on alone "$2" "$3" ;;
        fioqos) with_qos fio_on pinned "$2" "$3" ;;
        turn | spin) probe_on rsasync "$1" "$2" "$3" ;;
        qos) with_qos probe_on rsasync turn "$2" "$3" ;;
        before) probe_on before turn "$2" "$3" ;;
        *) echo "unknown case $1" >&2; return 1 ;;
    esac
}

echo "== rounds"
read -ra cases <<< "$CASES"
for op in $OPS; do
    for round in $(seq 1 "$ROUNDS"); do
        for index in "${!cases[@]}"; do
            case="${cases[$(((index + round) % ${#cases[@]}))]}"
            mapfile -t before < <(idle)
            line="$(run "$case" "${op%%:*}" "${op##*:}")"
            mapfile -t after < <(idle)
            states=""
            for i in "${!before[@]}"; do
                read -r used_before time_before <<< "${before[$i]}"
                read -r used_after time_after <<< "${after[$i]}"
                states="$states${states:+,}[$((used_after - used_before)),$((time_after - time_before))]"
            done
            echo "${line%\}},\"case\":\"$case\",\"round\":$round,\"idle_usage_time_us\":[$states]}"
        done
    done
done

echo "== events"
events=""
for event in sched:sched_switch sched:sched_wakeup irq:irq_handler_entry power:cpu_idle \
    io_uring:io_uring_submit_req io_uring:io_uring_complete io_uring:io_uring_task_add \
    io_uring:io_uring_local_work_run; do
    sudo perf stat -e "$event" -- true > /dev/null 2>&1 && events="$events${events:+,}$event"
done
echo "events: ${events:-none}"
if [ -n "$events" ]; then
    for case in $EVENTS; do
        output="$(sudo perf stat -C "$CPU" -x, -e "$events" -- bash -c "$(declare -f run fio_on probe_on with_qos); CPU=$CPU \
            COUNT=$COUNT size=$size file=$file home=$home run $case read 4096" 2>&1 > /dev/null)"
        echo "$case $COUNT operations: $(awk -F, '$3 != "" { printf "%s=%s ", $3, $1 }' <<< "$output")"
    done
fi
EOF
