#!/usr/bin/env bash
# The roofline probe on one host: random 4 KiB reads at queue depths 1, 32, and 128 on a fully
# written direct-I/O file on the instance NVMe, each run on one pinned CPU of a host with no other
# load. It compares `probe file` of the checkout with fio 3.43, which the host builds from its
# release commit.
#
# Run it from the checkout as the local recipe of an ephemeral target, for example:
#
#   PROBE_TARGET=aws-linux-arm64-e2 ~/dev-machines/dev-machine run rsasync aws-linux-arm64-e2 \
#     --ephemeral --execute --local bash probe/run-roofline.sh
#
# The cases:
# - `fio`: the io_uring engine of fio with fixed buffers and a registered file, its fastest form
#   with interrupts. `fioplain` has neither.
# - `tio`: `t/io_uring` of fio, a minimal loop with fixed buffers, a registered file, and a
#   registered ring, which waits for one completion. `tiobatch` submits and waits for a quarter of
#   the depth, as the tool does by default, so it shows the CPU that a batch saves.
# - `turn`: `probe file --drive turn`, the loop of driver-only.md.
#
# No case polls the device or runs a kernel thread that polls the ring.
#
# Each run prints one JSON line, with three measures of its CPU:
# - `cpu_loop_s`: the user and system time of the I/O loop alone. fio gives it for its job, the
#   probe gives it for its process from the first read to the last one, and for `t/io_uring` it is
#   the time of the process. The scheduler measures it, so it is exact.
# - `cpu_process_s`: the user and system time of the process and its threads, with its start.
# - `cpu_all_s`: the busy time of each CPU of the host in the run, from `/proc/stat` (user, nice,
#   system, irq, softirq, and steal), so it holds each kernel thread. The kernel samples it at its
#   tick, and the summary subtracts the busy time of the idle host.
#
# The script waits for a quiet host, and it measures the idle host before each depth. The order of
# the cases rotates from round to round. After the summary, one short run of each case at each
# depth counts the events of the CPU for each read under `perf stat`: the kernel entries, the
# context switches, the interrupts, and the reads that go to a worker thread of io_uring.
# `PROBE_CASES`, `PROBE_DEPTHS`, `PROBE_ROUNDS`, and `PROBE_SECONDS` select a partial rerun.
set -euo pipefail

target="${PROBE_TARGET:?set PROBE_TARGET to the target of the run}"
# `--live` prints the state of each node before the `<node>=<host>` lines.
host="$(~/dev-machines/dev-machine nodes rsasync "$target" --live | sed -n 's/^1=//p')"
[ -n "$host" ] || { echo "the target has no live node" >&2; exit 1; }

# A command over SSH has no login shell, so the script gets the path of the toolchain here.
~/dev-machines/ssh-bin/ssh "ubuntu@$host" \
    "PATH=\$HOME/.cargo/bin:\$PATH CPU=${PROBE_CPU:-1} SECONDS_EACH=${PROBE_SECONDS:-40} \
     ROUNDS=${PROBE_ROUNDS:-3} CASES='${PROBE_CASES:-fio fioplain tio tiobatch turn}' \
     DEPTHS='${PROBE_DEPTHS:-1 32 128}' bash -s" <<'EOF'
set -euo pipefail
size=$((64 << 30))
file=/mnt/probe/data
fio_commit=d2dcd7e053d1902d2420f151c83990e6acdd9f8c

echo "== host"
uname -rv
nproc
lscpu | grep -m1 'Model name' || true
cat /sys/devices/system/clocksource/clocksource0/current_clocksource
grep -E '^CONFIG_(HZ|IRQ_TIME_ACCOUNTING|TICK_CPU_ACCOUNTING|VIRT_CPU_ACCOUNTING_GEN)=' "/boot/config-$(uname -r)" || true

echo "== tools"
sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y xfsprogs git make gcc > /dev/null 2>&1
if [ ! -x ~/fio/fio ] || [ ! -x ~/fio/t/io_uring ]; then
    rm -rf ~/fio
    git clone --quiet --depth 1 --branch fio-3.43 https://github.com/axboe/fio ~/fio
    # The release commit of fio 3.43. A tag that moved fails the run.
    [ "$(git -C ~/fio rev-parse HEAD)" = "$fio_commit" ] || { echo "fio: not the release commit" >&2; exit 1; }
    (cd ~/fio && ./configure > configure.log 2>&1 && make -j"$(nproc)" fio t/io_uring > make.log 2>&1)
fi
~/fio/fio --version
git -C ~/fio rev-parse HEAD
(cd ~/rsasync && RUSTFLAGS=--cap-lints=warn cargo build --quiet --release --locked -p probe --target-dir ~/target-rsasync)

echo "== device"
device="$(lsblk -dpno NAME,MODEL | awk '/Instance Storage/ { print $1; exit }')"
[ -n "$device" ] || { echo "no instance NVMe" >&2; exit 1; }
if ! findmnt -n /mnt/probe > /dev/null; then
    # Format only an empty device.
    if sudo blkid -p "$device" > /dev/null 2>&1; then echo "$device is not empty" >&2; exit 1; fi
    sudo mkfs.xfs -q "$device"
    sudo mkdir -p /mnt/probe
    sudo mount -o noatime "$device" /mnt/probe
    sudo chown ubuntu:ubuntu /mnt/probe
fi
name="$(basename "$device")"
echo "$device $(cat /sys/block/"$name"/device/model) bytes=$(sudo blockdev --getsize64 "$device")" \
    "scheduler=$(cat /sys/block/"$name"/queue/scheduler) nr_requests=$(cat /sys/block/"$name"/queue/nr_requests)" \
    "io_poll=$(cat /sys/block/"$name"/queue/io_poll) poll_queues=$(cat /sys/module/nvme/parameters/poll_queues)"
findmnt -n -o FSTYPE,OPTIONS /mnt/probe
for irq in $(awk -F: '/nvme/ { print $1 }' /proc/interrupts); do
    echo "irq $irq $(awk -v irq="$irq:" '$1 == irq { print $NF }' /proc/interrupts) cpus=$(cat /proc/irq/"$irq"/effective_affinity_list)"
done
# Each block of the file is written, so each read reads the media.
if [ "$(stat -c %s "$file" 2> /dev/null || echo 0)" != "$size" ]; then
    ~/fio/fio --name=layout --filename="$file" --size="$size" --rw=write --bs=1m --direct=1 \
        --ioengine=io_uring --iodepth=16 --output-format=terse > /dev/null
fi
ls -l "$file"

# Runs one case, and prints its JSON line: `measure.py <case> <depth> <round> <cpu> <command>...`.
cat > ~/measure.py <<'PY'
import json, os, re, resource, subprocess, sys, time

def cpus():
    rows = {}
    with open("/proc/stat") as stat:
        for line in stat:
            if line.startswith("cpu"):
                name, *values = line.split()
                rows[name] = [int(value) for value in values]
    return rows

def busy(row):
    # user, nice, system, irq, softirq, steal
    return row[0] + row[1] + row[2] + row[5] + row[6] + row[7]

def spent(usage):
    return usage.ru_utime + usage.ru_stime

case, depth, round_, cpu = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
tick = os.sysconf("SC_CLK_TCK")
before, used, start = cpus(), spent(resource.getrusage(resource.RUSAGE_CHILDREN)), time.monotonic()
output = subprocess.run(sys.argv[5:], stdout=subprocess.PIPE, check=True).stdout.decode()
elapsed = time.monotonic() - start
used = spent(resource.getrusage(resource.RUSAGE_CHILDREN)) - used
after = cpus()
# The CPU that was most busy other than the CPU of the run: a host that is not quiet shows here.
others = {name: (busy(after[name]) - busy(before[name])) / tick for name in after if name not in ("cpu", "cpu" + cpu)}
other = max(others, key=others.get)
line = {"case": case, "depth": depth, "round": round_, "elapsed_s": round(elapsed, 3),
        "cpu_process_s": round(used, 3),
        "cpu_all_s": (busy(after["cpu"]) - busy(before["cpu"])) / tick,
        "cpu_irq_s": (after["cpu"][5] + after["cpu"][6] - before["cpu"][5] - before["cpu"][6]) / tick,
        "cpu_pinned_s": (busy(after["cpu" + cpu]) - busy(before["cpu" + cpu])) / tick,
        "cpu_other": [other, others[other]]}
if case.startswith("fio"):
    job = json.loads(output)["jobs"][0]
    read, percentile = job["read"], job["read"]["lat_ns"]["percentile"]
    line.update(ios=read["total_ios"], run_s=read["runtime"] / 1000,
                p50_ns=percentile["50.000000"], p99_ns=percentile["99.000000"],
                p999_ns=percentile["99.900000"],
                cpu_loop_s=round((job["usr_cpu"] + job["sys_cpu"]) / 100 * read["runtime"] / 1000, 3))
elif case.startswith("tio"):
    # The tool prints the reads of each second but the last one, so the count of the run is the
    # mean rate for the whole time.
    scale = {"": 1, "K": 1e3, "M": 1e6}
    rates = [float(number) * scale[unit] for number, unit in re.findall(r"^IOPS=([0-9.]+)([KM]?),", output, re.M)]
    if not rates:
        sys.exit("t/io_uring printed no rate:\n" + output)
    line.update(ios=round(sum(rates) / len(rates) * elapsed), run_s=round(elapsed, 3), seconds_printed=len(rates),
                cpu_loop_s=round(used, 3))
elif case == "idle":
    line.update(ios=0, run_s=round(elapsed, 3))
else:
    probe = json.loads(output)
    line.update(ios=probe["count"], run_s=probe["elapsed_ns"] / 1e9, p50_ns=probe["latency_ns"]["p50"],
                p99_ns=probe["latency_ns"]["p99"], p999_ns=probe["latency_ns"]["p999"],
                cpu_loop_s=probe["cpu_ns"] / 1e9, turns=probe["turns"], waits=probe["waits"],
                switches=probe["switches"])
print(json.dumps(line, separators=(",", ":")), flush=True)
PY

# The table of the runs: the mean of the rounds of each case at each depth.
cat > ~/summary.py <<'PY'
import collections, json, sys

runs = [json.loads(line) for line in sys.stdin if line.startswith("{")]
idle = [run for run in runs if run["case"] == "idle"]
# The busy CPUs of the idle host, for each second.
floor = sum(run["cpu_all_s"] for run in idle) / max(sum(run["elapsed_s"] for run in idle), 1e-9)
groups = collections.defaultdict(list)
for run in runs:
    if run["case"] != "idle":
        groups[(run["depth"], run["case"])].append(run)
print(f"idle host: {floor * 100:.2f}% of one CPU busy")
print("depth case      rounds     IOPS  cpu/io us (loop)  cpu/io us (all)   p50 us   p99 us  p999 us")
table = {}
for (depth, case), group in sorted(groups.items()):
    ios = sum(run["ios"] for run in group)
    # `t/io_uring` gives no latency.
    mean = lambda key: sum(run[key] for run in group) / len(group) / 1e3 if key in group[0] else float("nan")
    row = {"iops": sum(run["ios"] / run["run_s"] for run in group) / len(group),
           "all": sum(run["cpu_all_s"] - floor * run["elapsed_s"] for run in group) / ios * 1e6,
           "loop": sum(run["cpu_loop_s"] for run in group) / ios * 1e6,
           "p50": mean("p50_ns"), "p99": mean("p99_ns"), "p999": mean("p999_ns")}
    table[(depth, case)] = row
    print(f"{depth:5} {case:9} {len(group):6} {row['iops']:8.0f} {row['loop']:17.3f} {row['all']:16.3f}"
          f" {row['p50']:8.1f} {row['p99']:8.1f} {row['p999']:8.1f}")
print("depth  turn against    R cpu/io (loop)  R cpu/io (all)  added p50 us  added p99 us")
for (depth, case), row in sorted(table.items()):
    ours = table.get((depth, "turn"))
    if case == "turn" or ours is None:
        continue
    latency = case.startswith("fio")
    added = lambda key: f"{ours[key] - row[key]:12.1f}" if latency else f"{'':>12}"
    ratio = lambda key: ours[key] / row[key] if row[key] > 0 else float("nan")
    print(f"{depth:5}  {case:14} {ratio('loop'):16.2f} {ratio('all'):15.2f}  {added('p50')}  {added('p99')}")
PY

# The events of one run for each read: `events.py <JSON line of the run> <file of perf stat -x,>`.
cat > ~/events.py <<'PY'
import json, sys

run = json.loads(sys.argv[1])
each = []
for line in open(sys.argv[2]):
    # A line of `perf stat -x,`: the count, the unit, and the event.
    fields = line.strip().split(",")
    if len(fields) > 2 and fields[0].isdigit():
        each.append("%s=%.3f" % (fields[2].split(":")[-1], int(fields[0]) / run["ios"]))
print("%s depth %d: %d reads, for each read: %s" % (run["case"], run["depth"], run["ios"], " ".join(each)))
PY

# Runs case `$1` at depth `$2` in round `$3`, with `$4` reads for the probe.
run() {
    local case="$1" depth="$2" round="$3" count="$4" batch
    local fio=(taskset -c "$CPU" "$HOME/fio/fio" --name=rr --filename="$file" --size="$size" --direct=1
        --rw=randread --bs=4k --ioengine=io_uring --iodepth="$depth" --numjobs=1 --cpus_allowed="$CPU"
        --time_based --runtime="$SECONDS_EACH" --norandommap --randrepeat=0 --lat_percentiles=1
        --clat_percentiles=0 --percentile_list=50:99:99.9 --group_reporting --output-format=json)
    local tio=(taskset -c "$CPU" "$HOME/fio/t/io_uring" -d"$depth" -b4096 -p0 -B1 -F1 -n1 -O1 -R1 -r"$SECONDS_EACH")
    case "$case" in
        fio) set -- "${fio[@]}" --fixedbufs=1 --registerfiles=1 ;;
        fioplain) set -- "${fio[@]}" --fixedbufs=0 --registerfiles=0 ;;
        tio)
            batch=$((depth < 32 ? depth : 32))
            set -- "${tio[@]}" -s"$batch" -c1 "$file"
            ;;
        tiobatch)
            batch=$((depth / 4))
            set -- "${tio[@]}" -s"$batch" -c"$batch" "$file"
            ;;
        turn)
            set -- taskset -c "$CPU" "$HOME/target-rsasync/release/probe" file --path "$file" --op read \
                --block 4096 --size "$size" --count "$count" --seed "$RANDOM" --drive turn --depth "$depth"
            ;;
        *) echo "unknown case $case" >&2; return 1 ;;
    esac
    python3 ~/measure.py "$case" "$depth" "$round" "$CPU" "$@"
}

echo "== quiet"
# The services of the start of the host end, and no timer of the package tools starts a job.
cloud-init status --wait > /dev/null 2>&1 || true
sudo systemctl stop apt-daily.timer apt-daily-upgrade.timer man-db.timer fwupd-refresh.timer \
    unattended-upgrades.service snapd.service snapd.socket 2> /dev/null || true
# Wait until all CPUs together are busy for less than 1% of one CPU in 5 s, for 3 minutes at most.
for attempt in $(seq 1 36); do
    busy="$(python3 ~/measure.py idle 0 0 "$CPU" sleep 5 | python3 -c 'import json, sys; print(json.loads(sys.stdin.readline())["cpu_all_s"])')"
    echo "attempt $attempt: $busy s busy in 5 s"
    if python3 -c "import sys; sys.exit(0 if $busy < 0.05 else 1)"; then break; fi
done

echo "== runs"
runs=~/runs.jsonl
: > "$runs"
read -ra cases <<< "$CASES"
declare -A counts
for depth in $DEPTHS; do
    python3 ~/measure.py idle 0 0 "$CPU" sleep 15 | tee -a "$runs"
    # The reads of the probe for one run: the rate of fio at this depth, for the time of a run.
    count="$(SECONDS_EACH=5 run fio "$depth" 0 0 | python3 -c '
import json, sys
run = json.loads(sys.stdin.readline())
print(round(run["ios"] / run["run_s"]))')"
    counts[$depth]="$count"
    count=$((count * SECONDS_EACH))
    for round in $(seq 1 "$ROUNDS"); do
        for index in "${!cases[@]}"; do
            case="${cases[$(((index + round) % ${#cases[@]}))]}"
            # A batch needs a depth of 4 or more.
            if [ "$case" = tiobatch ] && [ "$depth" -lt 4 ]; then continue; fi
            run "$case" "$depth" "$round" "$count" | tee -a "$runs"
        done
    done
done
python3 ~/measure.py idle 0 0 "$CPU" sleep 15 | tee -a "$runs"

echo "== summary"
python3 ~/summary.py < "$runs"

# The events of the CPU for each read, in runs of 5 s that give no timing: a tracepoint slows the
# event that it counts.
echo "== events"
sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y linux-tools-aws > /dev/null 2>&1 || true
events=""
for event in syscalls:sys_enter_io_uring_enter sched:sched_switch irq:irq_handler_entry \
    io_uring:io_uring_submit_req io_uring:io_uring_complete io_uring:io_uring_queue_async_work; do
    sudo perf stat -e "$event" -- true > /dev/null 2>&1 && events="$events${events:+,}$event"
done
echo "events: ${events:-none}"
if [ -n "$events" ]; then
    for depth in $DEPTHS; do
        for case in $CASES; do
            if [ "$case" = tiobatch ] && [ "$depth" -lt 4 ]; then continue; fi
            line="$(sudo perf stat -C "$CPU" -x, -o /tmp/events.csv -e "$events" -- sudo -u ubuntu bash -c \
                "$(declare -f run); CPU=$CPU SECONDS_EACH=5 size=$size file=$file HOME=$HOME \
                 run $case $depth 0 $((${counts[$depth]} * 5))")"
            python3 ~/events.py "$line" /tmp/events.csv
        done
    done
fi
EOF
