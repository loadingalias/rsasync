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
# Each run prints one JSON line. `cpu_all_s` is the busy time of each CPU of the host in the run,
# from `/proc/stat` (user, nice, system, irq, softirq, and steal), so it holds the interrupts and
# each kernel thread. `cpu_process_s` is the user and system time of the process and its threads.
# The summary subtracts the busy time of the idle host from `cpu_all_s`. The order of the cases
# rotates from round to round. `PROBE_CASES`, `PROBE_DEPTHS`, `PROBE_ROUNDS`, and `PROBE_SECONDS`
# select a partial rerun.
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
line = {"case": case, "depth": depth, "round": round_, "elapsed_s": round(elapsed, 3),
        "cpu_process_s": round(used, 3),
        "cpu_all_s": (busy(after["cpu"]) - busy(before["cpu"])) / tick,
        "cpu_irq_s": (after["cpu"][5] + after["cpu"][6] - before["cpu"][5] - before["cpu"][6]) / tick,
        "cpu_pinned_s": (busy(after["cpu" + cpu]) - busy(before["cpu" + cpu])) / tick}
if case.startswith("fio"):
    job = json.loads(output)["jobs"][0]
    read, percentile = job["read"], job["read"]["lat_ns"]["percentile"]
    line.update(ios=read["total_ios"], run_s=read["runtime"] / 1000,
                p50_ns=percentile["50.000000"], p99_ns=percentile["99.000000"],
                p999_ns=percentile["99.900000"], fio_usr_pct=job["usr_cpu"], fio_sys_pct=job["sys_cpu"])
elif case.startswith("tio"):
    # The tool prints the reads of each second but the last one, so the count of the run is the
    # mean rate for the whole time.
    scale = {"": 1, "K": 1e3, "M": 1e6}
    rates = [float(number) * scale[unit] for number, unit in re.findall(r"^IOPS=([0-9.]+)([KM]?),", output, re.M)]
    if not rates:
        sys.exit("t/io_uring printed no rate:\n" + output)
    line.update(ios=round(sum(rates) / len(rates) * elapsed), run_s=round(elapsed, 3), seconds_printed=len(rates))
elif case == "idle":
    line.update(ios=0, run_s=round(elapsed, 3))
else:
    probe = json.loads(output)
    line.update(ios=probe["count"], run_s=probe["elapsed_ns"] / 1e9, p50_ns=probe["latency_ns"]["p50"],
                p99_ns=probe["latency_ns"]["p99"], p999_ns=probe["latency_ns"]["p999"],
                probe_cpu_s=probe["cpu_ns"] / 1e9, turns=probe["turns"], waits=probe["waits"],
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
print("depth case      rounds     IOPS  cpu/io us (all)  cpu/io us (process)   p50 us   p99 us  p999 us")
table = {}
for (depth, case), group in sorted(groups.items()):
    ios = sum(run["ios"] for run in group)
    # `t/io_uring` gives no latency.
    mean = lambda key: sum(run[key] for run in group) / len(group) / 1e3 if key in group[0] else float("nan")
    row = {"iops": sum(run["ios"] / run["run_s"] for run in group) / len(group),
           "all": sum(run["cpu_all_s"] - floor * run["elapsed_s"] for run in group) / ios * 1e6,
           "process": sum(run["cpu_process_s"] for run in group) / ios * 1e6,
           "p50": mean("p50_ns"), "p99": mean("p99_ns"), "p999": mean("p999_ns")}
    table[(depth, case)] = row
    print(f"{depth:5} {case:9} {len(group):6} {row['iops']:8.0f} {row['all']:16.3f} {row['process']:20.3f}"
          f" {row['p50']:8.1f} {row['p99']:8.1f} {row['p999']:8.1f}")
print("depth  turn against    R cpu/io (all)  R cpu/io (process)  added p50 us  added p99 us")
for (depth, case), row in sorted(table.items()):
    ours = table.get((depth, "turn"))
    if case == "turn" or ours is None:
        continue
    latency = case.startswith("fio")
    added = lambda key: f"{ours[key] - row[key]:12.1f}" if latency else f"{'':>12}"
    print(f"{depth:5}  {case:14} {ours['all'] / row['all']:15.2f} {ours['process'] / row['process']:19.2f}"
          f"  {added('p50')}  {added('p99')}")
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

echo "== runs"
runs=~/runs.jsonl
: > "$runs"
python3 ~/measure.py idle 0 0 "$CPU" sleep 20 | tee -a "$runs"
read -ra cases <<< "$CASES"
for depth in $DEPTHS; do
    # The reads of the probe for one run: the rate of fio at this depth, for the time of a run.
    count="$(SECONDS_EACH=5 run fio "$depth" 0 0 | python3 -c '
import json, sys
run = json.loads(sys.stdin.readline())
print(round(run["ios"] / run["run_s"] * int(sys.argv[1])))' "$SECONDS_EACH")"
    for round in $(seq 1 "$ROUNDS"); do
        for index in "${!cases[@]}"; do
            case="${cases[$(((index + round) % ${#cases[@]}))]}"
            # A batch needs a depth of 4 or more.
            if [ "$case" = tiobatch ] && [ "$depth" -lt 4 ]; then continue; fi
            run "$case" "$depth" "$round" "$count" | tee -a "$runs"
        done
    done
done
python3 ~/measure.py idle 0 0 "$CPU" sleep 20 | tee -a "$runs"

echo "== summary"
python3 ~/summary.py < "$runs"
EOF
