#!/usr/bin/env bash
# The early probe on a pair of hosts. Node 1 runs each server on CPUs 0-3: the rsasync server
# (`probe server`, four local workers with `Placement::Dedicated`) and a Tokio server with one
# `current_thread` runtime on each CPU, which this script builds outside the repository with the
# same compiler. Node 2 runs the open-loop client (`probe client`). The traffic uses the private
# addresses of the pair, and SSH over the tailnet only controls the hosts. The security group of
# the pair must let node 2 reach TCP port 7000 of node 1.
#
# Run it from the checkout as the local recipe of an ephemeral pair target, for example:
#
#   DEV_MACHINE_AWS_PLACEMENT_GROUP=<cluster group> \
#     ~/dev-machines/dev-machine run rsasync aws-pair-arm64 --ephemeral --execute --local \
#     bash probe/run-pair.sh
#
# For each case, a server and a count of connections, it finds the saturation: the first offered
# rate whose achieved rate is below 0.95 of it, from `PROBE_START_RATE` up by half each step. Then
# it runs 3 windows of 60 s, after 10 s of warm-up, at 70% of the saturation. Each client run
# prints one JSON line; this script adds the CPU time of the server process in the window, and its
# system calls when perf can count them. `PROBE_CASES` selects the cases, for example
# `PROBE_CASES="tokio:1000 tokio:10000"`.
set -euo pipefail

target="${PROBE_TARGET:-aws-pair-arm64}"
port=7000
server_cpus=0-3
client_workers=15
cases="${PROBE_CASES:-rsasync:1000 rsasync:10000 tokio:1000 tokio:10000}"
start_rate="${PROBE_START_RATE:-100000}"

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
# The client closes first, so each closed connection keeps its local port for 60 s (TIME_WAIT).
# A new connection can take such a port, and the range has room for each run of 10,000.
on "$client" 'sudo sysctl -q -w net.ipv4.tcp_tw_reuse=1 net.ipv4.ip_local_port_range="1024 65535"'

echo "== build"
for host in "$server" "$client"; do
    on "$host" 'cd ~/rsasync && cargo build --quiet --release --locked -p probe'
done
on "$server" 'cd ~/rsasync && cargo test --quiet --release --locked -p probe 2>&1 | tail -3'
on "$server" 'mkdir -p ~/tokio-echo/src && cp ~/rsasync/rust-toolchain.toml ~/tokio-echo/ && cat > ~/tokio-echo/Cargo.toml' <<'EOF'
[package]
name = "tokio-echo"
version = "0.0.0"
edition = "2024"

[dependencies]
libc = "=0.2.191"
tokio = { version = "=1.54.1", features = ["io-util", "net", "rt"] }

[profile.release]
codegen-units = 1
EOF
on "$server" 'cat > ~/tokio-echo/src/main.rs' <<'EOF'
//! A Tokio echo server: one `current_thread` runtime on each of the first `workers` CPUs, each
//! with its own reuse-port listener, and one task for each connection.
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn main() {
    let mut arguments = std::env::args().skip(1);
    let workers: usize = arguments.next().unwrap().parse().unwrap();
    let port: u16 = arguments.next().unwrap().parse().unwrap();
    let threads: Vec<_> = (0..workers)
        .map(|cpu| {
            std::thread::spawn(move || {
                // SAFETY: the set lives during the call, and the call changes only this thread.
                unsafe {
                    let mut set: libc::cpu_set_t = std::mem::zeroed();
                    libc::CPU_SET(cpu, &mut set);
                    assert_eq!(libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set), 0);
                }
                let runtime = tokio::runtime::Builder::new_current_thread().enable_io().build().unwrap();
                runtime.block_on(async move {
                    let socket = tokio::net::TcpSocket::new_v4().unwrap();
                    socket.set_reuseaddr(true).unwrap();
                    socket.set_reuseport(true).unwrap();
                    socket.bind(([0, 0, 0, 0], port).into()).unwrap();
                    let listener = socket.listen(4096).unwrap();
                    loop {
                        let (mut stream, _) = listener.accept().await.unwrap();
                        stream.set_nodelay(true).unwrap();
                        tokio::spawn(async move {
                            let mut buffer = vec![0_u8; 16 << 10];
                            loop {
                                let count = match stream.read(&mut buffer).await {
                                    Ok(0) | Err(_) => return,
                                    Ok(count) => count,
                                };
                                if stream.write_all(&buffer[..count]).await.is_err() {
                                    return;
                                }
                            }
                        });
                    }
                });
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}
EOF
on "$server" 'cd ~/tokio-echo && cargo build --quiet --release && grep -A1 "^name = \"tokio\"" Cargo.lock'
perf_ok="$(on "$server" 'sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y linux-tools-aws > /dev/null 2>&1; sudo perf stat -e raw_syscalls:sys_enter -- true > /dev/null 2>&1 && echo yes || echo no')"
echo "perf counts system calls: $perf_ok"

# Starts server `$1` on node 1 and prints its process ID.
start_server() {
    local command
    case "$1" in
        rsasync) command="rsasync/target/release/probe server --workers 4 --port $port" ;;
        tokio) command="tokio-echo/target/release/tokio-echo 4 $port" ;;
    esac
    on "$server" "ulimit -n 65536; cd ~; setsid nohup taskset -c $server_cpus $command > ~/$1.log 2>&1 < /dev/null & echo \$!"
}

# Runs the client at rate `$2` with `$1` connections, `$3` s of warm-up, and a window of `$4` s,
# and prints its JSON line with the CPU seconds and the system calls of server process `$5`.
measure() {
    local connections="$1" rate="$2" warmup="$3" window="$4" pid="$5" probe
    # The schedule starts 3 s after the client starts; the window starts after the warm-up.
    probe="sleep $((3 + warmup)); a=\$(awk '{print \$14+\$15}' /proc/$pid/stat)"
    if [ "$perf_ok" = yes ]; then
        probe="$probe; calls=\$(sudo perf stat -x, -e raw_syscalls:sys_enter -p $pid -- sleep $window 2>&1 | awk -F, '/raw_syscalls/ {print \$1}')"
    else
        probe="$probe; sleep $window; calls=null"
    fi
    probe="$probe; b=\$(awk '{print \$14+\$15}' /proc/$pid/stat); echo \"\$(( b - a )) \$(getconf CLK_TCK) \$calls\""
    on "$server" "$probe" > /tmp/probe-cpu.$$ &
    local result
    result="$(on "$client" "ulimit -n 65536; ~/rsasync/target/release/probe client --target $address:$port \
        --connections $connections --rate $rate --workers $client_workers --warmup $warmup --window $window --seed 1")"
    wait
    read -r ticks hz calls < /tmp/probe-cpu.$$
    rm -f /tmp/probe-cpu.$$
    echo "${result%\}},\"server_cpu_s\":$(awk -v t="$ticks" -v h="$hz" 'BEGIN { printf "%.2f", t / h }'),\"server_syscalls\":${calls:-null}}"
}

for case in $cases; do
    kind="${case%:*}"
    connections="${case#*:}"
    pid="$(start_server "$kind")"
    sleep 2
    on "$server" "head -8 ~/$kind.log"
    echo "== $kind, $connections connections: saturation"
    rate="$start_rate"
    saturation=0
    while [ "$rate" -le 4000000 ]; do
        line="$(measure "$connections" "$rate" 3 5 "$pid")"
        echo "$line"
        achieved="$(sed -E 's/.*"achieved":([0-9]+).*/\1/' <<<"$line")"
        # The saturation is the highest achieved rate of the search.
        [ "$achieved" -le "$saturation" ] || saturation="$achieved"
        if [ "$((achieved * 100))" -lt "$((rate * 95))" ]; then
            break
        fi
        rate=$((rate * 3 / 2))
    done
    point=$((saturation * 7 / 10))
    echo "== $kind, $connections connections: saturation $saturation, 3 runs at $point"
    for _ in 1 2 3; do
        measure "$connections" "$point" 10 60 "$pid"
    done
    on "$server" "kill $pid"
    sleep 2
done
