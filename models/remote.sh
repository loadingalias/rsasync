#!/usr/bin/env bash
# Runs model profiles on one remote Linux host, for a profile that needs more cores or memory
# than this Mac has. The host runs the commit `HEAD` of this checkout. It gets the commit as a Git
# bundle: a synced checkout has no Git directory, and `cargo xtask model` records the revision. It
# gets TLC and Apalache from `~/.local/opt` of this Mac, so the host runs the same builds. It
# downloads the JRE of its own platform and compares it with the SHA-256 of its release.
#
# Run it from a clean checkout as the local recipe of an ephemeral target, for example:
#
#   MODEL_TARGET=aws-linux-arm64-ubuntu2604 DEV_MACHINE_MAX_LEASE_MINUTES=100 \
#     ~/dev-machines/dev-machine run rsasync aws-linux-arm64-ubuntu2604 --ephemeral --execute \
#     --local bash models/remote.sh task-lifecycle/TaskLifecycle.Slots3.cfg
#
# The output has the record (`run.txt`) and the summary of the checker for each profile.
set -euo pipefail

target="${MODEL_TARGET:?set MODEL_TARGET to the target of the run}"
[ "$#" -gt 0 ] || { echo "usage: remote.sh <model>[/<profile>]..." >&2; exit 1; }
[ -z "$(git status --porcelain)" ] || { echo "the checkout has changes, and the host runs the commit HEAD" >&2; exit 1; }
revision="$(git rev-parse HEAD)"
# `--live` prints the state of each node before the `<node>=<host>` lines.
host="$(~/dev-machines/dev-machine nodes rsasync "$target" --live | sed -n 's/^1=//p')"
[ -n "$host" ] || { echo "the target has no live node" >&2; exit 1; }
remote=(~/dev-machines/ssh-bin/ssh "ubuntu@$host")

opt=~/.local/opt
tlc="$(cd "$opt/tla2tools" && echo *)"
apalache="$(cd "$opt/apalache" && echo *)"
tlc_sha="$(shasum -a 256 "$opt/tla2tools/$tlc/tla2tools.jar" | cut -d' ' -f1)"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
git bundle create -q "$scratch/source.bundle" HEAD
tar -C "$opt" -cf - "tla2tools/$tlc" "apalache/$apalache" | "${remote[@]}" 'mkdir -p ~/.local/opt && tar -C ~/.local/opt -xf -'
"${remote[@]}" 'cat > ~/source.bundle' < "$scratch/source.bundle"

# A command over SSH has no login shell, so the script gets the path of the toolchain here.
"${remote[@]}" "PATH=\$HOME/.cargo/bin:\$PATH REVISION=$revision TLC_SHA=$tlc_sha bash -s -- $*" <<'EOF'
set -euo pipefail
echo "== host"
uname -rv
nproc
free -g | sed -n 2p
opt=$HOME/.local/opt
echo "$TLC_SHA  $(echo "$opt"/tla2tools/*/tla2tools.jar)" | sha256sum -c

# The JRE of this platform, from its release of Eclipse Temurin.
jre=21.0.13+9
case "$(uname -m)" in
    aarch64) arch=aarch64 ;;
    x86_64) arch=x64 ;;
    *) echo "no JRE for $(uname -m)" >&2; exit 1 ;;
esac
file="OpenJDK21U-jre_${arch}_linux_hotspot_${jre/+/_}.tar.gz"
url="https://github.com/adoptium/temurin21-binaries/releases/download/jdk-${jre/+/%2B}/$file"
(cd /tmp && curl -fsSLo "$file" "$url" && curl -fsSLo "$file.sha256.txt" "$url.sha256.txt" && sha256sum -c "$file.sha256.txt")
mkdir -p "$opt/temurin-jre/$jre"
tar -xzf "/tmp/$file" -C "$opt/temurin-jre/$jre" --strip-components=1

rm -rf ~/model
git clone -q ~/source.bundle ~/model
cd ~/model
git -c advice.detachedHead=false checkout -q --detach "$REVISION"
echo "== revision $(git rev-parse HEAD)"
status=0
cargo xtask model "$@" || status=$?

echo "== records"
for record in $(find target/model -name run.txt | sort); do
    echo "-- $record"
    cat "$record"
    grep -E "distinct states found, 0 states left|depth of the complete|Checking temporal|Finished|Error|violated" \
        "$(dirname "$record")/checker.log" | tail -8
done
exit "$status"
EOF
