# Check the design models

rsasync has two TLA+ models of its design rules:

| Model | What it checks |
| --- | --- |
| [`models/operation-ownership/`](../../models/operation-ownership/README.md) | An operation keeps one home driver from its submission to its terminal release while its consumer moves between workers, drops, forgets, or cancels its future. Close, shutdown, and the detached debt of a zero-copy send, with the kernel events of io_uring and IOCP |
| [`models/task-lifecycle/`](../../models/task-lifecycle/README.md) | A spawn reserves a task slot before it admits the task. One owner polls a task, and a local task stays on its home. A wake from any thread, also from a signal handler, reaches its task, and a stale wake has no effect. Cancellation at a poll boundary, block_on, and shutdown |

A model checks the design, not the Rust code. Change a model when you change one of its rules.

## Install the checkers

The model command uses the Rust toolchain that `rust-toolchain.toml` pins and three tools. Put
one version of each tool under `~/.local/opt/<name>/<version>/`:

| Name | Version of the recorded evidence | Source | Path that the command uses |
| --- | --- | --- | --- |
| `temurin-jre` | 21.0.13+9 | [Eclipse Temurin](https://adoptium.net/temurin/releases/?version=21) JRE 21 | `bin/java` (on macOS, `Contents/Home/bin/java`) |
| `tla2tools` | 1.8.0 (TLC2 Version 2026.11.15.002146, rev bc1b506) | [TLA+ release v1.8.0](https://github.com/tlaplus/tlaplus/releases/tag/v1.8.0) | `tla2tools.jar` |
| `apalache` | 0.62.4 | [Apalache release v0.62.4](https://github.com/apalache-mc/apalache/releases/tag/v0.62.4), `apalache-0.62.4.tgz` without its top directory | `bin/apalache-mc` |

Compare each download with the SHA-256 that its release publishes. The TLA+ project builds the
v1.8.0 release again from its main branch, so its `tla2tools.jar` changes in place. A newer build
can give other state counts, but it must give the same verdicts. Each evidence record names the
`TLC2 Version` line of its build.

The command does not install anything. If a checker is missing, or two versions of one are
present, it stops and names the directory.

## Run the model

From the repository root:

```sh
cargo xtask model                                                       # every profile of every model
cargo xtask model operation-ownership                                   # every profile of one model
cargo xtask model operation-ownership/OperationOwnership.ZcWait_bad.cfg # one profile
cargo xtask model task-lifecycle/TaskLifecycle.cfg                      # the declared task-lifecycle profile
```

You can give several selections to one command. One model run at a time holds the host lock;
another run waits for it.

The command prints the digest of the model files, then one result for each profile:

```text
files of .../target/model/inputs/<run>/operation-ownership: sha256:<digest> (<count> files)
── tlc operation-ownership/OperationOwnership.ZcWait_bad.cfg
   `CloseReturns` is violated: as the known-bad variant requires, in 0.93s (exit status: 13)
```

- A main profile passes with `ok` and exit status 0: a complete search with no violation. The
  command reads the checker's own summary: TLC must report 0 states left on the queue, and
  Apalache the outcome `NoError` with no timed-out query. A search that stopped early fails as
  inconclusive, also when the checker exits with 0. Each checker runs with a cleared environment.
  Apalache uses an explicit empty JSON configuration and the pinned JRE on its path, so inherited
  options and user-wide settings cannot change the check.
- A `_bad` profile is a known-bad variant. It passes only when the checker reports a violation
  of its one property.
- A `_reach` profile is a reachability witness. It passes only when the checker reaches its
  target.

The command exits 0 when every selected profile passes. The output of each profile goes to
`target/model/operation-ownership/<profile>/<run>/`. Each run keeps `checker.log`,
`stderr.log`, and `trace.json` for a violation. It also keeps the exact profile and imported local
modules in `source/`, which is the directory that the checker reads. `run.txt` records the command,
source hashes, installed checker hashes, runner identity, original exit status, and verdict.
The command first retains the selected model directories under `target/model/inputs/<run>/`,
then validates and runs profiles from that revision. A new run never replaces an earlier one.
A record with no final verdict is incomplete.

TLC uses `-XX:+ExitOnOutOfMemoryError`: Java stops on heap exhaustion. This prevents the
pinned checker's [liveness index](https://github.com/tlaplus/tlaplus/blob/d8fb231de4e4/tlatools/org.lamport.tlatools/src/tlc2/tool/liveness/NodePtrTable.java)
from continuing after a failed allocation leaves its index arrays inconsistent. Such a run fails;
it gives no verification result.

Use a profile's source-closure hash to identify its exact inputs. Reuse a result only when those
inputs, the checker, and its assumptions still apply. The whole-directory digest also includes
`induction.matrix` when a model has one. Before any check, that file requires the whole
initialization, induction-step, and safety profiles for each declared domain. The step checks
`Next` and `IndInv` directly through Apalache, so adding a transition or invariant clause also adds
it to that obligation. A fault or witness cannot supply a proof. An induction profile can pair
`--discard-disabled=false` with `--no-deadlock=true` to omit enabledness prechecks. This does not
remove a transition or invariant check. The model must state its separate argument for enabledness
and for the complete, nonempty domain of its invariant initializer.

A Step profile can use `--tuning-options=search.invariantFilter=1->.*` when its initializer
already includes `IndInv`. This checks every invariant clause at the successor state and omits
the redundant initial-state check. The matrix rejects narrower filters, transition filters,
and filters on Init or Safety.

Apalache profiles can name a TLC configuration with `--config` and solver settings with
`--tuning-options-file`. Each input must be a file in the model directory, named without a directory
prefix. The command retains it with the profile. The runner owns `--config-file`; use the two model
input options for explicit settings.

## Large profiles

The operation-ownership gate has 55 profiles. Its 43 fault and witness checks take about 101
seconds of checker time on the shared development Mac. The complete searches take longer. The
largest profiles have these results on a quiet EC2 c8g.4xlarge (16 vCPUs, 32 GiB, 2026-11-11):

| Profile | Distinct states | Time |
| --- | --- | --- |
| `OperationOwnership.UringWide.cfg` | 22,058,438 | 36.1 minutes |
| `OperationOwnership.UringOps3.cfg` | 19,962,870 | 33.9 minutes |
| `OperationOwnership.IocpWide.cfg` | 7,752,242 | 12.4 minutes |
| `OperationOwnership.IocpOps3.cfg` | 9,357,002 | 15.1 minutes |

The [model evidence record](../../models/operation-ownership/README.md#evidence-record)
gives every positive profile, its host, and its retained evidence.
Each profile must finish within one hour on a 16-vCPU host, so that a run of every profile after a
change stays short. Before you run a new large profile in full, run it for 10 minutes, and estimate
its full size from the growth of the distinct states and the queue. An interrupted sample gives no result. The full profile must finish within one hour;
a larger host does not change that limit. Make the model smaller
only in a way that keeps the result sound: symmetry over identical processes, a bound that the
model proves, or an inductive proof. Each such change needs a known-bad variant that still fails
under it. A smaller bound must still reach each failure that the model names, and it needs a
profile with that bound raised by one.

The task-lifecycle gate has 27 profiles. Its 23 fault and witness checks take about 45 seconds of
checker time on the development Mac, and its declared profile about one minute. Its three-slot
profile is too large for that Mac; it has this result on a quiet EC2 c8g.4xlarge (16 vCPUs,
32 GiB, 2026-11-12):

| Profile | Distinct states | Time |
| --- | --- | --- |
| `TaskLifecycle.Slots3.cfg` | 9,177,449 | 19.1 minutes |

The [model evidence record](../../models/task-lifecycle/README.md#evidence-record) gives every
positive profile and its host. The model does not check a shutdown request during block_on, join
handles and their outcomes, a task with more than one event, operations and I/O, or time. Its
[limits](../../models/task-lifecycle/README.md#limits) give the full list.

On a smaller host, select the profiles that your change touches, and run the whole model before
you record evidence.

## When a profile fails

- A main profile reports a violation. The trace in `trace.json` starts at an initial state.
  Compare each step with the design's transitions. If the model follows the design, the trace is
  a counterexample to a design rule: the design changes first, and then the model. If the model
  does not follow the design, correct the model.
- A `_bad` profile reports "no violation". It has not supplied the required failure. Check
  whether the bounds reach the fault, the faulty transition runs, and the property detects
  the violating result before you change the profile.
- A profile reports a violation of a property other than its own. The variant fails for another
  reason, so it shows nothing about its own property.

After a change, run every profile and update the evidence record in the model's README with the
digest that the command prints.
