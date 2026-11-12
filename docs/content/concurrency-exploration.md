# Explore concurrent code

rsasync and `rstypes` check their concurrent code with [loom](https://docs.rs/loom/0.7.2/loom/).
Loom runs a test many times, and each run has a different order of the thread steps. In some
runs, a load gets an older value that the memory model permits. Loom also finds data races on
shared cells. This page tells you how to write and run these exploration tests.

## The `sync` module

Each of the two crates has a private `sync` module. Each atomic, `Arc` reference count,
`UnsafeCell` shared cell, and thread park of the crate comes from that module. One algorithm thus
runs in three forms:

| Form | Selected by | Items |
| --- | --- | --- |
| Native | The default build | `core::sync::atomic`, `alloc::sync::Arc`, a `core::cell::UnsafeCell`, and, in rsasync with the `std` feature, `std::thread` |
| Without `std` | `--no-default-features` | The same items with no `thread`. Thus a wait in this build is asynchronous |
| Verification | `--cfg loom`, in the unit tests only | `loom::sync::atomic`, `loom::sync::Arc`, `loom::cell::UnsafeCell`, and, with the `std` feature, `loom::thread` |

The native form adds no run-time work. Get access to a cell only through `with` and `with_mut`.
Each one gives a raw pointer while its closure runs, so loom knows when each access starts and
ends.

Do not use `core::sync`, `alloc::sync`, `std::sync`, `std::thread`, or `core::cell::UnsafeCell`
in other production code of the two crates. Loom cannot see an item that does not come from the
module. A native test can use them to drive the code under test, for example to start a thread.
Run this search from the repository root:

```sh
git grep -nE '(^|[^:[:alnum:]_])(core|alloc|std)::(\{|sync|thread|cell::UnsafeCell)' -- rstypes/src src ':!rstypes/src/sync.rs' ':!src/sync.rs' ':(glob,exclude)**/tests.rs'
```

The search also finds each import with braces, such as `use core::{...}`, because such an import
can name `sync` on a later line. It also finds the test modules that are not in a `tests.rs`
file, and the doc examples. Examine each line that it finds.

The search finds two exceptions:

- The `alloc::sync::Arc` of a trait object in `rstypes/src/buffer.rs`, which holds the owner of
  `Lease::from_owner`. The loom `Arc` cannot hold a trait object. That count only drops the owner,
  and the exploration of `rstypes` sees no algorithm in it.
- The `core::sync::atomic` import in `src/uring.rs`. The kernel also reads and writes the head,
  tail, and flag words of an io_uring ring, so they are hardware atomics in each form. The tests
  of the ring run on a Linux kernel, not in loom.

## Write an exploration test

An exploration test is a unit test in a module named `explore`, next to the code that it checks:

```rust
#[cfg(all(test, loom))]
mod explore {
    use crate::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn two_increments_give_two() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let count = Arc::new(AtomicUsize::new(0));
            let other = Arc::clone(&count);
            let thread = loom::thread::spawn(move || {
                other.fetch_add(1, Ordering::AcqRel);
            });
            count.fetch_add(1, Ordering::AcqRel);
            thread.join().unwrap();
            assert_eq!(count.load(Ordering::Acquire), 2);
        });
    }
}
```

- Put the module behind `#[cfg(all(test, loom))]`. Loom is a dev-dependency, so the verification
  form exists only in the unit tests of the crate.
- Run the body in a loom model, and start each thread with `loom::thread::spawn`. A loom item
  that is used outside a model causes a panic.
- Set the preemption bound in the test. The bound is the maximum number of forced thread switches
  in one run, and loom checks every run within it. The result then does not change with the
  environment.
- Keep each model small: two or three threads, and few operations in each thread.

## Run the tests

```sh
cargo xtask explore                  # each exploration test in the workspace
cargo xtask explore -p rsasync       # the tests of one crate
cargo xtask explore close            # the tests whose names contain `close`
cargo xtask explore --no-run         # build the verification form, and run nothing
```

The command builds the unit tests with `--cfg loom` and debug assertions, in release mode, in
`target/loom/`. Thus it does not make the builds of the other commands stale. It runs only the
tests in modules named `explore`. The other unit tests do not run, because a loom item works only
inside a model. If no test agrees with the selection, the command fails, because an empty
selection is not a pass.

The command gives each other argument to Nextest. A test name makes the selection smaller. Do not
add a `-E` filterset: Nextest adds its tests to the selection, which can then include a test that
is not an exploration test.

When a model fails, loom shows the failed assertion. To see each step of the failed run, run the
test again with the loom log:

```sh
LOOM_LOG=trace LOOM_LOCATION=1 cargo xtask explore -p rsasync <test name>
```

## Limits

- Each crate explores only its own atomics. The atomics of a dependency stay native. Thus, when an
  rsasync exploration test uses an `rstypes` type, loom does not explore the atomics of that type.
  Explore them in the tests of `rstypes`.
- Loom does not model all of the C11 memory model. It uses `AcqRel` for each `SeqCst` load and
  store, so it can report a failure that cannot occur. It models `fence(SeqCst)` correctly. It does
  not explore load buffering, so it can miss a failure with `Relaxed` orderings. Thus each
  ordering also needs its written argument.
- A pass shows that no failure occurs within the preemption bound of the test, and no more.
- A loom item has no `const` constructor. Do not construct a `sync` item in a `const` context.

Loom 0.7.2 is a dev-dependency of the two crates, only with `--cfg loom`. A normal build does not
compile it.
