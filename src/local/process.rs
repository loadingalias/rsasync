//! Child processes on a local owner.
//!
//! Each call has the meaning of the [`Driver`] call of the same name. A spawn runs on the blocking
//! service of the runtime ([`Builder::blocking`](crate::runtime::Builder::blocking)). The driver
//! reaps each child that it owns once, and a wait can run on any thread.

use std::io;
use std::process::Command;

use super::{Home, Operation, PipeReader, PipeWriter, rewrap, submit};
use crate::driver::{self, ChildOptions, Cleanup, Driver, End, Refused, Resource, Signal, SignalChildError, Wait};
use crate::task::with_driver;

/// A child process whose home is the driver of the owner of this thread.
///
/// Its end policy applies at [`Child::close`], at its drop, and at the end of its home: a child with
/// `End::Kill` dies, and a child with `End::Keep` runs on, and the driver still reaps it.
#[derive(Debug)]
pub struct Child {
    /// The write end of the standard input, when the command piped it.
    pub stdin: Option<PipeWriter>,
    /// The read end of the standard output, when the command piped it.
    pub stdout: Option<PipeReader>,
    /// The read end of the standard error, when the command piped it.
    pub stderr: Option<PipeReader>,
    inner: driver::Child,
    _home: Home,
}

impl Child {
    /// The child `child`, whose pipe ends get the home that `cleanup` gives.
    fn new(mut child: driver::Child, cleanup: impl Fn(Resource) -> Cleanup) -> Self {
        let stdin = child.stdin.take().map(|end| {
            let home = cleanup(end.resource());
            PipeWriter::new(end, home)
        });
        let [stdout, stderr] = [child.stdout.take(), child.stderr.take()].map(|end| {
            end.map(|end| {
                let home = cleanup(end.resource());
                PipeReader::new(end, home)
            })
        });
        Self {
            stdin,
            stdout,
            stderr,
            inner: child,
            _home: Home::default(),
        }
    }

    /// Spawns `command` with `options`, as [`Driver::spawn`] does.
    ///
    /// # Errors
    ///
    /// A refusal, with `command`, with the reason of [`Driver::spawn`], also `Reason::Blocking`
    /// when the runtime has no blocking service, or `Reason::NoDriver` when no driver runs on this
    /// thread. The future gives an error of the system.
    #[expect(clippy::result_large_err, reason = "a refusal gives the caller its `Command` back")]
    pub fn spawn(
        command: Command,
        options: ChildOptions,
    ) -> Result<Operation<'static, io::Result<Self>>, Refused<Command>> {
        let spawn = submit(command, |driver, command| driver.spawn(command, options))?;
        Ok(Operation::home(spawn.with_output(|result, held, spawn| {
            driver::spawned_child(result, held)
                .map(|spawned| spawned.map(|child| Self::new(child, |resource| spawn.cleanup(resource))))
        })))
    }

    /// Takes `child`, which you started, with the end policy `end`, as [`Driver::adopt_child`] does.
    ///
    /// # Errors
    ///
    /// A refusal, with `child`, with the reason of [`Driver::adopt_child`], or `Reason::NoDriver`
    /// when no driver runs on this thread.
    pub fn adopt(child: std::process::Child, end: End) -> Result<Self, Refused<std::process::Child>> {
        submit(child, |driver, child| {
            let child = driver.adopt_child(child, end)?;
            Ok(Self::new(child, |resource| driver.cleanup(resource)))
        })
    }

    /// The process ID of the child.
    #[must_use]
    pub const fn id(&self) -> u32 {
        self.inner.id()
    }

    /// Gives a future of the exit status of the child, as [`driver::Child::wait`] does.
    pub fn wait(&mut self) -> Wait<'_> {
        self.inner.wait()
    }

    /// Sends `signal` to the child, as [`Driver::signal_child`] does.
    ///
    /// # Errors
    ///
    /// The [`SignalChildError`] of the driver, and `OtherDriver` when no driver of this thread owns
    /// the child.
    pub fn signal(&self, signal: Signal) -> Result<(), SignalChildError> {
        to_home(|driver| driver.signal_child(&self.inner, signal))
    }

    /// Kills the child, as [`Driver::kill_child`] does.
    ///
    /// # Errors
    ///
    /// As for [`Child::signal`].
    pub fn kill(&self) -> Result<(), SignalChildError> {
        to_home(|driver| driver.kill_child(&self.inner))
    }

    /// Sends `signal` to the process tree of the child, as [`Driver::signal_tree`] does.
    ///
    /// # Errors
    ///
    /// As for [`Child::signal`].
    pub fn signal_tree(&self, signal: Signal) -> Result<(), SignalChildError> {
        to_home(|driver| driver.signal_tree(&self.inner, signal))
    }

    /// Kills the process tree of the child, as [`Driver::kill_tree`] does.
    ///
    /// # Errors
    ///
    /// As for [`Child::signal`].
    pub fn kill_tree(&self) -> Result<(), SignalChildError> {
        to_home(|driver| driver.kill_tree(&self.inner))
    }

    /// Closes the child by its end policy, as [`Driver::close_child`] does. Its pipe ends stay
    /// open until their own close or drop.
    ///
    /// # Errors
    ///
    /// A refusal, with the child, with the reason of [`Driver::close_child`], or
    /// `Reason::NoDriver` when no driver runs on this thread.
    #[expect(
        clippy::result_large_err,
        reason = "a refusal gives the caller its child back, so its end policy still applies at its drop"
    )]
    pub fn close(self) -> Result<(), Refused<Self>> {
        let Self {
            stdin,
            stdout,
            stderr,
            inner,
            _home,
        } = self;
        submit(inner, Driver::close_child).map_err(|refused| {
            rewrap(refused, |inner| Self {
                stdin,
                stdout,
                stderr,
                inner,
                _home,
            })
        })
    }
}

/// Calls `f` with the driver of the owner of this thread. With no driver, it gives `OtherDriver`,
/// as the driver does for a child of another home: both have one recovery.
fn to_home(f: impl FnOnce(&mut Driver) -> Result<(), SignalChildError>) -> Result<(), SignalChildError> {
    with_driver((), |driver, ()| f(driver)).unwrap_or(Err(SignalChildError::OtherDriver))
}
