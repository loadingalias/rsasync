//! Pipe ends on a local owner: a pipe end that you give to the driver, or a standard stream of a
//! [`Child`](super::Child).
//!
//! Each call has the meaning of the [`Driver`] call of the same name, under the stream rules: one
//! operation for each end, and a dropped receive leaves its bytes to the next receive. The close of
//! a write end is the end of the stream for its reader.

use std::io;
use std::os::fd::OwnedFd;

use super::{Homed, rewrap, submit};
use crate::buffer::{Backing, Lease};
use crate::driver::{self, Cleanup, Closed, Driver, Operation, Refused, Regions};

/// The read end of a pipe, whose home is the driver of the owner of this thread.
///
/// A drop closes it, as [`PipeReader::close`] does, with no future.
#[derive(Debug)]
pub struct PipeReader {
    inner: Homed<driver::PipeReader>,
}

impl PipeReader {
    /// The read end `reader`, whose close `cleanup` records at a drop.
    pub(super) fn new(reader: driver::PipeReader, cleanup: Cleanup) -> Self {
        Self {
            inner: Homed::new(reader, cleanup),
        }
    }

    /// Takes `fd`, the read end of a pipe or a FIFO, as [`Driver::adopt_reader`] does.
    ///
    /// # Errors
    ///
    /// A refusal, with `fd`, with the reason of [`Driver::adopt_reader`], or `Reason::NoDriver`
    /// when no driver runs on this thread.
    pub fn adopt(fd: OwnedFd) -> Result<Self, Refused<OwnedFd>> {
        submit(fd, |driver, fd| {
            let reader = driver.adopt_reader(fd)?;
            let cleanup = driver.cleanup(reader.resource());
            Ok(Self::new(reader, cleanup))
        })
    }

    /// Receives into the capacity of `buffer` after its initialized bytes, as [`Driver::recv`]
    /// does.
    ///
    /// # Errors
    ///
    /// A refusal, with the buffer, when the driver has no room for the operation, when no driver
    /// runs on this thread, or with `Reason::Resource` when the pipe end has another home.
    pub fn recv(&mut self, buffer: Backing) -> Result<Operation<'_, (io::Result<usize>, Backing)>, Refused<Backing>> {
        let reader = &mut self.inner.resource;
        submit(buffer, |driver, buffer| driver.recv(reader, buffer))
    }

    /// Closes the read end, as [`Driver::close_reader`] does.
    ///
    /// # Errors
    ///
    /// A refusal, with the read end, when the driver has no room for the operation or when no
    /// driver runs on this thread.
    pub fn close(self) -> Result<Operation<'static, Closed>, Refused<Self>> {
        self.inner
            .close(Driver::close_reader)
            .map_err(|refused| rewrap(refused, |inner| Self { inner }))
    }
}

/// The write end of a pipe, whose home is the driver of the owner of this thread.
///
/// A drop closes it, as [`PipeWriter::close`] does, with no future: its reader then reads the end
/// of the stream.
#[derive(Debug)]
pub struct PipeWriter {
    inner: Homed<driver::PipeWriter>,
}

impl PipeWriter {
    /// The write end `writer`, whose close `cleanup` records at a drop.
    pub(super) fn new(writer: driver::PipeWriter, cleanup: Cleanup) -> Self {
        Self {
            inner: Homed::new(writer, cleanup),
        }
    }

    /// Takes `fd`, the write end of a pipe or a FIFO, as [`Driver::adopt_writer`] does.
    ///
    /// # Errors
    ///
    /// As for [`PipeReader::adopt`].
    pub fn adopt(fd: OwnedFd) -> Result<Self, Refused<OwnedFd>> {
        submit(fd, |driver, fd| {
            let writer = driver.adopt_writer(fd)?;
            let cleanup = driver.cleanup(writer.resource());
            Ok(Self::new(writer, cleanup))
        })
    }

    /// Sends the bytes of `lease`, as [`Driver::send`] does.
    ///
    /// # Errors
    ///
    /// As for [`PipeReader::recv`], with the lease.
    pub fn send(&mut self, lease: Lease) -> Result<Operation<'_, (io::Result<usize>, Lease)>, Refused<Lease>> {
        let writer = &mut self.inner.resource;
        submit(lease, |driver, lease| driver.send(writer, lease))
    }

    /// Sends the bytes of `regions` in order, in one operation, as [`Driver::send_vectored`] does.
    ///
    /// # Errors
    ///
    /// As for [`PipeReader::recv`], with the regions.
    pub fn send_vectored(
        &mut self,
        regions: Regions,
    ) -> Result<Operation<'_, (io::Result<usize>, Regions)>, Refused<Regions>> {
        let writer = &mut self.inner.resource;
        submit(regions, |driver, regions| driver.send_vectored(writer, regions))
    }

    /// Closes the write end, as [`Driver::close_writer`] does: the future gives the progress that
    /// dropped sends owe.
    ///
    /// # Errors
    ///
    /// As for [`PipeReader::close`].
    pub fn close(self) -> Result<Operation<'static, Closed>, Refused<Self>> {
        self.inner
            .close(Driver::close_writer)
            .map_err(|refused| rewrap(refused, |inner| Self { inner }))
    }
}
