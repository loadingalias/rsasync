//! Files and directories on a local owner.
//!
//! Each call has the meaning of the [`Driver`] call of the same name. Several operations of one
//! file run at one time, and each borrows the file. A close waits until each operation of the file
//! ends, also each one whose future dropped, and a drop of the file starts the same close.

use std::io;
use std::path::Path;

use super::{Homed, Operation, rewrap, submit};
use crate::buffer::{Backing, Lease};
use crate::driver::{
    self, Alignment, Buffers, Completion, Driver, Length, OpenOptions, Refused, Regions, SyncMode, WriteMode,
};

/// A file whose home is the driver of the owner of this thread.
#[derive(Debug)]
pub struct File {
    inner: Homed<driver::File>,
}

impl File {
    /// The file that `operation` opened.
    fn new(file: driver::File, operation: &Completion) -> Self {
        let cleanup = operation.cleanup(file.resource());
        Self {
            inner: Homed::new(file, cleanup),
        }
    }

    /// Opens the file at `path` with `options`, as [`Driver::open_file`] does.
    ///
    /// # Errors
    ///
    /// A refusal when the driver has no room for the operation, or when no driver runs on this
    /// thread. The future gives an error of the system.
    pub fn open(path: &Path, options: OpenOptions) -> Result<Operation<'static, io::Result<Self>>, Refused<()>> {
        let open = submit((), |driver, ()| driver.open_file(path, options))?;
        Ok(Operation::home(open.with_output(|result, held, open| {
            driver::file_opened(result, held).map(|opened| opened.map(|file| Self::new(file, open)))
        })))
    }

    /// The alignment of direct I/O, for a file that `Cache::Direct` opened.
    #[must_use]
    pub const fn alignment(&self) -> Option<Alignment> {
        self.inner.resource.alignment()
    }

    /// Reads into the capacity of `buffer` after its initialized bytes, from `offset`, as
    /// [`Driver::read_at`] does.
    ///
    /// # Errors
    ///
    /// A refusal, with the buffer, when the driver has no room for the operation, when no driver
    /// runs on this thread, with `Reason::Resource` when the file has another home, or with
    /// `Reason::Alignment`.
    pub fn read_at(
        &self,
        buffer: Backing,
        offset: u64,
    ) -> Result<Operation<'_, (io::Result<usize>, Backing)>, Refused<Backing>> {
        let file = &self.inner.resource;
        submit(buffer, |driver, buffer| driver.read_at(file, buffer, offset)).map(Operation::home)
    }

    /// Reads until `buffer` is full, from `offset`, as [`Driver::read_exact_at`] does.
    ///
    /// # Errors
    ///
    /// As for [`File::read_at`].
    pub fn read_exact_at(
        &self,
        buffer: Backing,
        offset: u64,
    ) -> Result<Operation<'_, (io::Result<()>, Backing)>, Refused<Backing>> {
        let file = &self.inner.resource;
        submit(buffer, |driver, buffer| driver.read_exact_at(file, buffer, offset)).map(Operation::home)
    }

    /// Reads into `buffers` in order, from `offset`, as [`Driver::read_vectored_at`] does.
    ///
    /// # Errors
    ///
    /// As for [`File::read_at`], with the buffers.
    pub fn read_vectored_at(
        &self,
        buffers: Buffers,
        offset: u64,
    ) -> Result<Operation<'_, (io::Result<usize>, Buffers)>, Refused<Buffers>> {
        let file = &self.inner.resource;
        submit(buffers, |driver, buffers| {
            driver.read_vectored_at(file, buffers, offset)
        })
        .map(Operation::home)
    }

    /// Writes the bytes of `lease` at `offset`, with `mode`, as [`Driver::write_at`] does.
    ///
    /// # Errors
    ///
    /// As for [`File::read_at`], with the lease.
    pub fn write_at(
        &self,
        lease: Lease,
        offset: u64,
        mode: WriteMode,
    ) -> Result<Operation<'_, (io::Result<usize>, Lease)>, Refused<Lease>> {
        let file = &self.inner.resource;
        submit(lease, |driver, lease| driver.write_at(file, lease, offset, mode)).map(Operation::home)
    }

    /// Writes the bytes of `regions` in order at `offset`, as [`Driver::write_vectored_at`] does.
    ///
    /// # Errors
    ///
    /// As for [`File::read_at`], with the regions.
    pub fn write_vectored_at(
        &self,
        regions: Regions,
        offset: u64,
        mode: WriteMode,
    ) -> Result<Operation<'_, (io::Result<usize>, Regions)>, Refused<Regions>> {
        let file = &self.inner.resource;
        submit(regions, |driver, regions| {
            driver.write_vectored_at(file, regions, offset, mode)
        })
        .map(Operation::home)
    }

    /// The length of the file, as [`Driver::file_len`] does.
    ///
    /// # Errors
    ///
    /// As for [`File::read_at`].
    pub fn len(&self) -> Result<Operation<'_, io::Result<u64>>, Refused<()>> {
        let file = &self.inner.resource;
        submit((), |driver, ()| driver.file_len(file)).map(Operation::home)
    }

    /// Changes the length of the file to `len`, as [`Driver::set_file_len`] does.
    ///
    /// # Errors
    ///
    /// As for [`File::read_at`].
    pub fn set_len(&self, len: u64) -> Result<Operation<'_, io::Result<()>>, Refused<()>> {
        let file = &self.inner.resource;
        submit((), |driver, ()| driver.set_file_len(file, len)).map(Operation::home)
    }

    /// Allocates the blocks of `len` bytes at `offset`, as [`Driver::allocate`] does.
    ///
    /// # Errors
    ///
    /// As for [`File::read_at`].
    pub fn allocate(
        &self,
        offset: u64,
        len: u64,
        length: Length,
    ) -> Result<Operation<'_, io::Result<()>>, Refused<()>> {
        let file = &self.inner.resource;
        submit((), |driver, ()| driver.allocate(file, offset, len, length)).map(Operation::home)
    }

    /// Frees the blocks of `len` bytes at `offset`, as [`Driver::punch_hole`] does.
    ///
    /// # Errors
    ///
    /// As for [`File::read_at`].
    pub fn punch_hole(&self, offset: u64, len: u64) -> Result<Operation<'_, io::Result<()>>, Refused<()>> {
        let file = &self.inner.resource;
        submit((), |driver, ()| driver.punch_hole(file, offset, len)).map(Operation::home)
    }

    /// Makes the contents of the file durable by `mode`, as [`Driver::sync`] does.
    ///
    /// # Errors
    ///
    /// As for [`File::read_at`].
    pub fn sync(&self, mode: SyncMode) -> Result<Operation<'_, io::Result<()>>, Refused<()>> {
        let file = &self.inner.resource;
        submit((), |driver, ()| driver.sync(file, mode)).map(Operation::home)
    }

    /// Closes the file, as [`Driver::close_file`] does: the future ends after each operation of the
    /// file ended.
    ///
    /// # Errors
    ///
    /// A refusal, with the file, when the driver has no room for the operation or when no driver
    /// runs on this thread.
    pub fn close(self) -> Result<Operation<'static, ()>, Refused<Self>> {
        self.inner
            .close(Driver::close_file)
            .map_err(|refused| rewrap(refused, |inner| Self { inner }))
    }
}

/// A directory whose home is the driver of the owner of this thread, for [`Directory::sync`].
#[derive(Debug)]
pub struct Directory {
    inner: Homed<driver::Directory>,
}

impl Directory {
    /// Opens the directory at `path`, as [`Driver::open_directory`] does.
    ///
    /// # Errors
    ///
    /// As for [`File::open`].
    pub fn open(path: &Path) -> Result<Operation<'static, io::Result<Self>>, Refused<()>> {
        let open = submit((), |driver, ()| driver.open_directory(path))?;
        Ok(Operation::home(open.with_output(|result, held, open| {
            driver::directory_opened(result, held).map(|opened| {
                opened.map(|directory| {
                    let cleanup = open.cleanup(directory.resource());
                    Self {
                        inner: Homed::new(directory, cleanup),
                    }
                })
            })
        })))
    }

    /// Makes the names of the directory durable, as [`Driver::sync_directory`] does.
    ///
    /// # Errors
    ///
    /// As for [`File::read_at`].
    pub fn sync(&self) -> Result<Operation<'_, io::Result<()>>, Refused<()>> {
        let directory = &self.inner.resource;
        submit((), |driver, ()| driver.sync_directory(directory)).map(Operation::home)
    }

    /// Closes the directory, as [`Driver::close_directory`] does.
    ///
    /// # Errors
    ///
    /// As for [`File::close`].
    pub fn close(self) -> Result<Operation<'static, ()>, Refused<Self>> {
        self.inner
            .close(Driver::close_directory)
            .map_err(|refused| rewrap(refused, |inner| Self { inner }))
    }
}

/// Renames `from` to `to`, as [`Driver::rename`] does, on the driver of this thread.
///
/// # Errors
///
/// A refusal when the driver has no room for the operation, or when no driver runs on this thread.
/// The future gives an error of the system.
pub fn rename(from: &Path, to: &Path) -> Result<Operation<'static, io::Result<()>>, Refused<()>> {
    submit((), |driver, ()| driver.rename(from, to)).map(Operation::home)
}

/// Removes the name `path`, as [`Driver::unlink`] does, on the driver of this thread.
///
/// # Errors
///
/// As for [`rename`].
pub fn unlink(path: &Path) -> Result<Operation<'static, io::Result<()>>, Refused<()>> {
    submit((), |driver, ()| driver.unlink(path)).map(Operation::home)
}
