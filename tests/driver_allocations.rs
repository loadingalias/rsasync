//! A send and a receive of the driver allocate nothing for each operation: the kernel reads and
//! writes the buffers of the caller in place. A send or a receive that stages its bytes through an
//! allocated buffer fails this test. It is a test binary of its own, because a binary has one
//! global allocator, and the unit tests of the crate have theirs.
#![cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    feature = "std"
))]

use core::alloc::{GlobalAlloc, Layout};
use core::cell::Cell;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::alloc::System;
use std::io::{Read as _, Write as _};
use std::thread;

use rsasync::buffer::{Backing, Buffer as _, Lease};
use rsasync::driver::{Config, Driver};

/// Counts the allocations of each thread.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

#[expect(
    unsafe_code,
    reason = "the test allocator counts each allocation and forwards it to the system allocator"
)]
// SAFETY: each method forwards its arguments to the system allocator, which meets the contract.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.set(ALLOCATIONS.get() + 1);
        // SAFETY: the caller meets the contract of `alloc`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller meets the contract of `dealloc`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

/// Turns the driver and polls `future` on this thread until it gives its output.
fn drive<F: Future + Unpin>(driver: &mut Driver, mut future: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        let more = driver.turn().unwrap().more();
        if let Poll::Ready(output) = Pin::new(&mut future).poll(&mut cx) {
            return output;
        }
        if !more {
            driver.wait().unwrap();
        }
    }
}

#[test]
fn a_send_and_a_receive_allocate_nothing_for_each_operation() {
    let mut driver = Driver::new(Config::new()).unwrap();
    let mut listener = driver.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let client = thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream.write_all(b"ping").unwrap();
        let mut response = [0; 4];
        stream.read_exact(&mut response).unwrap();
        response
    });
    let accept = driver.accept(&mut listener).unwrap();
    let (mut stream, _) = drive(&mut driver, accept).unwrap().unwrap();
    let buffer = Backing::from(Vec::with_capacity(64));
    let lease = Lease::from(b"pong".to_vec());

    // The counter sees an allocation of this thread.
    let before = ALLOCATIONS.get();
    drop(Vec::<u8>::with_capacity(1));
    let counted = ALLOCATIONS.get() - before;

    let before = ALLOCATIONS.get();
    let receive = driver.recv(&mut stream, buffer).unwrap();
    let (received, buffer) = drive(&mut driver, receive).unwrap();
    let send = driver.send(&mut stream, lease).unwrap();
    let (sent, _lease) = drive(&mut driver, send).unwrap();
    let allocations = ALLOCATIONS.get() - before;

    assert_eq!((received.unwrap(), buffer.bytes()), (4, &b"ping"[..]));
    assert_eq!(sent.unwrap(), 4);
    assert_eq!((counted, allocations), (1, 0));
    assert_eq!(&client.join().unwrap(), b"pong");
}
