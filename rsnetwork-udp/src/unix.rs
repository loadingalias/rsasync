use core::mem::{offset_of, size_of};
use core::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

mod control;
#[cfg(feature = "std")]
pub(crate) mod io;
pub mod sys;

pub use control::{Control, Ecn, Metadata, Transmit};

/// `sa_family_t`: the type of an address family.
#[cfg(target_os = "linux")]
pub type SaFamily = u16;
/// `sa_family_t`: the type of an address family.
#[cfg(target_os = "macos")]
pub type SaFamily = u8;

/// The address family of IPv4 (`AF_INET`).
pub const AF_INET: SaFamily = 2;
/// The address family of IPv6 (`AF_INET6`).
#[cfg(target_os = "linux")]
pub const AF_INET6: SaFamily = 10;
/// The address family of IPv6 (`AF_INET6`).
#[cfg(target_os = "macos")]
pub const AF_INET6: SaFamily = 30;

/// `struct sockaddr_in`: an IPv4 address and port, both in network byte order. The layout agrees with
/// `netinet/in.h` of Linux 6.1 and of the macOS 27.2 SDK.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SockAddrIn {
    /// The length of the address, 16.
    #[cfg(target_os = "macos")]
    pub sin_len: u8,
    /// [`AF_INET`].
    pub sin_family: SaFamily,
    /// The port, in network byte order.
    pub sin_port: u16,
    /// The address, in network byte order.
    pub sin_addr: [u8; 4],
    /// Padding to the size of `struct sockaddr`.
    pub sin_zero: [u8; 8],
}

/// `struct sockaddr_in6`: an IPv6 address, port, flow label, and scope. The layout agrees with `linux/in6.h` of
/// Linux 6.1 and `netinet6/in6.h` of the macOS 27.2 SDK.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SockAddrIn6 {
    /// The length of the address, 28.
    #[cfg(target_os = "macos")]
    pub sin6_len: u8,
    /// [`AF_INET6`].
    pub sin6_family: SaFamily,
    /// The port, in network byte order.
    pub sin6_port: u16,
    /// The flow information, in network byte order.
    pub sin6_flowinfo: u32,
    /// The address, in network byte order.
    pub sin6_addr: [u8; 16],
    /// The scope identifier, in host byte order.
    pub sin6_scope_id: u32,
}

/// `struct __kernel_sockaddr_storage` of Linux 6.1, or `struct sockaddr_storage` of the macOS 27.2 SDK: room for
/// each socket address, with the alignment of a pointer. The kernel writes an address into it, for example at an
/// accept.
#[repr(C, align(8))]
#[derive(Debug, Clone, Copy)]
pub struct SockAddrStorage {
    /// The length of the address.
    #[cfg(target_os = "macos")]
    pub ss_len: u8,
    /// The address family, which tells the layout of the bytes.
    pub ss_family: SaFamily,
    data: [u8; 126],
}

impl SockAddrStorage {
    /// Storage with no address.
    pub const EMPTY: Self = Self {
        #[cfg(target_os = "macos")]
        ss_len: 0,
        ss_family: 0,
        data: [0; 126],
    };

    /// The size of the storage, as the kernel takes it in a `socklen_t`.
    pub const LEN: u32 = 128;

    /// Storage that holds `address`, and the length of that address, as a call takes them.
    #[must_use]
    pub fn new(address: SocketAddr) -> (Self, u32) {
        let mut storage = Self::EMPTY;
        match address {
            SocketAddr::V4(address) => {
                storage.write(SockAddrIn {
                    #[cfg(target_os = "macos")]
                    sin_len: 16,
                    sin_family: AF_INET,
                    sin_port: address.port().to_be(),
                    sin_addr: address.ip().octets(),
                    sin_zero: [0; 8],
                });
                (storage, 16)
            }
            SocketAddr::V6(address) => {
                storage.write(SockAddrIn6 {
                    #[cfg(target_os = "macos")]
                    sin6_len: 28,
                    sin6_family: AF_INET6,
                    sin6_port: address.port().to_be(),
                    sin6_flowinfo: address.flowinfo().to_be(),
                    sin6_addr: address.ip().octets(),
                    sin6_scope_id: address.scope_id(),
                });
                (storage, 28)
            }
        }
    }

    /// The address that the kernel wrote, with the length that it gave. `None` for another
    /// family, or for a length that is too short for its family.
    #[must_use]
    pub fn to_socket_addr(&self, len: u32) -> Option<SocketAddr> {
        let len = usize::try_from(len).ok()?;
        match self.ss_family {
            AF_INET if len >= size_of::<SockAddrIn>() => {
                let raw = self.read::<SockAddrIn>();
                Some(SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::from(raw.sin_addr),
                    u16::from_be(raw.sin_port),
                )))
            }
            AF_INET6 if len >= size_of::<SockAddrIn6>() => {
                let raw = self.read::<SockAddrIn6>();
                Some(SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(raw.sin6_addr),
                    u16::from_be(raw.sin6_port),
                    u32::from_be(raw.sin6_flowinfo),
                    raw.sin6_scope_id,
                )))
            }
            _ => None,
        }
    }

    /// Writes `address` at the start of the storage.
    #[expect(unsafe_code, reason = "the storage holds the bytes of each socket address")]
    fn write<A: Address>(&mut self, address: A) {
        // SAFETY: `A` is a socket address, which is smaller than the storage and needs no more
        // alignment.
        unsafe { core::ptr::from_mut(self).cast::<A>().write(address) };
    }

    /// The bytes of the storage as the address type `A`.
    #[expect(unsafe_code, reason = "the storage holds the bytes of each socket address")]
    fn read<A: Address>(&self) -> A {
        // SAFETY: `A` is a socket address, which is smaller than the storage and needs no more
        // alignment, and each of its bit patterns is valid: it has only integers and byte arrays.
        unsafe { core::ptr::from_ref(self).cast::<A>().read() }
    }
}

impl Default for SockAddrStorage {
    fn default() -> Self {
        Self::EMPTY
    }
}

/// A socket address type that [`SockAddrStorage`] can hold.
trait Address {}
impl Address for SockAddrIn {}
impl Address for SockAddrIn6 {}

const _: () = {
    assert!(size_of::<SockAddrIn>() == 16);
    assert!(offset_of!(SockAddrIn, sin_port) == 2);
    assert!(offset_of!(SockAddrIn, sin_addr) == 4);
    assert!(size_of::<SockAddrIn6>() == 28);
    assert!(offset_of!(SockAddrIn6, sin6_flowinfo) == 4);
    assert!(offset_of!(SockAddrIn6, sin6_addr) == 8);
    assert!(offset_of!(SockAddrIn6, sin6_scope_id) == 24);
    assert!(size_of::<SockAddrStorage>() == 128);
    assert!(align_of::<SockAddrStorage>() == 8);
    assert!(align_of::<SockAddrIn>() <= align_of::<SockAddrStorage>());
    assert!(align_of::<SockAddrIn6>() <= align_of::<SockAddrStorage>());
    #[cfg(target_os = "macos")]
    assert!(
        offset_of!(SockAddrIn, sin_family) == 1
            && offset_of!(SockAddrIn6, sin6_family) == 1
            && offset_of!(SockAddrStorage, ss_family) == 1
    );
};

#[cfg(test)]
mod tests {
    use super::{AF_INET, AF_INET6, SockAddrIn, SockAddrIn6, SockAddrStorage};
    use core::net::SocketAddr;

    /// Storage that holds `address` as the kernel writes it.
    #[expect(unsafe_code, reason = "the test writes an address as the kernel does")]
    fn storage<A>(address: A) -> SockAddrStorage {
        let mut storage = SockAddrStorage::EMPTY;
        // SAFETY: the address is smaller than the storage and needs no more alignment.
        unsafe { core::ptr::from_mut(&mut storage).cast::<A>().write(address) };
        storage
    }

    /// The decode reads the bytes in network order, as the kernel writes them, for each family;
    /// a short length or another family gives no address.
    #[test]
    fn an_address_in_network_order_decodes_to_its_socket_address() {
        let v4 = storage(SockAddrIn {
            #[cfg(target_os = "macos")]
            sin_len: 16,
            sin_family: AF_INET,
            sin_port: 8080_u16.to_be(),
            sin_addr: [127, 0, 0, 1],
            sin_zero: [0; 8],
        });
        assert_eq!(
            v4.to_socket_addr(16),
            Some("127.0.0.1:8080".parse::<SocketAddr>().unwrap())
        );
        assert_eq!(v4.to_socket_addr(15), None);

        let mut addr = [0; 16];
        addr[15] = 1;
        let v6 = storage(SockAddrIn6 {
            #[cfg(target_os = "macos")]
            sin6_len: 28,
            sin6_family: AF_INET6,
            sin6_port: 443_u16.to_be(),
            sin6_flowinfo: 0,
            sin6_addr: addr,
            sin6_scope_id: 0,
        });
        assert_eq!(v6.to_socket_addr(28), Some("[::1]:443".parse::<SocketAddr>().unwrap()));
        assert_eq!(SockAddrStorage::EMPTY.to_socket_addr(128), None);
    }
}
