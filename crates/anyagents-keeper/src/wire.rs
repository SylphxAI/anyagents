//! The kernel↔keeper wire: one JSON object per line over a Unix stream
//! socket, and descriptor passing (`SCM_RIGHTS`) for the agent's process lock.
//!
//! A frame is complete only at its newline. A peer that disappears in the
//! middle of a frame leaves bytes without one, and those bytes are dropped:
//! a half-sent request is never acted on.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The largest frame either side accepts. A harness answer is small; a
/// delivered message is at most a mailbox body.
pub const MAX_FRAME: usize = 64 << 20;

/// A request from the kernel. `args` is the op's argument object.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    pub op: String,
    #[serde(default)]
    pub args: Value,
}

/// Why a request was not answered with a value.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Fault {
    /// The adapter's own error, carried across unchanged.
    Harness {
        error: anyagents_harness::HarnessError,
    },
    /// The keeper does not know this op: the caller uses the contract's
    /// default for it. This is how the op set grows without a protocol bump.
    UnknownOp,
    /// No protocol version both sides speak. Only `stop` is still accepted.
    Protocol { keeper: (u32, u32) },
    /// The request could not be read.
    BadRequest { message: String },
    /// The adapter is gone: the keeper is stopping.
    Stopped,
    /// This connection may not do that: its peer is another user, or it is
    /// not the keeper's current owner. The keeper is alive.
    Forbidden { message: String },
    /// The keeper cannot serve this connection now (it could not start a
    /// thread for it). The keeper is alive.
    Busy,
}

/// One line from the keeper: the answer to a request, or an event.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Reply {
    Ok { id: u64, ok: Value },
    Err { id: u64, err: Fault },
    Event { event: Event },
}

/// What the keeper says without being asked.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Event {
    /// Something the kernel reads through the harness changed.
    Activity,
    /// A newer kernel took this keeper over; this connection ends.
    Superseded,
}

/// Reads frames, keeping any descriptors that arrived with them.
pub struct FrameReader {
    fd: RawFd,
    buf: Vec<u8>,
    fds: Vec<OwnedFd>,
}

impl FrameReader {
    /// Read from `stream`, which must outlive the reader.
    pub fn new(stream: &UnixStream) -> Self {
        Self {
            fd: stream.as_raw_fd(),
            buf: Vec::new(),
            fds: Vec::new(),
        }
    }

    /// The next complete frame, or `None` at end of stream. Bytes after the
    /// last newline at end of stream are a frame the peer never finished.
    pub fn next_frame(&mut self) -> io::Result<Option<Vec<u8>>> {
        let mut scanned = 0;
        loop {
            if let Some(end) = self.buf[scanned..].iter().position(|b| *b == b'\n') {
                let end = scanned + end;
                let frame = self.buf[..end].to_vec();
                self.buf.drain(..=end);
                return Ok(Some(frame));
            }
            scanned = self.buf.len();
            if self.buf.len() > MAX_FRAME {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "frame larger than the wire allows",
                ));
            }
            let mut chunk = [0u8; 16 * 1024];
            let read = recv_with_fds(self.fd, &mut chunk, &mut self.fds)?;
            if read == 0 {
                self.buf.clear();
                return Ok(None);
            }
            self.buf.extend_from_slice(&chunk[..read]);
        }
    }

    /// Take the descriptors received so far, oldest first.
    pub fn take_fds(&mut self) -> Vec<OwnedFd> {
        std::mem::take(&mut self.fds)
    }
}

/// Write one frame, with `fds` attached to its first byte.
pub fn send_frame(stream: &UnixStream, value: &impl Serialize, fds: &[RawFd]) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    bytes.push(b'\n');
    let mut sent = send_with_fds(stream.as_raw_fd(), &bytes, fds)?;
    while sent < bytes.len() {
        sent += send_with_fds(stream.as_raw_fd(), &bytes[sent..], &[])?;
    }
    Ok(())
}

/// Never raise `SIGPIPE` for a write to a peer that left: the error is the
/// answer, in the kernel, the keeper and a CLI alike.
pub fn quiet_pipe(stream: &UnixStream) -> io::Result<()> {
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    {
        let on: libc::c_int = 1;
        // SAFETY: a valid socket descriptor and a c_int option value.
        let rc = unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_NOSIGPIPE,
                (&on as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
    let _ = stream;
    Ok(())
}

#[cfg(target_os = "linux")]
const SEND_FLAGS: libc::c_int = libc::MSG_NOSIGNAL;
#[cfg(not(target_os = "linux"))]
const SEND_FLAGS: libc::c_int = 0;

#[cfg(target_os = "linux")]
const RECV_FLAGS: libc::c_int = libc::MSG_CMSG_CLOEXEC;
#[cfg(not(target_os = "linux"))]
const RECV_FLAGS: libc::c_int = 0;

/// Room for a handful of descriptors, aligned for `cmsghdr`.
#[repr(C, align(8))]
struct ControlBuffer([u8; 128]);

fn send_with_fds(fd: RawFd, bytes: &[u8], fds: &[RawFd]) -> io::Result<usize> {
    let mut iov = libc::iovec {
        iov_base: bytes.as_ptr() as *mut libc::c_void,
        iov_len: bytes.len(),
    };
    let mut control = ControlBuffer([0; 128]);
    // SAFETY: an all-zero msghdr is a valid empty message.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    if !fds.is_empty() {
        let payload = std::mem::size_of_val(fds);
        // SAFETY: CMSG_SPACE only computes a size.
        let space = unsafe { libc::CMSG_SPACE(payload as u32) } as usize;
        if space > control.0.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many descriptors for one frame",
            ));
        }
        msg.msg_control = control.0.as_mut_ptr().cast();
        msg.msg_controllen = space as _;
        // SAFETY: the control buffer holds one header of `space` bytes, and
        // CMSG_FIRSTHDR/CMSG_DATA point inside it.
        unsafe {
            let header = libc::CMSG_FIRSTHDR(&msg);
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = libc::CMSG_LEN(payload as u32) as _;
            std::ptr::copy_nonoverlapping(
                fds.as_ptr().cast::<u8>(),
                libc::CMSG_DATA(header),
                payload,
            );
        }
    }
    loop {
        // SAFETY: `msg` describes live buffers for the duration of the call.
        let sent = unsafe { libc::sendmsg(fd, &msg, SEND_FLAGS) };
        if sent >= 0 {
            return Ok(sent as usize);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn recv_with_fds(fd: RawFd, buf: &mut [u8], fds: &mut Vec<OwnedFd>) -> io::Result<usize> {
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    let mut control = ControlBuffer([0; 128]);
    // SAFETY: an all-zero msghdr is a valid empty message.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.0.as_mut_ptr().cast();
    msg.msg_controllen = control.0.len() as _;
    let read = loop {
        // SAFETY: `msg` describes live buffers for the duration of the call.
        let read = unsafe { libc::recvmsg(fd, &mut msg, RECV_FLAGS) };
        if read >= 0 {
            break read as usize;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    };
    // SAFETY: the kernel filled `msg_controllen` bytes of the control buffer;
    // the CMSG_* walk stays inside it.
    unsafe {
        let mut header = libc::CMSG_FIRSTHDR(&msg);
        while !header.is_null() {
            if (*header).cmsg_level == libc::SOL_SOCKET && (*header).cmsg_type == libc::SCM_RIGHTS {
                let data = libc::CMSG_DATA(header).cast::<RawFd>();
                let bytes = (*header).cmsg_len as usize - libc::CMSG_LEN(0) as usize;
                for index in 0..bytes / std::mem::size_of::<RawFd>() {
                    let raw = data.add(index).read_unaligned();
                    let owned = OwnedFd::from_raw_fd(raw);
                    #[cfg(not(target_os = "linux"))]
                    set_cloexec(raw)?;
                    fds.push(owned);
                }
            }
            header = libc::CMSG_NXTHDR(&msg, header);
        }
    }
    if msg.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "descriptors were cut from a frame",
        ));
    }
    Ok(read)
}

/// Mark `fd` close-on-exec, so a harness never inherits it.
pub fn set_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: fcntl on a descriptor this process owns.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: as above.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The user id of the process at the other end of `stream`.
pub fn peer_uid(stream: &UnixStream) -> io::Result<u32> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: an all-zero ucred is valid for getsockopt to fill.
        let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        // SAFETY: `cred` and `len` are valid for the call.
        let rc = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut cred as *mut libc::ucred).cast(),
                &mut len,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(cred.uid)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let mut uid: libc::uid_t = 0;
        let mut gid: libc::gid_t = 0;
        // SAFETY: valid out pointers for getpeereid.
        if unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(uid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frame_cut_off_by_the_peer_is_dropped() {
        let (left, right) = UnixStream::pair().unwrap();
        use std::io::Write as _;
        (&left).write_all(b"{\"a\":1}\n{\"half\":").unwrap();
        drop(left);
        let mut reader = FrameReader::new(&right);
        assert_eq!(reader.next_frame().unwrap().unwrap(), b"{\"a\":1}");
        assert!(reader.next_frame().unwrap().is_none());
    }

    #[test]
    fn a_descriptor_travels_with_its_frame() {
        let (left, right) = UnixStream::pair().unwrap();
        let file = tempfile::tempfile().unwrap();
        send_frame(
            &left,
            &serde_json::json!({"lock": true}),
            &[file.as_raw_fd()],
        )
        .unwrap();
        let mut reader = FrameReader::new(&right);
        assert_eq!(reader.next_frame().unwrap().unwrap(), b"{\"lock\":true}");
        let fds = reader.take_fds();
        assert_eq!(fds.len(), 1);
        // SAFETY: fcntl F_GETFD on a received, owned descriptor.
        let flags = unsafe { libc::fcntl(fds[0].as_raw_fd(), libc::F_GETFD) };
        assert!(
            flags & libc::FD_CLOEXEC != 0,
            "a received lock leaks to children"
        );
    }

    #[test]
    fn the_peer_is_this_user() {
        let (left, _right) = UnixStream::pair().unwrap();
        // SAFETY: geteuid takes no arguments and cannot fail.
        assert_eq!(peer_uid(&left).unwrap(), unsafe { libc::geteuid() });
    }
}
