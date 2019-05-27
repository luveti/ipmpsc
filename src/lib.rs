//! Inter-Process Multiple Producer, Single Consumer Channels for Rust
//!
//! This library provides a type-safe, high-performance inter-process
//! channel implementation based on a shared memory ring buffer.  It
//! uses [bincode](https://github.com/TyOverby/bincode) for
//! (de)serialization, including zero-copy deserialization, making it
//! ideal for messages with large `&str` or `&[u8]` fields.  And it
//! has a name that rolls right off the tongue.

#![deny(warnings)]

#[cfg(test)]
#[macro_use]
extern crate serde_derive;

use failure::{format_err, Error};
use memmap::MmapMut;
use serde::{Deserialize, Serialize};
use std::{
    cell::UnsafeCell,
    fs::{File, OpenOptions},
    mem,
    os::raw::c_long,
    sync::{
        atomic::{AtomicU32, Ordering::SeqCst},
        Arc,
    },
    time::{Duration, Instant, SystemTime},
};
use tempfile::NamedTempFile;

const BEGINNING: u32 = mem::size_of::<Header>() as u32;

const DECADE_SECS: u64 = 60 * 60 * 24 * 365 * 10;

// libc::PTHREAD_PROCESS_SHARED doesn't exist for Android for some
// reason, so we need to declare it ourselves:
#[cfg(target_os = "android")]
const PTHREAD_PROCESS_SHARED: i32 = 1;

#[cfg(not(target_os = "android"))]
const PTHREAD_PROCESS_SHARED: i32 = libc::PTHREAD_PROCESS_SHARED;

pub mod error {
    use failure::Fail;

    /// Error indicating that the caller has attempted to read more than
    /// one message from a given [`ZeroCopyContext`](struct.ZeroCopyContext.html).
    #[derive(Fail, Debug)]
    #[fail(display = "A ZeroCopyContext may only be used to receive one message")]
    pub struct AlreadyReceived;

    /// Error indicating that the caller attempted to send a message of
    /// zero serialized size, which is not supported.
    #[derive(Fail, Debug)]
    #[fail(display = "Serialized size of message is zero")]
    pub struct ZeroSizedMessage;

    /// Error indicating that the caller attempted to send a message of
    /// serialized size greater than the ring buffer capacity.
    #[derive(Fail, Debug)]
    #[fail(display = "Serialized size of message is too large for ring buffer")]
    pub struct MessageTooLarge;
}

macro_rules! nonzero {
    ($x:expr) => {{
        let x = $x;
        if x == 0 {
            Ok(())
        } else {
            Err(format_err!("{} failed: {}", stringify!($x), x))
        }
    }};
}

#[repr(C)]
struct Header {
    mutex: UnsafeCell<libc::pthread_mutex_t>,
    condition: UnsafeCell<libc::pthread_cond_t>,
    read: AtomicU32,
    write: AtomicU32,
}

impl Header {
    fn init(&self) -> Result<(), Error> {
        unsafe {
            // TODO: replace mem::uninitialized with MaybeUninit once
            // Rust 1.36 is released (see
            // https://gankro.github.io/blah/initialize-me-maybe/ for
            // why)

            let mut attr = mem::uninitialized::<libc::pthread_mutexattr_t>();
            nonzero!(libc::pthread_mutexattr_init(&mut attr))?;
            nonzero!(libc::pthread_mutexattr_setpshared(
                &mut attr,
                PTHREAD_PROCESS_SHARED
            ))?;
            nonzero!(libc::pthread_mutex_init(self.mutex.get(), &attr))?;
            nonzero!(libc::pthread_mutexattr_destroy(&mut attr))?;

            let mut attr = mem::uninitialized::<libc::pthread_condattr_t>();
            nonzero!(libc::pthread_condattr_init(&mut attr))?;
            nonzero!(libc::pthread_condattr_setpshared(
                &mut attr,
                PTHREAD_PROCESS_SHARED
            ))?;
            nonzero!(libc::pthread_cond_init(self.condition.get(), &attr))?;
            nonzero!(libc::pthread_condattr_destroy(&mut attr))?;
        }

        self.read.store(BEGINNING, SeqCst);
        self.write.store(BEGINNING, SeqCst);

        Ok(())
    }

    fn lock(&self) -> Result<Lock, Error> {
        unsafe {
            nonzero!(libc::pthread_mutex_lock(self.mutex.get()))?;
        }
        Ok(Lock(self))
    }

    fn notify_all(&self) -> Result<(), Error> {
        unsafe { nonzero!(libc::pthread_cond_broadcast(self.condition.get())) }
    }
}

struct Lock<'a>(&'a Header);

impl<'a> Lock<'a> {
    fn wait(&self) -> Result<(), Error> {
        unsafe {
            nonzero!(libc::pthread_cond_wait(
                self.0.condition.get(),
                self.0.mutex.get()
            ))
        }
    }

    #[allow(clippy::cast_lossless)]
    fn timed_wait(&self, timeout: Duration) -> Result<(), Error> {
        let then = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            + timeout;

        let then = libc::timespec {
            tv_sec: then.as_secs() as libc::time_t,
            tv_nsec: then.subsec_nanos() as c_long,
        };

        let timeout_ok = |result| if result == libc::ETIMEDOUT { 0 } else { result };

        unsafe {
            nonzero!(timeout_ok(libc::pthread_cond_timedwait(
                self.0.condition.get(),
                self.0.mutex.get(),
                &then
            )))
        }
    }
}

impl<'a> Drop for Lock<'a> {
    fn drop(&mut self) {
        unsafe {
            libc::pthread_mutex_unlock(self.0.mutex.get());
        }
    }
}

/// Represents the receiving end of an inter-process channel, capable
/// of receiving any message type implementing
/// [`serde::Deserialize`](https://docs.serde.rs/serde/trait.Deserialize.html).
pub struct Receiver {
    map: MmapMut,
    _file: Option<NamedTempFile>,
}

/// Borrows a [`Receiver`](struct.Receiver.html) for the purpose of
/// doing zero-copy deserialization of messages containing references.
///
/// An instance of this type may only be used to deserialize a single
/// message before it is dropped because the
/// [`Drop`](https://doc.rust-lang.org/std/ops/trait.Drop.html)
/// implementation is what advances the ring buffer pointer.  Also,
/// the borrowed [`Receiver`](struct.Receiver.html) may not be used
/// directly while it is borrowed by a
/// [`ZeroCopyContext`](struct.ZeroCopyContext.html).
///
/// Use
/// [`Receiver::zero_copy_context`](struct.Receiver.html#method.zero_copy_context)
/// to create an instance.
pub struct ZeroCopyContext<'a> {
    receiver: &'a Receiver,
    position: Option<u32>,
}

impl<'a> ZeroCopyContext<'a> {
    /// Attempt to read a message without blocking.
    ///
    /// This will return `Ok(None)` if there are no messages
    /// immediately available.  It will return
    /// `Err(Error::from(`[`error::AlreadyReceived`](error/struct.AlreadyReceived.html)`))`
    /// if this instance has already been used to read a message.
    pub fn try_recv<'b, T: Deserialize<'b>>(&'b mut self) -> Result<Option<T>, Error> {
        if self.position.is_some() {
            Err(Error::from(error::AlreadyReceived))
        } else {
            Ok(
                if let Some((value, position)) = self.receiver.try_recv_0()? {
                    self.position = Some(position);
                    Some(value)
                } else {
                    None
                },
            )
        }
    }

    /// Attempt to read a message, blocking if necessary until one
    /// becomes available.
    ///
    /// This will return
    /// `Err(Error::from(`[`error::AlreadyReceived`](error/struct.AlreadyReceived.html)`))`
    /// if this instance has already been used to read a message.
    pub fn recv<'b, T: Deserialize<'b>>(&'b mut self) -> Result<T, Error> {
        self.recv_timeout(Duration::from_secs(DECADE_SECS))
            .map(Option::unwrap)
    }

    /// Attempt to read a message, blocking for up to the specified
    /// duration if necessary until one becomes available.
    ///
    /// This will return
    /// `Err(Error::from(`[`error::AlreadyReceived`](error/struct.AlreadyReceived.html)`))`
    /// if this instance has already been used to read a message.
    pub fn recv_timeout<'b, T: Deserialize<'b>>(
        &'b mut self,
        timeout: Duration,
    ) -> Result<Option<T>, Error> {
        if self.position.is_some() {
            Err(Error::from(error::AlreadyReceived))
        } else {
            Ok(
                if let Some((value, position)) = self.receiver.recv_timeout_0(timeout)? {
                    self.position = Some(position);
                    Some(value)
                } else {
                    None
                },
            )
        }
    }
}

impl<'a> Drop for ZeroCopyContext<'a> {
    fn drop(&mut self) {
        if let Some(position) = self.position.take() {
            let _ = self.receiver.seek(position);
        }
    }
}

impl Receiver {
    /// Creates a new [`Receiver`](struct.Receiver.html) backed by a file with the specified
    /// name.
    ///
    /// The file will be created if it does not already exist or
    /// truncated otherwise.  Once this method has returned
    /// successfully, any number of senders may be created using the
    /// [`Sender::from_path`](struct.Sender.html#method.from_path)
    /// method.
    pub fn from_path(path: &str, size_in_bytes: u32) -> Result<Receiver, Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        file.set_len(u64::from(BEGINNING + size_in_bytes))?;

        Ok(Receiver {
            map: map(&file)?,
            _file: None,
        })
    }

    /// Creates a new [`Receiver`](struct.Receiver.html) backed by a
    /// temporary file which will be deleted when the
    /// [`Receiver`](struct.Receiver.html) is dropped.
    ///
    /// The name of the file is returned along with the
    /// [`Receiver`](struct.Receiver.html) and may be used to create
    /// one or more corresponding senders using the
    /// [`Sender::from_path`](struct.Sender.html#method.from_path)
    /// method.
    pub fn temp_file(size_in_bytes: u32) -> Result<(String, Receiver), Error> {
        let file = NamedTempFile::new()?;

        file.as_file()
            .set_len(u64::from(BEGINNING + size_in_bytes))?;

        Ok((
            file.path()
                .to_str()
                .ok_or_else(|| format_err!("unable to represent path as string"))?
                .to_owned(),
            Receiver {
                map: map(file.as_file())?,
                _file: Some(file),
            },
        ))
    }

    fn header(&self) -> &Header {
        #[allow(clippy::cast_ptr_alignment)]
        unsafe {
            &*(self.map.as_ptr() as *const Header)
        }
    }

    fn seek(&self, position: u32) -> Result<(), Error> {
        let header = self.header();
        let _lock = header.lock()?;
        header.read.store(position, SeqCst);
        header.notify_all()
    }

    /// Attempt to read a message without blocking.
    ///
    /// This will return `Ok(None)` if there are no messages
    /// immediately available.
    pub fn try_recv<T>(&self) -> Result<Option<T>, Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        Ok(if let Some((value, position)) = self.try_recv_0()? {
            self.seek(position)?;

            Some(value)
        } else {
            None
        })
    }

    fn try_recv_0<'a, T: Deserialize<'a>>(&'a self) -> Result<Option<(T, u32)>, Error> {
        let header = self.header();

        let mut read = header.read.load(SeqCst);
        let write = header.write.load(SeqCst);

        Ok(loop {
            if write != read {
                let buffer = self.map.as_ref();
                let start = read + 4;
                let size = bincode::deserialize::<u32>(&buffer[read as usize..start as usize])?;
                if size > 0 {
                    let end = start + size;
                    break Some((
                        bincode::deserialize(&buffer[start as usize..end as usize])?,
                        end,
                    ));
                } else if write < read {
                    read = BEGINNING;
                    let _lock = header.lock()?;
                    header.read.store(read, SeqCst);
                    header.notify_all()?;
                } else {
                    return Err(format_err!("corrupt ring buffer"));
                }
            } else {
                break None;
            }
        })
    }

    /// Attempt to read a message, blocking if necessary until one
    /// becomes available.
    pub fn recv<T>(&self) -> Result<T, Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.recv_timeout(Duration::from_secs(DECADE_SECS))
            .map(Option::unwrap)
    }

    /// Attempt to read a message, blocking for up to the specified
    /// duration if necessary until one becomes available.
    pub fn recv_timeout<T>(&self, timeout: Duration) -> Result<Option<T>, Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        Ok(
            if let Some((value, position)) = self.recv_timeout_0(timeout)? {
                self.seek(position)?;

                Some(value)
            } else {
                None
            },
        )
    }

    /// Borrows this receiver for deserializing a message with
    /// references that refer directly to this
    /// [`Receiver`](struct.Receiver.html)'s ring buffer rather than
    /// copying out of it.
    ///
    /// Because those references refer directly to the ring buffer,
    /// the read pointer cannot be advanced until the lifetime of
    /// those references ends.
    ///
    /// To ensure the above, the following rules apply:
    ///
    /// 1. The underlying [`Receiver`](struct.Receiver.html) cannot be
    /// used while a [`ZeroCopyContext`](struct.ZeroCopyContext.html)
    /// borrows it (enforced at compile time).
    ///
    /// 2. References in a message deserialized using a given
    /// [`ZeroCopyContext`](struct.ZeroCopyContext.html) cannot
    /// outlive that instance (enforced at compile time).
    ///
    /// 3. A given [`ZeroCopyContext`](struct.ZeroCopyContext.html)
    /// can only be used to deserialize a single message before it
    /// must be discarded since the read pointer is advanced only when
    /// the instance is dropped (enforced at run time).
    pub fn zero_copy_context(&mut self) -> ZeroCopyContext {
        ZeroCopyContext {
            receiver: self,
            position: None,
        }
    }

    fn recv_timeout_0<'a, T: Deserialize<'a>>(
        &'a self,
        timeout: Duration,
    ) -> Result<Option<(T, u32)>, Error> {
        let mut deadline = None;
        loop {
            if let Some(value_and_position) = self.try_recv_0()? {
                return Ok(Some(value_and_position));
            }

            let header = self.header();

            let mut now = Instant::now();
            deadline = deadline.or_else(|| Some(now + timeout));

            let read = header.read.load(SeqCst);

            let lock = header.lock()?;
            while read == header.write.load(SeqCst) {
                let deadline = deadline.unwrap();
                if deadline > now {
                    lock.timed_wait(deadline - now)?;
                    now = Instant::now();
                } else {
                    return Ok(None);
                }
            }
        }
    }
}

fn map(file: &File) -> Result<MmapMut, Error> {
    unsafe {
        let map = MmapMut::map_mut(&file)?;

        #[allow(clippy::cast_ptr_alignment)]
        (*(map.as_ptr() as *const Header)).init()?;

        Ok(map)
    }
}

/// Represents the sending end of an inter-process channel.
#[derive(Clone)]
pub struct Sender {
    map: Arc<UnsafeCell<MmapMut>>,
}

unsafe impl Sync for Sender {}

unsafe impl Send for Sender {}

impl Sender {
    /// Creates a new [`Sender`](struct.Sender.html) backed by a file with
    /// the specified name.
    ///
    /// The file must already exist and have been initialized by a
    /// call to
    /// [`Receiver::temp_file`](struct.Receiver.html#method.temp_file)
    /// or
    /// [`Receiver::from_path`](struct.Receiver.html#method.from_path).
    /// Any number of senders may be created for a given receiver,
    /// allowing multiple processes to send messages simultaneously to
    /// that receiver.
    ///
    /// When creating multiple [`Sender`](struct.Sender.html)s for a
    /// given [`Receiver`](struct.Receiver.html) in a single process,
    /// it is much more efficient to use a single `from_path` call and
    /// `clone` the resulting [`Sender`](struct.Sender.html) than it
    /// is to make multiple calls to `from_path`.
    pub fn from_path(path: &str) -> Result<Sender, Error> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let map = unsafe { MmapMut::map_mut(&file)? };

        Ok(Sender {
            map: Arc::new(UnsafeCell::new(map)),
        })
    }

    /// Send the specified message, waiting for sufficient contiguous
    /// space to become available in the ring buffer if necessary.
    ///
    /// The serialized size of the message must be greater than zero
    /// or else this method will return
    /// `Err(Error::from(`[`error::ZeroSizedMessage`](error/struct.ZeroSizedMessage.html)`))`.
    /// If the serialized size is greater than the ring buffer
    /// capacity, this method will return
    /// `Err(Error::from(`[`error::MessageTooLarge`](error/struct.MessageTooLarge.html)`))`.
    pub fn send(&self, value: &impl Serialize) -> Result<(), Error> {
        self.send_0(value, false)
    }

    /// Send the specified message, waiting for the ring buffer to
    /// become completely empty first.
    ///
    /// This method is appropriate for sending time-sensitive messages
    /// where buffering would introduce undesirable latency.
    ///
    /// The serialized size of the message must be greater than zero
    /// or else this method will return
    /// `Err(Error::from(`[`error::ZeroSizedMessage`](error/struct.ZeroSizedMessage.html)`))`.
    /// If the serialized size is greater than the ring buffer
    /// capacity, this method will return
    /// `Err(Error::from(`[`error::MessageTooLarge`](error/struct.MessageTooLarge.html)`))`.
    pub fn send_when_empty(&self, value: &impl Serialize) -> Result<(), Error> {
        self.send_0(value, true)
    }

    fn send_0(&self, value: &impl Serialize, wait_until_empty: bool) -> Result<(), Error> {
        #[allow(clippy::cast_ptr_alignment)]
        let header = unsafe { &*((*self.map.get()).as_ptr() as *const Header) };

        let size = bincode::serialized_size(value)? as u32;

        if size == 0 {
            return Err(Error::from(error::ZeroSizedMessage));
        }

        let map_len = unsafe { (*self.map.get()).len() };

        if (BEGINNING + size + 8) as usize > map_len {
            return Err(Error::from(error::MessageTooLarge));
        }

        let lock = header.lock()?;
        let mut write = header.write.load(SeqCst);
        loop {
            let read = header.read.load(SeqCst);

            if write == read || (write > read && !wait_until_empty) {
                if (write + size + 8) as usize <= map_len {
                    break;
                } else if read != BEGINNING {
                    assert!(write > BEGINNING);

                    unsafe {
                        bincode::serialize_into(
                            &mut (*self.map.get())[write as usize..(write + 4) as usize],
                            &0_u32,
                        )?;
                    }
                    write = BEGINNING;
                    header.write.store(write, SeqCst);
                    header.notify_all()?;
                    continue;
                }
            } else if write + size + 8 <= read && !wait_until_empty {
                break;
            }

            lock.wait()?;
        }

        let start = write + 4;
        unsafe {
            bincode::serialize_into(
                &mut (*self.map.get())[write as usize..start as usize],
                &size,
            )?;
        }

        let end = start + size;
        unsafe {
            bincode::serialize_into(&mut (*self.map.get())[start as usize..end as usize], value)?;
        }

        header.write.store(end, SeqCst);
        header.notify_all()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{arbitrary::any, collection::vec, prop_assume, proptest, strategy::Strategy};
    use std::thread;

    #[derive(Debug)]
    struct Case {
        channel_size: u32,
        data: Vec<Vec<u8>>,
    }

    impl Case {
        fn run(&self) -> Result<(), Error> {
            let (name, rx) = Receiver::temp_file(self.channel_size)?;

            let expected = self.data.clone();
            let receiver_thread = thread::spawn(move || -> Result<(), Error> {
                for item in &expected {
                    let received = rx.recv::<Vec<u8>>()?;
                    assert_eq!(item, &received);
                }

                Ok(())
            });

            let tx = Sender::from_path(&name)?;

            for item in &self.data {
                tx.send(item)?;
            }

            receiver_thread
                .join()
                .map_err(|e| format_err!("{:?}", e))??;

            Ok(())
        }
    }

    fn arb_case() -> impl Strategy<Value = Case> {
        (32_u32..1024).prop_flat_map(|channel_size| {
            vec(vec(any::<u8>(), 0..(channel_size as usize - 24)), 1..1024)
                .prop_map(move |data| Case { channel_size, data })
        })
    }

    #[test]
    fn simple_case() -> Result<(), Error> {
        Case {
            channel_size: 1024,
            data: (0..1024)
                .map(|_| (0_u8..101).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
        }
        .run()
    }

    #[test]
    fn zero_copy() -> Result<(), Error> {
        #[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
        struct Foo<'a> {
            borrowed_str: &'a str,
            #[serde(with = "serde_bytes")]
            borrowed_bytes: &'a [u8],
        }

        let sent = Foo {
            borrowed_str: "hi",
            borrowed_bytes: &[0, 1, 2, 3],
        };

        let (name, mut rx) = Receiver::temp_file(256)?;
        let tx = Sender::from_path(&name)?;

        tx.send(&sent)?;
        tx.send(&42_u32)?;

        {
            let mut rx = rx.zero_copy_context();
            let received = rx.recv()?;

            assert_eq!(sent, received);
        }

        assert_eq!(42_u32, rx.recv()?);

        Ok(())
    }

    proptest! {
        #[test]
        fn arbitrary_case(case in arb_case()) {
            let result = case.run();
            prop_assume!(result.is_ok(), "error: {:?}", result.unwrap_err());
        }
    }
}
