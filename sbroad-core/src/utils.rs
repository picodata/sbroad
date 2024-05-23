use std::cell::{RefCell, RefMut};
use std::io::{Error, Result, Write};
use std::ops::DerefMut;

use tarantool::fiber::mutex::MutexGuard as TMutexGuard;
use tarantool::fiber::Mutex as TMutex;

/// [`MutexLike`] is a mutex abstraction to work with different mutexes in general manner.
///
/// It is needed as sometimes we are unable to use tarantool's [`TMutex`].
/// For instance, if we run ordinary Rust unit tests, tarantool symbols would be missing.
pub trait MutexLike<T> {
    type Guard<'a>: DerefMut<Target = T>
    where
        Self: 'a,
        T: 'a;

    fn lock(&self) -> Self::Guard<'_>;
}

impl<T> MutexLike<T> for TMutex<T> {
    type Guard<'a> = TMutexGuard<'a, T> where T: 'a;

    fn lock(&self) -> Self::Guard<'_> {
        self.lock()
    }
}

impl<T> MutexLike<T> for RefCell<T> {
    type Guard<'a> = RefMut<'a, T>
    where T: 'a;

    fn lock(&self) -> Self::Guard<'_> {
        self.borrow_mut()
    }
}

#[derive(Debug, Default)]
pub struct ByteCounter(usize);

impl ByteCounter {
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.0
    }
}

impl Write for ByteCounter {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct SliceWriter<'b> {
    buf: &'b mut [u8],
    pos: usize,
}

impl SliceWriter<'_> {
    #[must_use]
    pub fn new(buf: &mut [u8]) -> SliceWriter {
        SliceWriter { buf, pos: 0 }
    }
}

impl<'b> Write for SliceWriter<'b> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let len = buf.len();
        if len > self.buf.len() - self.pos {
            return Err(Error::new(
                std::io::ErrorKind::WriteZero,
                format!(
                    "no space left in buffer (position = {}, length = {}, requested = {})",
                    self.pos,
                    self.buf.len(),
                    len
                ),
            ));
        }
        self.buf[self.pos..self.pos + len].copy_from_slice(buf);
        self.pos += len;
        Ok(len)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
