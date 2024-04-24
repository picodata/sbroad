use std::cell::{RefCell, RefMut};
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
