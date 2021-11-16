// Copyright (c) 2021, BlockProject 3D
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//       this list of conditions and the following disclaimer in the documentation
//       and/or other materials provided with the distribution.
//     * Neither the name of BlockProject 3D nor the names of its contributors
//       may be used to endorse or promote products derived from this software
//       without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use super::core::ThreadManager;
use crate::Join;
use crossbeam::thread::{Scope, ScopedJoinHandle};

impl<'a> Join for ScopedJoinHandle<'a, ()> {
    fn join(self) -> std::thread::Result<()> {
        self.join()
    }
}

/// Represents a ScopedThreadManager (to use with crossbeam::scope).
pub struct ScopedThreadManager<'env, 'scope>(&'env Scope<'scope>);

impl<'env, 'scope: 'env> ThreadManager<'scope> for ScopedThreadManager<'env, 'scope> {
    type Handle = ScopedJoinHandle<'env, ()>;

    fn spawn_thread<F: FnOnce() + Send + 'scope>(&self, func: F) -> Self::Handle {
        self.0.spawn(|_| func())
    }
}

impl<'env, 'scope> ScopedThreadManager<'env, 'scope> {
    /// Creates new ScopedThreadManager.
    ///
    /// # Arguments
    ///
    /// * `scope`: the [Scope](crossbeam::thread::Scope) to use.
    ///
    /// returns: ScopedThreadManager
    ///
    /// # Examples
    ///
    /// ```
    /// use bp3d_threads::ThreadPool;
    /// use bp3d_threads::ScopedThreadManager;
    /// crossbeam::scope(|scope| {
    ///     let manager = ScopedThreadManager::new(scope);
    ///     let mut pool: ThreadPool<ScopedThreadManager, i32> = ThreadPool::new(4);
    ///     assert!(pool.is_idle());
    ///     pool.dispatch(&manager, |_| 12);
    ///     assert!(!pool.is_idle());
    ///     pool.join().unwrap();
    ///     assert!(pool.is_idle());
    /// }).unwrap();
    /// ```
    pub fn new(scope: &'env Scope<'scope>) -> Self {
        Self(scope)
    }
}

#[cfg(test)]
mod tests {
    use crate::thread_pool::{ScopedThreadManager, ThreadPool};
    use std::ops::Deref;

    fn fibonacci_recursive(n: usize) -> usize {
        if n == 0 {
            0
        } else if n == 1 {
            1
        } else {
            fibonacci_recursive(n - 1) + fibonacci_recursive(n - 2)
        }
    }

    #[test]
    fn basic() {
        const N: usize = 50;
        let mystr = String::from("This is a test");
        let s = mystr.deref();
        let mut tasks = 0;
        crossbeam::scope(|scope| {
            let manager = ScopedThreadManager::new(scope);
            let mut pool: ThreadPool<ScopedThreadManager, usize> = ThreadPool::new(4);
            for _ in 0..N - 1 {
                pool.dispatch(&manager, |_| fibonacci_recursive(20));
            }
            pool.dispatch(&manager, |_| {
                if s == "This is a test" {
                    fibonacci_recursive(20)
                } else {
                    0
                }
            });
            assert!(!pool.is_idle());
            pool.join().unwrap();
            assert!(pool.is_idle());
            while let Some(event) = pool.poll() {
                assert_eq!(event, 6765);
                tasks += 1;
            }
        })
        .unwrap();
        assert_eq!(tasks, N);
    }
}
