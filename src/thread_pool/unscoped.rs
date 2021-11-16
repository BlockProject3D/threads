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

use crate::thread_pool::core::ThreadManager;
use crate::Join;
use std::thread::JoinHandle;

impl Join for JoinHandle<()> {
    fn join(self) -> std::thread::Result<()> {
        self.join()
    }
}

pub struct UnscopedThreadManager();

impl Default for UnscopedThreadManager {
    fn default() -> Self {
        Self::new()
    }
}

impl UnscopedThreadManager {
    pub fn new() -> Self {
        Self()
    }
}

impl ThreadManager<'static> for UnscopedThreadManager {
    type Handle = JoinHandle<()>;

    fn spawn_thread<F: FnOnce() + Send + 'static>(&self, func: F) -> Self::Handle {
        std::thread::spawn(func)
    }
}

#[cfg(test)]
mod tests {
    use crate::thread_pool::{ThreadPool, UnscopedThreadManager};

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
        let manager = UnscopedThreadManager::new();
        let mut pool: ThreadPool<usize, UnscopedThreadManager> = ThreadPool::new(4);
        for _ in 0..N {
            pool.dispatch(&manager, |_| fibonacci_recursive(20));
        }
        assert!(!pool.is_empty());
        pool.join().unwrap();
        assert!(pool.is_empty());
        let mut tasks = 0;
        while let Some(event) = pool.poll() {
            assert_eq!(event, 6765);
            tasks += 1;
        }
        assert_eq!(tasks, N);
    }
}
