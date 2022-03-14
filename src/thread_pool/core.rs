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

//! A thread pool with support for function results

use std::iter::repeat_with;
use std::sync::Arc;
use std::time::Duration;
use std::vec::IntoIter;
use crossbeam::deque::{Injector, Stealer, Worker};
use crossbeam::queue::{ArrayQueue, SegQueue};

const INNER_RESULT_BUFFER: usize = 16;

struct Task<'env, T: Send + 'static> {
    func: Box<dyn FnOnce(usize) -> T + Send + 'env>,
    id: usize,
}

struct WorkThread<'env, T: Send + 'static>
{
    id: usize,
    worker: Worker<Task<'env, T>>,
    task_queue: Arc<Injector<Task<'env, T>>>,
    task_stealers: Box<[Option<Stealer<Task<'env, T>>>]>,
    term_queue: Arc<ArrayQueue<usize>>,
    end_queue: Arc<SegQueue<Vec<T>>>
}

impl<'env, T: Send + 'static> WorkThread<'env, T>
{
    pub fn new(id: usize, task_queue: Arc<Injector<Task<'env, T>>>,
               worker: Worker<Task<'env, T>>,
               task_stealers: Box<[Option<Stealer<Task<'env, T>>>]>,
               term_queue: Arc<ArrayQueue<usize>>,
               end_queue: Arc<SegQueue<Vec<T>>>) -> WorkThread<'env, T>
    {
        WorkThread {
            id,
            worker,
            task_queue,
            task_stealers,
            term_queue,
            end_queue
        }
    }

    fn attempt_steal_task(&self) -> Option<Task<'env, T>> {
        self.worker.pop().or_else(|| {
            std::iter::repeat_with(|| {
                self.task_queue.steal_batch_and_pop(&self.worker).or_else(|| {
                    self.task_stealers.iter()
                        .filter_map(|v| {
                            if let Some(v) = v {
                                Some(v)
                            } else {
                                None
                            }
                        })
                        .map(|v| v.steal_batch_and_pop(&self.worker))
                        .collect()
                })
            }).find(|v| !v.is_retry()).and_then(|v| v.success())
        })
    }

    fn empty_inner_buffer(&self, mut inner: Vec<T>) -> Vec<T>
    {
        if inner.len() > 0 {
            let buffer = std::mem::replace(&mut inner, Vec::with_capacity(INNER_RESULT_BUFFER));
            self.end_queue.push(buffer);
        }
        inner
    }

    fn check_empty_inner_buffer(&self, mut inner: Vec<T>) -> Vec<T> {
        if inner.len() >= INNER_RESULT_BUFFER {
            inner = self.empty_inner_buffer(inner);
        }
        inner
    }

    fn iteration(&self)
    {
        let mut inner = Vec::with_capacity(INNER_RESULT_BUFFER);
        while let Some(task) = self.attempt_steal_task() {
            let res = (task.func)(task.id);
            inner.push(res);
            inner = self.check_empty_inner_buffer(inner);
        }
        self.empty_inner_buffer(inner);
    }

    fn main_loop(&self)
    {
        self.iteration();
        /*if self.error_flag.get() {
            self.term_channel_in.send(self.id).unwrap();
            return;
        }*/
        // Wait 100ms and give another try before shutting down to let a chance to the main thread to refill the task channel.
        std::thread::sleep(Duration::from_millis(100));
        self.iteration();
        self.term_queue.push(self.id).unwrap();
    }
}

/// Trait to access the join function of a thread handle.
pub trait Join {
    /// Joins this thread.
    fn join(self) -> std::thread::Result<()>;
}

/// Trait to handle spawning generic threads.
pub trait ThreadManager<'env> {
    /// The type of thread handle (must have a join() function).
    type Handle: Join;

    /// Spawns a thread using this manager.
    ///
    /// # Arguments
    ///
    /// * `func`: the function to run in the thread.
    ///
    /// returns: Self::Handle
    fn spawn_thread<F: FnOnce() + Send + 'env>(&self, func: F) -> Self::Handle;
}

struct Inner<'env, M: ThreadManager<'env>, T: Send + 'static> {
    end_queue: Arc<SegQueue<Vec<T>>>,
    threads: Box<[Option<M::Handle>]>,
    task_stealers: Box<[Option<Stealer<Task<'env, T>>>]>,
    term_queue: Arc<ArrayQueue<usize>>,
    running_threads: usize,
    n_threads: usize
}

/// An iterator into a thread pool.
pub struct Iter<'a, 'env, M: ThreadManager<'env>, T: Send + 'static> {
    inner: &'a mut Inner<'env, M, T>,
    batch: Option<IntoIter<T>>,
    thread_id: usize
}

impl<'a, 'env, M: ThreadManager<'env>, T: Send + 'static> Iter<'a, 'env, M, T> {
    fn pump_next_batch(&mut self) -> Option<std::thread::Result<()>> {
        while self.batch.is_none() {
            if self.inner.running_threads == 0 {
                return None;
            }
            if let Some(h) = self.inner.threads[self.thread_id].take() {
                if let Err(e) = h.join() {
                    return Some(Err(e));
                }
                self.inner.term_queue.pop();
                self.inner.running_threads -= 1;
                while let Some(batch) = self.inner.end_queue.pop() {
                    self.batch = Some(batch.into_iter());
                }
            }
            self.inner.task_stealers[self.thread_id] = None;
            self.thread_id += 1;
        }
        Some(Ok(()))
    }
}

impl<'a, 'env, M: ThreadManager<'env>, T: Send + 'static> Iter<'a, 'env, M, Vec<T>> {
    /// Collect this iterator into a single [Vec](std::vec::Vec) when each task returns a
    /// [Vec](std::vec::Vec).
    pub fn to_vec(mut self) -> std::thread::Result<Vec<T>> {
        let mut v = Vec::new();
        for i in 0..self.inner.n_threads {
            if let Some(h) = self.inner.threads[i].take() {
                h.join()?;
                self.inner.term_queue.pop();
                self.inner.running_threads -= 1;
                while let Some(batch) = self.inner.end_queue.pop() {
                    for r in batch {
                        v.extend(r);
                    }
                }
            }
            self.inner.task_stealers[i] = None;
        }
        Ok(v)
    }
}

impl<'a, 'env, M: ThreadManager<'env>, T: Send + 'static, E: Send + 'static> Iter<'a, 'env, M, Result<Vec<T>, E>> {
    /// Collect this iterator into a single [Result](std::result::Result) of [Vec](std::vec::Vec)
    /// when each task returns a [Result](std::result::Result) of [Vec](std::vec::Vec).
    pub fn to_vec(mut self) -> std::thread::Result<Result<Vec<T>, E>> {
        let mut v = Vec::new();
        for i in 0..self.inner.n_threads {
            if let Some(h) = self.inner.threads[i].take() {
                h.join()?;
                self.inner.term_queue.pop();
                self.inner.running_threads -= 1;
                while let Some(batch) = self.inner.end_queue.pop() {
                    for r in batch {
                        match r {
                            Ok(items) => v.extend(items),
                            Err(e) => return Ok(Err(e))
                        }
                    }
                }
            }
            self.inner.task_stealers[i] = None;
        }
        Ok(Ok(v))
    }
}

impl<'a, 'env, M: ThreadManager<'env>, T: Send + 'static> Iterator for Iter<'a, 'env, M, T> {
    type Item = std::thread::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.pump_next_batch() {
            None => return None,
            Some(v) => match v {
                Ok(_) => (),
                Err(e) => return Some(Err(e))
            }
        };
        // SAFETY: always safe because while self.batch.is_none(). So if this is reached then
        // batch has to be Some.
        let batch = unsafe { self.batch.as_mut().unwrap_unchecked() };
        let item = batch.next();
        match item {
            None => {
                self.batch = None;
                self.next()
            },
            Some(v) => Some(Ok(v))
        }
    }
}

/// Core thread pool.
pub struct ThreadPool<'env, M: ThreadManager<'env>, T: Send + 'static> {
    task_queue: Arc<Injector<Task<'env, T>>>,
    end_batch: Option<Vec<T>>,
    inner: Inner<'env, M, T>,
    task_id: usize,
}

impl<'env, M: ThreadManager<'env>, T: Send> ThreadPool<'env, M, T> {
    /// Creates a new thread pool
    ///
    /// # Arguments
    ///
    /// * `n_threads`: maximum number of threads allowed to run at the same time.
    ///
    /// returns: ThreadPool<T, Manager>
    ///
    /// # Examples
    ///
    /// ```
    /// use bp3d_threads::UnscopedThreadManager;
    /// use bp3d_threads::ThreadPool;
    /// let _: ThreadPool<UnscopedThreadManager, ()> = ThreadPool::new(4);
    /// ```
    pub fn new(n_threads: usize) -> Self {
        Self {
            task_queue: Arc::new(Injector::new()),
            inner: Inner {
                task_stealers: vec![None; n_threads].into_boxed_slice(),
                end_queue: Arc::new(SegQueue::new()),
                term_queue: Arc::new(ArrayQueue::new(n_threads)),
                n_threads,
                running_threads: 0,
                threads: repeat_with(|| None)
                    .take(n_threads)
                    .collect::<Vec<Option<M::Handle>>>()
                    .into_boxed_slice(),
            },
            end_batch: None,
            task_id: 0,
        }
    }

    fn rearm_one_thread_if_possible(&mut self, manager: &M) {
        if self.inner.running_threads < self.inner.n_threads {
            for (i, handle) in self.inner.threads.iter_mut().enumerate() {
                if handle.is_none() {
                    let worker = Worker::new_fifo();
                    let stealer = worker.stealer();
                    //Required due to a bug in rust: rust believes that Handle and Manager have to be Send
                    // when Task doesn't have anything to do with the Manager or the Handle!
                    let rust_hack_1 = self.task_queue.clone();
                    let rust_hack_2 = self.inner.task_stealers.clone();
                    let rust_hack_3 = self.inner.end_queue.clone();
                    let rust_hack_4 = self.inner.term_queue.clone();
                    self.inner.task_stealers[i] = Some(stealer);
                    *handle = Some(manager.spawn_thread(move || {
                        let thread = WorkThread::new(i,
                                                     rust_hack_1,
                                                     worker,
                                                     rust_hack_2,
                                                     rust_hack_4,
                                                     rust_hack_3);
                        thread.main_loop()
                    }));
                    break;
                }
            }
            self.inner.running_threads += 1;
        }
    }

    /// Send a new task to the injector queue.
    ///
    /// **The task execution order is not guaranteed,
    /// however the task index is guaranteed to be the order of the call to dispatch.**
    ///
    /// **If a task panics it will leave a dead thread in the corresponding slot until .wait() is called.**
    ///
    /// # Arguments
    ///
    /// * `manager`: the thread manager to spawn a new thread if needed.
    /// * `f`: the task function to execute.
    ///
    /// # Examples
    ///
    /// ```
    /// use bp3d_threads::UnscopedThreadManager;
    /// use bp3d_threads::ThreadPool;
    /// let manager = UnscopedThreadManager::new();
    /// let mut pool: ThreadPool<UnscopedThreadManager, ()> = ThreadPool::new(4);
    /// pool.send(&manager, |_| ());
    /// ```
    pub fn send<F: FnOnce(usize) -> T + Send + 'env>(
        &mut self,
        manager: &M,
        f: F,
    ) {
        let task = Task {
            func: Box::new(f),
            id: self.task_id,
        };
        self.task_queue.push(task);
        self.task_id += 1;
        self.rearm_one_thread_if_possible(manager);
    }

    /// Schedule a new task to run.
    ///
    /// Returns true if the task was successfully scheduled, false otherwise.
    ///
    /// *NOTE: Since version 1.1.0, failure is no longer possible so this function will never return false.*
    ///
    /// **The task execution order is not guaranteed,
    /// however the task index is guaranteed to be the order of the call to dispatch.**
    ///
    /// **If a task panics it will leave a dead thread in the corresponding slot until .join() is called.**
    ///
    /// # Arguments
    ///
    /// * `manager`: the thread manager to spawn a new thread if needed.
    /// * `f`: the task function to execute.
    ///
    /// returns: bool
    #[deprecated(since="1.1.0", note="Please use `send` instead")]
    pub fn dispatch<F: FnOnce(usize) -> T + Send + 'env>(
        &mut self,
        manager: &M,
        f: F,
    ) -> bool {
        self.send(manager, f);
        true
    }

    /// Returns true if this thread pool is idle.
    ///
    /// **An idle thread pool does neither have running threads nor waiting tasks
    /// but may still have waiting results to poll.**
    pub fn is_idle(&self) -> bool {
        self.task_queue.is_empty() && self.inner.running_threads == 0
    }

    /// Poll a result from this thread pool if any, returns None if no result is available.
    pub fn poll(&mut self) -> Option<T> {
        if let Some(v) = self.inner.term_queue.pop() {
            self.inner.threads[v] = None;
            self.inner.task_stealers[v] = None;
            self.inner.running_threads -= 1;
        }
        if self.end_batch.is_none() {
            self.end_batch = self.inner.end_queue.pop();
        }
        let value = match self.end_batch.as_mut() {
            None => None,
            Some(v) => {
                let val = v.pop();
                if v.len() == 0 {
                    self.end_batch = None;
                }
                val
            }
        };
        value
    }

    /// Waits for all tasks to finish execution and stops all threads while iterating over task
    /// results.
    ///
    /// *Use this to periodically clean-up the thread pool, if you know that some tasks may panic.*
    ///
    /// **Use this function in map-reduce kind of scenarios.**
    ///
    /// # Errors
    ///
    /// Returns an error if a thread did panic.
    pub fn reduce(&mut self) -> Iter<'_, 'env, M, T> {
        Iter {
            inner: &mut self.inner,
            batch: None,
            thread_id: 0
        }
    }

    /// Waits for all tasks to finish execution and stops all threads.
    ///
    /// *Use this to periodically clean-up the thread pool, if you know that some tasks may panic.*
    ///
    /// # Errors
    ///
    /// Returns an error if a thread did panic.
    pub fn wait(&mut self) -> std::thread::Result<()> {
        for i in 0..self.inner.n_threads {
            if let Some(h) = self.inner.threads[i].take() {
                h.join()?;
                self.inner.term_queue.pop();
                self.inner.running_threads -= 1;
            }
            self.inner.task_stealers[i] = None;
        }
        Ok(())
    }

    /// Waits for all tasks to finish execution and stops all threads.
    ///
    /// *Use this to periodically clean-up the thread pool, if you know that some tasks may panic.*
    ///
    /// # Errors
    ///
    /// Returns an error if a thread did panic.
    #[deprecated(since="1.1.0", note="Please use `wait` or `reduce` instead")]
    pub fn join(&mut self) -> std::thread::Result<()> {
        self.wait()
    }
}
