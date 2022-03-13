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
use crossbeam::deque::{Injector, Stealer, Worker};
use crossbeam::queue::{ArrayQueue, SegQueue};
use crate::Reduce;

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

/// Core thread pool.
pub struct ThreadPool<'env, Manager: ThreadManager<'env>, T: Send + 'static> {
    task_queue: Arc<Injector<Task<'env, T>>>,
    task_stealers: Box<[Option<Stealer<Task<'env, T>>>]>,
    end_queue: Arc<SegQueue<Vec<T>>>,
    end_batch: Option<Vec<T>>,
    term_queue: Arc<ArrayQueue<usize>>,
    n_threads: usize,
    threads: Box<[Option<Manager::Handle>]>,
    running_threads: usize,
    task_id: usize,
}

impl<'env, Manager: ThreadManager<'env>, T: Send> ThreadPool<'env, Manager, T> {
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
            task_stealers: vec![None; n_threads].into_boxed_slice(),
            end_queue: Arc::new(SegQueue::new()),
            end_batch: None,
            term_queue: Arc::new(ArrayQueue::new(n_threads)),
            n_threads,
            running_threads: 0,
            threads: repeat_with(|| None)
                .take(n_threads)
                .collect::<Vec<Option<Manager::Handle>>>()
                .into_boxed_slice(),
            task_id: 0,
        }
    }

    fn rearm_one_thread_if_possible(&mut self, manager: &Manager) {
        if self.running_threads < self.n_threads {
            for (i, handle) in self.threads.iter_mut().enumerate() {
                if handle.is_none() {
                    let worker = Worker::new_fifo();
                    let stealer = worker.stealer();
                    //Required due to a bug in rust: rust believes that Handle and Manager have to be Send
                    // when Task doesn't have anything to do with the Manager or the Handle!
                    let rust_hack_1 = self.task_queue.clone();
                    let rust_hack_2 = self.task_stealers.clone();
                    let rust_hack_3 = self.end_queue.clone();
                    let rust_hack_4 = self.term_queue.clone();
                    self.task_stealers[i] = Some(stealer);
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
            self.running_threads += 1;
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
        manager: &Manager,
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
        manager: &Manager,
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
        self.task_queue.is_empty() && self.running_threads == 0
    }

    /// Poll a result from this thread pool if any, returns None if no result is available.
    pub fn poll(&mut self) -> Option<T> {
        if let Some(v) = self.term_queue.pop() {
            self.threads[v] = None;
            self.task_stealers[v] = None;
            self.running_threads -= 1;
        }
        if self.end_batch.is_none() {
            self.end_batch = self.end_queue.pop();
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

    /// Waits for all tasks to finish execution and stops all threads then returns all task results.
    ///
    /// *Use this to periodically clean-up the thread pool, if you know that some tasks may panic.*
    ///
    /// **Use this function in map-reduce kind of scenarios.**
    ///
    /// # Errors
    ///
    /// Returns an error if a thread did panic.
    pub fn reduce(&mut self) -> std::thread::Result<Vec<T>> {
        let mut vec = Vec::new();
        for i in 0..self.n_threads {
            if let Some(h) = self.threads[i].take() {
                h.join()?;
                self.term_queue.pop();
                self.running_threads -= 1;
                while let Some(batch) = self.end_queue.pop() {
                    vec.extend(batch);
                }
            }
            self.task_stealers[i] = None;
        }
        Ok(vec)
    }

    /// Waits for all tasks to finish execution and stops all threads while running a reducer function for each result.
    ///
    /// *Use this to periodically clean-up the thread pool, if you know that some tasks may panic.*
    ///
    /// **Use this function in map-reduce kind of scenarios.**
    ///
    /// # Errors
    ///
    /// Returns an error if a thread did panic.
    pub fn reduce_with<R: Default + Reduce, F: FnMut(T) -> R>(&mut self, mut reducer: F) -> std::thread::Result<R> {
        let mut r= R::default();
        for i in 0..self.n_threads {
            if let Some(h) = self.threads[i].take() {
                h.join()?;
                self.term_queue.pop();
                self.running_threads -= 1;
                while let Some(batch) = self.end_queue.pop() {
                    for v in batch {
                        r.reduce(reducer(v));
                    }
                }
            }
            self.task_stealers[i] = None;
        }
        Ok(r)
    }

    /// Waits for all tasks to finish execution and stops all threads while running a reducer function for each result.
    ///
    /// *Use this to periodically clean-up the thread pool, if you know that some tasks may panic.*
    ///
    /// **Use this function in map-reduce kind of scenarios.**
    ///
    /// # Errors
    ///
    /// Returns an error if a thread did panic or that a reducer failed.
    pub fn try_reduce_with<R: Default + Reduce, E, F: FnMut(T) -> Result<R, E>>(&mut self, mut reducer: F) -> std::thread::Result<Result<R, E>> {
        let mut r= R::default();
        for i in 0..self.n_threads {
            if let Some(h) = self.threads[i].take() {
                h.join()?;
                self.term_queue.pop();
                self.running_threads -= 1;
                while let Some(batch) = self.end_queue.pop() {
                    for v in batch {
                        match reducer(v) {
                            Ok(v) => r.reduce(v),
                            Err(e) => return Ok(Err(e))
                        }
                    }
                }
            }
            self.task_stealers[i] = None;
        }
        Ok(Ok(r))
    }

    /// Waits for all tasks to finish execution and stops all threads.
    ///
    /// *Use this to periodically clean-up the thread pool, if you know that some tasks may panic.*
    ///
    /// # Errors
    ///
    /// Returns an error if a thread did panic.
    pub fn wait(&mut self) -> std::thread::Result<()> {
        for i in 0..self.n_threads {
            if let Some(h) = self.threads[i].take() {
                h.join()?;
                self.term_queue.pop();
                self.running_threads -= 1;
            }
            self.task_stealers[i] = None;
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
    #[deprecated(since="1.1.0", note="Please use `wait`, `reduce` or `reduce_with` instead")]
    pub fn join(&mut self) -> std::thread::Result<()> {
        let results = self.reduce()?;
        if let Some(v) = &mut self.end_batch {
            v.extend(results);
        } else {
            self.end_batch = Some(results);
        }
        Ok(())
    }
}
