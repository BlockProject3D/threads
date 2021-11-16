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
use std::time::Duration;
use crossbeam_channel::{bounded, Receiver, Sender, unbounded};

struct Task<'env, T: Send + 'static>
{
    func: Box<dyn FnOnce(usize) -> T + Send + 'env>,
    id: usize
}

fn thread_pool_worker<T: Send>(tasks: Receiver<Task<T>>, out: Sender<T>)
{
    while let Ok(v) = tasks.try_recv() {
        let res = (v.func)(v.id);
        if out.send(res).is_err() {
            break;
        }
    }
    // Wait 100ms before running the next iteration to let a chance to the main thread to refill the task channel.
    std::thread::sleep(Duration::from_millis(100));
}

pub trait Join
{
    fn join(self) -> std::thread::Result<()>;
}

pub trait ThreadManager<'env>
{
    type Handle : Join;

    fn spawn_thread<F: FnOnce() + Send + 'env>(&self, func: F) -> Self::Handle;
}

pub struct ThreadPool<'env, T: Send + 'static, Manager: ThreadManager<'env>>
{
    end_channel_out: Receiver<T>,
    end_channel_in: Sender<T>,
    task_channel_out: Receiver<Task<'env, T>>,
    task_channel_in: Sender<Task<'env, T>>,
    term_channel_out: Receiver<usize>,
    term_channel_in: Sender<usize>,
    n_threads: usize,
    threads: Box<[Option<Manager::Handle>]>,
    running_threads: usize,
    task_id: usize,
}

impl<'env, T: Send, Manager: ThreadManager<'env>> ThreadPool<'env, T, Manager>
{
    pub fn new(n_threads: usize) -> Self
    {
        let (end_channel_in, end_channel_out) = unbounded();
        let (task_channel_in, task_channel_out) = unbounded();
        let (term_channel_in, term_channel_out) = bounded(n_threads);
        Self {
            end_channel_out,
            end_channel_in,
            task_channel_out,
            task_channel_in,
            term_channel_out,
            term_channel_in,
            n_threads,
            running_threads: 0,
            threads: repeat_with(|| None).take(n_threads).collect::<Vec<Option<Manager::Handle>>>().into_boxed_slice(),
            task_id: 0
        }
    }

    fn rearm_one_thread_if_possible(&mut self, manager: &Manager)
    {
        if self.running_threads < self.n_threads {
            for (i, handle) in self.threads.iter_mut().enumerate() {
                if handle.is_none() {
                    let tasks = self.task_channel_out.clone();
                    let out = self.end_channel_in.clone();
                    let term = self.term_channel_in.clone();
                    *handle = Some(manager.spawn_thread(move || {
                        thread_pool_worker(tasks, out);
                        term.send(i).unwrap();
                    }));
                    break;
                }
            }
            self.running_threads += 1;
        }
    }

    pub fn dispatch<F: FnOnce(usize) -> T + Send + 'env>(&mut self, manager: &Manager, f: F) -> bool
    {
        let task = Task {
            func: Box::new(f),
            id: self.task_id
        };
        if self.task_channel_in.send(task).is_err() {
            return false;
        }
        self.task_id += 1;
        self.rearm_one_thread_if_possible(manager);
        true
    }

    pub fn is_empty(&self) -> bool
    {
        self.task_channel_in.is_empty() && self.running_threads == 0
    }

    pub fn poll(&mut self) -> Option<T>
    {
        if let Ok(v) = self.term_channel_out.try_recv() {
            self.threads[v] = None;
            self.running_threads -= 1;
        }
        match self.end_channel_out.try_recv() {
            Ok(v) => Some(v),
            Err(_) => None
        }
    }

    pub fn join(&mut self) -> std::thread::Result<()>
    {
        for handle in self.threads.iter_mut() {
            if let Some(h) = handle.take() {
                h.join()?;
                let _ = self.term_channel_out.recv();
                self.running_threads -= 1;
            }
        }
        Ok(())
    }
}
