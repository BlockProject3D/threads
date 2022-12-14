// Copyright (c) 2022, BlockProject 3D
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

use std::sync::atomic::{AtomicBool, AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicIsize, AtomicU16, AtomicU32, AtomicU64, AtomicUsize};
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

/// Represents any type which can be used in an AtomicCell (ex: i8, u8, etc).
///
/// *NOTE: This trait is intended to be implemented, not directly used, for use please check
/// [AtomicCell](AtomicCell)*
pub trait Atomic {
    /// The atomic representation of this type.
    type Atomic;

    /// Constructs an atomic from an instance of this type.
    ///
    /// # Arguments
    ///
    /// * `value`: the value to turn into an atomic.
    ///
    /// returns: Self::Atomic
    fn atomic_new(value: Self) -> Self::Atomic;

    /// Replaces the value in an atomic.
    ///
    /// *The ordering for store operations is [Release](Ordering::Release).*
    ///
    /// # Arguments
    ///
    /// * `atomic`: the atomic to mutate.
    /// * `value`: the new replacement value.
    ///
    /// returns: ()
    fn atomic_set(atomic: &Self::Atomic, value: Self);

    /// Reads the value of an atomic.
    ///
    /// *The ordering for load operations is [Acquire](Ordering::Acquire).*
    ///
    /// # Arguments
    ///
    /// * `atomic`: the atomic to read.
    ///
    /// returns: Self
    fn atomic_get(atomic: &Self::Atomic) -> Self;
}

macro_rules! atomic_scalar {
    ($(($native: ty: $atomic: ty))*) => {
        $(
        impl Atomic for $native {
            type Atomic = $atomic;
            fn atomic_new(value: Self) -> Self::Atomic {
                Self::Atomic::new(value)
            }
            fn atomic_set(atomic: &Self::Atomic, value: Self) {
                atomic.store(value, Ordering::Release);
            }
            fn atomic_get(atomic: &Self::Atomic) -> Self {
                atomic.load(Ordering::Acquire)
            }
        }
        )*
    };
}

atomic_scalar! {
    (u8: AtomicU8)
    (u16: AtomicU16)
    (u32: AtomicU32)
    (u64: AtomicU64)
    (usize: AtomicUsize)
    (i8: AtomicI8)
    (i16: AtomicI16)
    (i32: AtomicI32)
    (i64: AtomicI64)
    (isize: AtomicIsize)
    (bool: AtomicBool)
}

impl Atomic for f32 {
    type Atomic = AtomicU32;

    fn atomic_new(value: Self) -> Self::Atomic {
        //SAFETY: We don't care whatever value it is, we just want the binary representation of
        // the float as a U32 (32 bits)
        unsafe { Self::Atomic::new(std::mem::transmute(value)) }
    }

    fn atomic_set(atomic: &Self::Atomic, value: Self) {
        unsafe { atomic.store(std::mem::transmute(value), Ordering::Release) }
    }

    fn atomic_get(atomic: &Self::Atomic) -> Self {
        let val = atomic.load(Ordering::Acquire);
        unsafe { std::mem::transmute(val) }
    }
}

impl Atomic for f64 {
    type Atomic = AtomicU64;

    fn atomic_new(value: Self) -> Self::Atomic {
        //SAFETY: We don't care whatever value it is, we just want the binary representation of
        // the float as a U64 (64 bits)
        unsafe { Self::Atomic::new(std::mem::transmute(value)) }
    }

    fn atomic_set(atomic: &Self::Atomic, value: Self) {
        unsafe { atomic.store(std::mem::transmute(value), Ordering::Release) }
    }

    fn atomic_get(atomic: &Self::Atomic) -> Self {
        let val = atomic.load(Ordering::Acquire);
        unsafe { std::mem::transmute(val) }
    }
}

/// A type of cell which only stores atomic values.
/// It provides the same basic get and set operations as a [Cell](std::cell::Cell).
pub struct AtomicCell<T: Atomic> {
    inner: T::Atomic
}

impl<T: Atomic + Default> Default for AtomicCell<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Atomic> AtomicCell<T> {

    /// Creates a new AtomicCell with a value.
    ///
    /// # Arguments
    ///
    /// * `value`: the initial value to store.
    ///
    /// returns: AtomicCell<T>
    ///
    /// # Examples
    ///
    /// ```
    /// use bp3d_threads::AtomicCell;
    /// AtomicCell::new(42);
    /// ```
    pub fn new(value: T) -> AtomicCell<T> {
        AtomicCell {
            inner: T::atomic_new(value)
        }
    }

    /// Gets the value stored in the cell.
    pub fn get(&self) -> T {
        T::atomic_get(&self.inner)
    }

    /// Sets the value of the cell.
    ///
    /// # Arguments
    ///
    /// * `value`: the new value to store in the cell.
    ///
    /// returns: ()
    ///
    /// # Examples
    ///
    /// ```
    /// use bp3d_threads::AtomicCell;
    /// let cell = AtomicCell::new(0);
    /// assert_eq!(cell.get(), 0);
    /// cell.set(42);
    /// assert_eq!(cell.get(), 42);
    /// ```
    pub fn set(&self, value: T) {
        T::atomic_set(&self.inner, value)
    }
}

impl<T: Atomic> Clone for AtomicCell<T> {
    fn clone(&self) -> Self {
        Self::new(self.get())
    }
}
