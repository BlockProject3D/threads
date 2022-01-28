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

/// Simplify usage in map-reduce scenarios. Provides reduce function for any [AddAssign](std::ops::AddAssign).
pub trait Reduce
{
    /// Merges the current reduced result with a new result.
    ///
    /// # Arguments
    ///
    /// * `other`: the new result to merge in.
    ///
    /// returns: ()
    fn reduce(&mut self, other: Self);
}

macro_rules! impl_reduce {
    ($($t:ty)*) => {
        $(
            impl Reduce for $t {
                fn reduce(&mut self, other: Self) {
                    *self += other;
                }
            }
        )*
    };
}

impl_reduce!(i8 i16 i32 i64 u8 u16 u32 u64 isize usize i128 u128 f32 f64);

impl<T> Reduce for Vec<T> {
    fn reduce(&mut self, other: Self) {
        self.extend(other);
    }
}

impl Reduce for () {
    fn reduce(&mut self, _: Self) {
        //return nothing as this means the reducer doesn't have anything to merge
    }
}
