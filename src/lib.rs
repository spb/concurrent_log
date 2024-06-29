//! A log type which permits concurrent appends through shared references,
//! and pruning via a mutable reference.

use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    marker::PhantomData,
    mem::MaybeUninit,
    sync::atomic::{
        AtomicUsize,
        Ordering,
    },
};

use parking_lot::{
    RwLock,
    RwLockUpgradableReadGuard
};

#[cfg(feature = "serde")]
mod serialisation;

struct Segment<T>(Box<[UnsafeCell<MaybeUninit<T>>]>);

impl<T> Segment<T>
{
    fn new(size: usize) -> Self
    {
        Self((0..size).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect::<Vec<_>>().into_boxed_slice())
    }

    fn size(&self) -> usize
    {
        self.0.len()
    }

    // Requires that the element at offset `index` is in bounds and has previously been initialised
    unsafe fn get<'a>(&self, index: usize) -> &'a T
    {
        // `UnsafeCell` guarantees the pointer isn't null, so unwrap() won't fail
        &*(self.0.get_unchecked(index).get().as_ref().unwrap().assume_init_ref() as *const T)
    }

    // Requires that the element at offset `index` is in bounds
    unsafe fn get_mut(&mut self, index: usize) -> &mut MaybeUninit<T>
    {
        self.0.get_unchecked_mut(index).get_mut()
    }

    // Requires that the element at offset `index` is in bounds but has *not* previously been initialised
    unsafe fn put(&self, index: usize, item: T)
    {
        // `UnsafeCell` guarantees the pointer isn't null, so unwrap() won't fail
        self.0.get_unchecked(index).get().as_mut().unwrap().write(item);
    }
}

/// A log supporting concurrent append operations and exclusive trimming of old entries.
///
/// Push and get operations are supported concurrently via a shared reference. Multiple
/// simultaneous push operations are supported, and will only block if a new segment
/// needs to be allocated to provide additional capacity.
pub struct ConcurrentLog<T>
{
    segment_size: usize,
    segments: RwLock<VecDeque<Segment<T>>>,
    next_index: AtomicUsize,
    safe_size: AtomicUsize,
    pending_puts: AtomicUsize,
    start_index: usize,
    _marker: PhantomData<T>
}

// ConcurrentLog is Send/Sync if its element type is
unsafe impl<T: Send> Send for ConcurrentLog<T> { }
unsafe impl<T: Sync> Sync for ConcurrentLog<T> { }

impl<T> ConcurrentLog<T>
{
    /// Construct an empty `ConcurrentLog` with the default segment size
    pub fn new() -> Self
    {
        Self::with_segment_size(2048)
    }

    /// Construct an empty `ConcurrentLog` with the specified segment size
    pub fn with_segment_size(segment_size: usize) -> Self
    {
        ConcurrentLog {
            segment_size,
            segments: RwLock::new(VecDeque::new()),
            next_index: AtomicUsize::new(0),
            pending_puts: AtomicUsize::new(0),
            safe_size: AtomicUsize::new(0),
            start_index: 0,
            _marker: PhantomData
        }
    }

    /// Return the segment size used by this log
    pub fn segment_size(&self) -> usize
    {
        self.segment_size
    }

    /// Take an index into the log and return the segment index and index into that segment
    fn segment_for_index(&self, index: usize) -> (usize, usize)
    {
        let effective_index = index - self.start_index;
        let segment_size = self.segment_size();
        (effective_index / segment_size, effective_index % segment_size)
    }

    /// Check that the segment for the given index exists, and allocate it
    /// if not.
    fn ensure_segment(&self, required_segment: usize)
    {
        // Using an upgradeable read guard here ensures exclusivity for this test,
        // and that two threads won't simultaneously decide that a new segment is required
        // before serially creating it.
        //
        // If this is contended, then the uniqueness of upgradable locks ensures that
        // the loser won't be able to test the size of `segments` until after the winner
        // has finished updating it.
        let segments = self.segments.upgradable_read();

        // Test whether we need a new segment
        if required_segment >= segments.len()
        {
            let mut segments = RwLockUpgradableReadGuard::upgrade(segments);
            segments.push_back(Segment::new(self.segment_size()));
        }
    }

    /// Iterate over elements in the log.
    ///
    /// If elements are appended to the log during the iteration, either by the same or another
    /// thread, they may or may not be returned by the iterator.
    pub fn iter(&self) -> Iterator<T>
    {
        Iterator {
            log: self,
            current_index: self.start_index
        }
    }

    /// Append an element to the end of the log. This can be done concurrently using
    /// shared references, and is safe to do while read access or iteration is in progress.
    ///
    /// Returns the index of the new item
    pub fn push(&self, item: T) -> usize
    {
        self.push_with_index(item, |_,_| ())
    }

    /// Append an element to the end of the log, executing the provided closure to provide its
    /// index before insertion is finalised. The same concurrency guarantees apply as for `push()`.
    ///
    /// Returns the index of the new item
    pub fn push_with_index(&self, mut item: T, update: impl FnOnce(&mut T, usize) -> ()) -> usize
    {
        // AcqRel ordering means that this add will be seen by any other thread accessing `pending_puts`
        // before the addition to `new_index`.
        self.pending_puts.fetch_add(1, Ordering::AcqRel);

        let new_index = self.next_index.fetch_add(1, Ordering::AcqRel);
        let (required_segment, segment_index) = self.segment_for_index(new_index);

        // ensure_segment_for_index manages locking of `segments` itself
        self.ensure_segment(required_segment);

        // Update the new entry with the calculated index
        update(&mut item, new_index);

        unsafe {
            // Safety: put() requires that
            //     - the index hasn't previously been initialised; we know this
            //       because we're in control of `self.next_index` and only use each value once
            //     - the index is in bounds for the segment; we know this because the value from
            //       `segment_for_index` is modulo the segment size
            //
            // Also get().unwrap() is OK because we just made sure it exists with `ensure_segment`
            self.segments.read().get(required_segment).unwrap().put(segment_index, item);
        }

        // Use of AcqRel here means that if this returns 0, then we're guaranteed to see all other
        // operations from threads that had pending puts at some point recently - in particular, their
        // changes to `next_index` will be required to appear after the increment and before the decrement,
        // meaning if the value reaches 0 then a load of `next_index` at that moment will be one past the
        // index that's safe to access (because the puts have completed)
        //
        // NB `fetch_sub` returns the value before decrementing, so if it returned 1 then the value is now 0
        if self.pending_puts.fetch_sub(1, Ordering::AcqRel) == 1
        {
            let new_next_index = self.next_index.load(Ordering::Acquire);
            // Make sure another thread hasn't started another put between our fetch_sub and load ops
            if self.pending_puts.load(Ordering::Acquire) == 0
            {
                // new_next_index is the index of the next element that will be inserted, which is
                // the same as the current number of elements
                self.safe_size.store(new_next_index, Ordering::Release);
            }
        }

        new_index
    }

    /// The number of entries currently in the log
    pub fn size(&self) -> usize
    {
        // Relaxed ordering is OK here; this isn't updated until after a new element is safely
        // constructed, so the worst case is that it returns the value from before the insert
        self.safe_size.load(Ordering::Relaxed) - self.start_index
    }

    /// The first currently valid index
    pub fn start_index(&self) -> usize
    {
        self.start_index
    }

    /// The last currently valid index
    pub fn last_index(&self) -> usize
    {
        self.safe_size.load(Ordering::Relaxed)
    }

    /// Retrieve an entry from the log
    pub fn get(&self, index: usize) -> Option<&T>
    {
        if index < self.start_index || index >= self.safe_size.load(Ordering::Acquire)
        {
            None
        }
        else
        {
            let (segment_index, index_in_segment) = self.segment_for_index(index);
            let segments = self.segments.read();
            let segment = segments.get(segment_index)?;
            unsafe {
                Some(segment.get(index_in_segment))
            }
        }
    }

    /// Access an entry mutably.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T>
    {
        if index < self.start_index || index >= self.safe_size.load(Ordering::Acquire)
        {
            None
        }
        else
        {
            let (segment_index, index_in_segment) = self.segment_for_index(index);
            let segments = self.segments.get_mut();
            let segment = segments.get_mut(segment_index)?;
            unsafe {
                Some(segment.get_mut(index_in_segment).assume_init_mut())
            }
        }
    }

    /// Remove elements from the front of the log, probably in order to free up memory.
    ///
    /// `trim()` will remove a number of whole segments from the front of the log, and
    /// if the supplied predicate satisfies the conditions listed below, will retain all
    /// elements for which the predicate would return false.
    ///
    /// Indices for retained elements will be unchanged; indices that previously referred
    /// to removed elements will cease to be valid.
    ///
    /// ### The `test` predicate
    ///
    /// If `test` returns `true` for an entry, it should also be true for every entry
    /// before it in the log, and if it returns `false` for an entry then it should also
    /// be false for every entry after it. The intention is that it should test the age
    /// of the entry either via serial number or timestamp, in order to match entries older
    /// than a particular cut-off point.
    ///
    /// If this condition does not hold for the supplied predicate and the entries in
    /// the log, then the number of segments trimmed will be unpredictable.
    pub fn trim(&mut self, test: impl Fn(&T) -> bool)
    {
        let segments = self.segments.get_mut();

        loop
        {
            // If we don't have any segments, don't trim anything
            let first_segment = match segments.front()
            {
                Some(seg) => seg,
                None => return
            };

            // If we have less than a full segment, don't trim it
            if self.safe_size.load(Ordering::Relaxed) < first_segment.size()
            {
                return;
            }

            // Now run the supplied test
            let should_trim = unsafe {
                test(&*first_segment.get(0)) &&
                    test(&*first_segment.get(first_segment.size() - 1))
            };

            // As soon as we reach a segment that's not trimmable, we stop
            if ! should_trim
            {
                return;
            }

            if let Some(mut popped) = segments.pop_front()
            {
                // If we removed a segment, then our start index needs to be updated
                // accordingly.
                self.start_index += popped.size();

                // NB we never trim a segment that wasn't full
                for idx in 0..popped.size()
                {
                    unsafe {
                        popped.get_mut(idx).assume_init_drop();
                    }
                }
            }
        }
    }
}

impl<T> Drop for ConcurrentLog<T>
{
    fn drop(&mut self)
    {
        let segments = self.segments.get_mut();
        let mut size = self.safe_size.load(Ordering::Relaxed) - self.start_index;

        while let Some(mut segment) = segments.pop_front()
        {
            for i in 0..std::cmp::min(size, segment.size())
            {
                unsafe {
                    segment.get_mut(i).assume_init_drop()
                }
            }

            // If this saturates, it's because this was the last segment, so
            // we don't need to worry about inaccuracy
            size = size.saturating_sub(segment.size());
        }
    }
}

impl<T> Default for ConcurrentLog<T>
{
    fn default() -> Self
    {
        Self::new()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for ConcurrentLog<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result
    {
        f.write_str("ConcurrentLog ")?;
        f.debug_map().entries(
            (self.start_index..self.safe_size.load(Ordering::Relaxed))
                .map(|idx| (idx, self.get(idx).unwrap()))
        ).finish()
    }
}

pub struct Iterator<'a, T>
{
    log: &'a ConcurrentLog<T>,
    current_index: usize,
}

impl<'a, T> std::iter::Iterator for Iterator<'a, T>
{
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T>
    {
        let index = self.current_index;
        self.current_index += 1;

        self.log.get(index)
    }
}

#[cfg(test)]
mod tests;