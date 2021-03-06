use super::ConcurrentLog;

#[test]
fn simple_construct_and_fill()
{
    let log = ConcurrentLog::new();

    for i in 0..1000
    {
        log.push(i);
    }

    assert_eq!(log.size(), 1000);
}

#[test]
fn larger_construct_and_fill()
{
    let log = ConcurrentLog::with_segment_size(128);

    for i in 0..10000
    {
        log.push(i);
    }

    assert_eq!(log.size(), 10000);
}


#[test]
fn debug_print()
{
    let mut log = ConcurrentLog::with_segment_size(4);

    for i in 0..10
    {
        log.push(i);
    }

    assert_eq!(format!("{:?}", log), "ConcurrentLog {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9}");

    log.trim(|i| *i < 5);

    assert_eq!(format!("{:?}", log), "ConcurrentLog {4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9}");
}

#[test]
fn iter()
{
    let log = ConcurrentLog::new();
    let mut vec = Vec::new();

    for i in 0..50
    {
        log.push(i);
        vec.push(i);
    }

    let vec2 = log.iter().copied().collect::<Vec<_>>();

    assert_eq!(vec, vec2);
}

#[test]
fn iter_after_trim()
{
    let mut log = ConcurrentLog::with_segment_size(8);

    for i in 0..50
    {
        log.push(i);

    }

    log.trim(|i| *i < 20);

    let vec = (16..50).into_iter().collect::<Vec<_>>();
    let vec2 = log.iter().copied().collect::<Vec<_>>();

    assert_eq!(vec, vec2);
}

use std::sync::Arc;

#[test]
fn concurrent_fill()
{
    // Segment size is deliberately small to maximise possible
    // contention at segment edges
    let log = Arc::new(ConcurrentLog::with_segment_size(64));

    let mut threads = Vec::new();

    for i in 0..20
    {
        let log = Arc::clone(&log);

        threads.push(std::thread::spawn(move || {
            for _j in 0..1000
            {
                log.push(i);
            }
        }));
    }

    for thread in threads
    {
        thread.join().unwrap();
    }

    assert_eq!(log.size(), 20000);

    let mut counts = [0; 20];

    for entry in log.iter()
    {
        counts[*entry] += 1;
    }

    assert_eq!(counts, [1000; 20]);
}

#[test]
fn trim()
{
    let mut log = ConcurrentLog::with_segment_size(32);

    for i in 0..100
    {
        log.push(i);
    }

    log.trim(|i| *i < 50);

    // trim() removes entire segments, which we set to 32 items
    // Given the <50 condition, that'll remove one block, or 32 items
    assert_eq!(log.size(), 68);
    assert_eq!(log.get(0), None);
    assert_eq!(log.get(31), None);
    assert_eq!(log.get(32), Some(&32));
    assert_eq!(log.get(99), Some(&99));
    assert_eq!(log.get(100), None);
}

use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};

struct DropCounter<'a> (&'a AtomicUsize);

impl<'a> Drop for DropCounter<'a>
{
    fn drop(&mut self)
    {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn drop_called()
{
    let log = ConcurrentLog::new();
    let counter = AtomicUsize::new(0);

    for _i in 0..100
    {
        log.push(DropCounter(&counter));
    }

    drop(log);

    assert_eq!(counter.load(Ordering::Relaxed), 100);
}

#[test]
fn trim_drops()
{
    let mut log = ConcurrentLog::with_segment_size(32);
    let counter = AtomicUsize::new(0);

    for i in 0..100
    {
        log.push((i, DropCounter(&counter)));
    }

    log.trim(|(i,_)| *i < 50);

    // One block trimmed, so it should have dropped 32 items
    assert_eq!(counter.load(Ordering::Relaxed), 32);

    drop(log);

    // The rest should now have been dropped
    assert_eq!(counter.load(Ordering::Relaxed), 100);
}