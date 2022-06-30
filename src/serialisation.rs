use super::*;

use serde::{
    Serialize,
    Serializer,
    ser::{
        SerializeSeq,
    },
    de::{
        Visitor,
    },
    Deserialize,
};

impl<T: Serialize> Serialize for ConcurrentLog<T>
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error>
    {
        let serialized_len = self.safe_size.load(Ordering::Relaxed);

        let mut seq = serializer.serialize_seq(Some(serialized_len))?;

        for item in self.iter()
        {
            seq.serialize_element(item)?;
        }

        seq.end()
    }
}

struct LogVisitor<'de, T: Deserialize<'de>>(PhantomData<fn() -> &'de T>);

impl<'de, T: Deserialize<'de>> LogVisitor<'de, T>
{
    fn new() -> Self
    {
        Self(PhantomData)
    }
}

impl<'de, T: Deserialize<'de>> Visitor<'de> for LogVisitor<'de, T>
{
    type Value = ConcurrentLog<T>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a sequence of elements")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>
    {
        let log = ConcurrentLog::new();

        while let Some(element) = seq.next_element()?
        {
            log.push(element);
        }

        Ok(log)
    }
}

impl<'de, T: 'de + Deserialize<'de>> Deserialize<'de> for ConcurrentLog<T>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>
    {
        deserializer.deserialize_seq(LogVisitor::new())
    }
}

#[cfg(test)]
mod tests
{
    use crate::*;

    #[test]
    fn serialise()
    {
        let log = ConcurrentLog::with_segment_size(4);

        for i in 0..10
        {
            log.push(i);
        }

        assert_eq!(serde_json::to_string(&log).unwrap(), "[0,1,2,3,4,5,6,7,8,9]")
    }

    #[test]
    fn deserialize()
    {
        let log: ConcurrentLog<usize> = serde_json::from_str("[0,1,2,3,4,5,6,7,8,9]").unwrap();

        assert_eq!(log.size(), 10);
        for i in 0..10
        {
            assert_eq!(log.get(i), Some(&i));
        }
    }
}