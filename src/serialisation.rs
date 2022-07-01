use super::*;

use serde::{
    Serialize,
    Serializer,
    ser::{
        SerializeSeq,
        SerializeStruct,
    },
    de::{
        Visitor,
    },
    Deserialize,
};

struct LogListSerializer<'a, T>(&'a ConcurrentLog<T>);

impl <'a, T: Serialize> Serialize for LogListSerializer<'a, T>
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error>
    {
        let serialized_len = self.0.safe_size.load(Ordering::Relaxed) - self.0.start_index;

        let mut seq = serializer.serialize_seq(Some(serialized_len))?;

        for item in self.0.iter()
        {
            seq.serialize_element(item)?;
        }

        seq.end()
    }
}

impl<T: Serialize> Serialize for ConcurrentLog<T>
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error>
    {
        let mut state = serializer.serialize_struct("ConcurrentLog", 3)?;
        state.serialize_field("segment_size", &self.segment_size)?;
        state.serialize_field("start_index", &self.start_index)?;
        state.serialize_field("data", &LogListSerializer(self))?;
        state.end()
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

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result
    {
        formatter.write_str("struct ConcurrentLog")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
        where
            M: serde::de::MapAccess<'de>
    {
        let mut segment_size = None;
        let mut start_index = None;

        while let Some(key) = map.next_key::<&str>()?
        {
            match key
            {
                "segment_size" => segment_size = Some(map.next_value()?),
                "start_index" => start_index = Some(map.next_value()?),
                "data" => {
                    let mut log = match segment_size {
                        Some(size) => ConcurrentLog::with_segment_size(size),
                        None => ConcurrentLog::new()
                    };
                    if let Some(start) = start_index
                    {
                        log.start_index = start;
                        log.next_index.store(start, Ordering::Relaxed);
                        log.safe_size.store(start, Ordering::Relaxed);
                    }

                    let values = map.next_value::<Vec<T>>()?;

                    for value in values
                    {
                        log.push(value)
                    }

                    return Ok(log);
                },
                _ => return Err(serde::de::Error::unknown_field(key, &["segment_size, start_index, data"]))
            }
        }

        Err(serde::de::Error::missing_field("data"))
    }

}

impl<'de, T: 'de + Deserialize<'de>> Deserialize<'de> for ConcurrentLog<T>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>
    {
        deserializer.deserialize_struct("ConcurrentLog", &["segment_size", "start_index", "data"], LogVisitor::new())
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

        assert_eq!(serde_json::to_string(&log).unwrap(), "{\"segment_size\":4,\"start_index\":0,\"data\":[0,1,2,3,4,5,6,7,8,9]}")
    }

    #[test]
    fn deserialize()
    {
        let log: ConcurrentLog<usize> = serde_json::from_str("{\"segment_size\":4,\"start_index\":0,\"data\":[0,1,2,3,4,5,6,7,8,9]}").unwrap();

        assert_eq!(log.size(), 10);
        for i in 0..10
        {
            assert_eq!(log.get(i), Some(&i));
        }
    }

    #[test]
    fn roundtrip()
    {
        let log = ConcurrentLog::with_segment_size(8);

        for i in 0..50
        {
            log.push(i);
        }

        let serialised_log = serde_json::to_string(&log).unwrap();

        let deserialised_log: ConcurrentLog<i32> = serde_json::from_str(&serialised_log).unwrap();

        assert_eq!(log.size(), deserialised_log.size());
        assert_eq!(log.start_index(), deserialised_log.start_index());
        assert_eq!(log.last_index(), deserialised_log.last_index());

        for idx in log.start_index()..log.last_index()
        {
            assert_eq!(log.get(idx), deserialised_log.get(idx))
        }
    }

    #[test]
    fn roundtrip_after_trim()
    {
        let mut log = ConcurrentLog::with_segment_size(8);

        for i in 0..50
        {
            log.push(i);
        }

        log.trim(|i| *i < 20);

        let serialised_log = serde_json::to_string(&log).unwrap();

        let deserialised_log: ConcurrentLog<i32> = serde_json::from_str(&serialised_log).unwrap();

        assert_eq!(log.size(), deserialised_log.size());
        assert_eq!(log.start_index(), deserialised_log.start_index());
        assert_eq!(log.last_index(), deserialised_log.last_index());

        for idx in log.start_index()..log.last_index()
        {
            assert_eq!(log.get(idx), deserialised_log.get(idx))
        }
    }
}