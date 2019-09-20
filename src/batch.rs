use crate::utils::build_inner_key;
use rocksdb::{Error, WriteBatch};

pub struct Batch {
    pub(in crate) inner: WriteBatch,
    table_id: [u8; 4],
}

impl Batch {
    #[inline]
    pub(in crate) fn new(table_id: [u8; 4]) -> Batch {
        Batch {
            inner: WriteBatch::default(),
            table_id: table_id,
        }
    }

    #[inline]
    pub fn put<K, V>(&mut self, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.inner.put(build_inner_key(self.table_id, key), value)
    }

    #[inline]
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), Error> {
        self.inner.delete(build_inner_key(self.table_id, key))
    }

    #[inline]
    pub fn delete_range<F, T>(&mut self, from_key: F, to_key: T) -> Result<(), Error>
    where
        F: AsRef<[u8]>,
        T: AsRef<[u8]>,
    {
        self.inner.delete_range(
            build_inner_key(self.table_id, from_key),
            build_inner_key(self.table_id, to_key),
        )
    }
}
