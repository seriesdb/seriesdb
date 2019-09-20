use crate::batch::Batch;
use crate::iterator::Iterator;
use crate::utils;
use crate::Engine;
use crate::Error;
use bytes::Bytes;
use rocksdb::DBVector;
use std::fmt;

pub struct Table<'a> {
    pub(in crate) engine: &'a Engine,
    pub(in crate) id: [u8; 4],
    pub(in crate) anchor: Bytes,
}

impl<'a> fmt::Debug for Table<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "id: {:?}, anchor: {:?}", self.id, self.anchor)
    }
}

impl<'a> Table<'a> {
    #[inline]
    pub(in crate) fn new(engine: &Engine, id: [u8; 4], anchor: Bytes) -> Table {
        Table { engine, id, anchor }
    }

    #[inline]
    pub fn put<K, V>(&self, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.engine.put(utils::build_inner_key(self.id, key), value)
    }

    #[inline]
    pub fn batch(&self) -> Batch {
        Batch::new(self.id)
    }

    #[inline]
    pub fn write(&self, b: Batch) -> Result<(), Error> {
        self.engine.write(b.inner)
    }

    #[inline]
    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), Error> {
        self.engine.delete(utils::build_inner_key(self.id, key))
    }

    #[inline]
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<DBVector>, Error> {
        self.engine.get(utils::build_inner_key(self.id, key))
    }

    #[inline]
    pub fn iter(&self) -> Iterator {
        Iterator::new(self.engine, self.id, &self.anchor)
    }
}

#[test]
fn test_put() {
    utils::run_test("test_put", |db| {
        let name = "huobi.btc.usdt.1min";
        let table = db.new_table(name).unwrap();
        let result = table.put(b"k111", b"v111");
        assert!(result.is_ok());
    })
}

#[allow(unused_must_use)]
#[test]
fn test_get() {
    utils::run_test("test_get", |db| {
        let name = "huobi.btc.usdt.1min";
        let table = db.new_table(name).unwrap();
        table.put(b"k111", b"v111");
        let result = table.get(b"k111");
        assert_eq!(result.unwrap().unwrap().to_utf8().unwrap(), "v111");
    })
}

#[allow(unused_must_use)]
#[test]
fn test_delete() {
    utils::run_test("test_delete", |db| {
        let name = "huobi.btc.usdt.1min";
        let table = db.new_table(name).unwrap();
        table.put(b"k111", b"v111");
        table.get(b"k111");
        let result = table.delete(b"k111");
        assert!(result.is_ok());
        let result = table.get(b"k111");
        assert!(result.unwrap().is_none());
    })
}
