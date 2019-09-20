use crate::utils;
use bytes::Bytes;
use rocksdb::DBRawIterator;

pub struct Iterator<'a> {
    inner: DBRawIterator<'a>,
    table_id: [u8; 4],
    anchor: &'a Bytes,
}

impl<'a> Iterator<'a> {
    pub(in crate) fn new(
        engine: &'a crate::Engine,
        table_id: [u8; 4],
        anchor: &'a Bytes,
    ) -> Iterator<'a> {
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        let iter: DBRawIterator = engine.raw_iterator_opt(&opts);
        Iterator {
            inner: iter,
            table_id,
            anchor,
        }
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        if self.inner.valid() && !self.is_anchor() {
            return true;
        } else {
            return false;
        }
    }

    #[inline]
    pub fn seek_to_first(&mut self) {
        self.inner.seek(self.table_id)
    }

    #[inline]
    pub fn seek_to_last(&mut self) {
        self.inner.seek(self.anchor);
        if self.inner.valid() {
            self.inner.prev()
        }
    }

    #[inline]
    pub fn seek<K: AsRef<[u8]>>(&mut self, key: K) {
        self.inner.seek(utils::build_inner_key(self.table_id, key));
    }

    #[inline]
    pub fn seek_for_prev<K: AsRef<[u8]>>(&mut self, key: K) {
        self.inner
            .seek_for_prev(utils::build_inner_key(self.table_id, key));
    }

    #[inline]
    pub fn next(&mut self) {
        self.inner.next()
    }

    #[inline]
    pub fn prev(&mut self) {
        self.inner.prev()
    }

    #[inline]
    pub fn key(&self) -> Option<&[u8]> {
        if let Some(v) = self.inner_key() {
            Some(utils::extract_key(v))
        } else {
            None
        }
    }

    #[inline]
    pub fn value(&self) -> Option<&[u8]> {
        unsafe { self.inner.value_inner() }
    }

    #[inline]
    fn inner_key(&self) -> Option<&[u8]> {
        unsafe { self.inner.key_inner() }
    }

    #[inline]
    fn is_anchor(&self) -> bool {
        return self.inner_key().unwrap() == self.anchor;
    }
}

#[test]
fn test_seek() {
    utils::run_test("test_seek", |db| {
        let name = "huobi.btc.usdt.1m";
        let table = db.new_table(name).unwrap();
        let k1 = b"k1";
        let v1 = b"v1";
        let k2 = b"k2";
        let v2 = b"v2";
        let k3 = b"k3";
        let v3 = b"v3";
        assert!(table.put(k1, v1).is_ok());
        assert!(table.put(k2, v2).is_ok());
        assert!(table.put(k3, v3).is_ok());
        let mut iter = table.iter();
        iter.seek_to_first();
        assert!(iter.is_valid());
        assert_eq!(k1, iter.key().unwrap());
    });
}
