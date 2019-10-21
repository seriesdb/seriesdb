use crate::consts::*;
use crate::update::Update;
use crate::utils::*;
use bytes::Bytes;
use rocksdb::WriteBatchIterator;
use std::fmt::{Debug, Formatter, Result as FmtResult};

pub struct Updates {
    pub sn: u64,
    pub vec: Vec<Update>,
}

impl WriteBatchIterator for Updates {
    fn put(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
        self.vec.push(Update::Put {
            key: Bytes::from(key.as_ref()),
            value: Bytes::from(value.as_ref()),
        })
    }
    fn delete(&mut self, key: Box<[u8]>) {
        let table_id = extract_table_id(&key);
        if table_id == DELETE_RANGE_HINT_TABLE_ID {
            let (from_key, to_key) = extract_delete_range_hint(key);
            self.vec.push(Update::DeleteRange { from_key, to_key })
        } else {
            self.vec.push(Update::Delete {
                key: Bytes::from(key.as_ref()),
            })
        }
    }
}

impl Debug for Updates {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{:?}@{:?}", &self.vec, self.sn)
    }
}

impl Updates {
    pub fn new() -> Self {
        Updates { sn: 0, vec: vec![] }
    }
}
