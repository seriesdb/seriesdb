use crate::consts::*;
use crate::options::Options;
use crate::table::Table;
use crate::utils;
use crate::Engine;
use crate::Error;
use bytes::Bytes;
use rocksdb::WriteBatch;
use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

const MAX_KEY_LEN: u8 = 4;

pub struct Db {
    pub(in crate) engine: Engine,
    pub(in crate) lock: RwLock<HashMap<String, ([u8; 4], Bytes)>>,
}

impl Db {
    #[inline]
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Db, Error> {
        Db::new2(path, &Options::new())
    }

    #[inline]
    pub fn new2<P: AsRef<Path>>(path: P, opts: &Options) -> Result<Db, Error> {
        let db = Db {
            engine: Engine::open(&opts.inner, path)?,
            lock: RwLock::new(HashMap::new()),
        };
        db.put_metadata_table_anchor()?;
        Ok(db)
    }

    #[inline]
    pub fn destroy<P: AsRef<Path>>(path: P) -> Result<(), Error> {
        Engine::destroy(&Options::new().inner, path)
    }

    pub fn new_table(&self, name: &str) -> Result<Table, Error> {
        let lock = self.lock.read().unwrap();
        if let Some((id, anchor)) = lock.get(name) {
            Ok(Table {
                engine: &self.engine,
                id: *id,
                anchor: anchor.slice_from(0),
            })
        } else {
            drop(lock);
            let mut lock = self.lock.write().unwrap();
            let name_to_id_table_inner_key = utils::build_name_to_id_table_inner_key(name);
            if let Some(id) = self.engine.get(&name_to_id_table_inner_key)? {
                let id = utils::extract_table_id(id);
                Ok(Table {
                    engine: &self.engine,
                    id,
                    anchor: utils::build_userland_table_anchor(id, MAX_KEY_LEN),
                })
            } else {
                let table = self.create_table(name)?;
                lock.insert(
                    name.to_owned(),
                    (
                        table.id,
                        utils::build_userland_table_anchor(table.id, MAX_KEY_LEN),
                    ),
                );
                Ok(table)
            }
        }
    }

    pub fn rename_table(&self, old_name: &str, new_name: &str) -> Result<(), Error> {
        let mut lock = self.lock.write().unwrap();
        let mut batch = WriteBatch::default();
        let name_to_id_table_inner_key = utils::build_name_to_id_table_inner_key(old_name);
        if let Some(id) = self.engine.get(&name_to_id_table_inner_key)? {
            let id = utils::extract_table_id(id);
            let id_to_name_table_inner_key = utils::build_id_to_name_table_inner_key(id);
            batch.delete(&name_to_id_table_inner_key)?;
            batch.delete(&id_to_name_table_inner_key)?;
            batch.put(utils::build_name_to_id_table_inner_key(new_name), id)?;
            batch.put(id_to_name_table_inner_key, new_name)?;

            let anchor = utils::build_userland_table_anchor(id, MAX_KEY_LEN);
            lock.insert(new_name.to_owned(), (id, anchor));
            lock.remove(old_name);
        }
        self.engine.write(batch)
    }

    pub fn destroy_table(&self, name: &str) -> Result<(), Error> {
        let mut lock = self.lock.write().unwrap();
        let mut batch = WriteBatch::default();
        let name_to_id_table_inner_key = utils::build_name_to_id_table_inner_key(name);
        if let Some(id) = self.engine.get(&name_to_id_table_inner_key)? {
            let id = utils::extract_table_id(id);
            let id_to_name_table_inner_key = utils::build_id_to_name_table_inner_key(id);
            let anchor = utils::build_userland_table_anchor(id, MAX_KEY_LEN);
            batch.delete(&name_to_id_table_inner_key)?;
            batch.delete(&id_to_name_table_inner_key)?;
            batch.delete_range(id, anchor)?;
            lock.remove(name);
        }
        self.engine.write(batch)
    }

    pub fn get_tables(&self) -> Vec<(String, u32)> {
        let mut result: Vec<(String, u32)> = Vec::new();
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        let mut iter = self.engine.raw_iterator_opt(&opts);
        iter.seek(ID_TO_NAME_TABLE_ID);
        let anchor = utils::build_id_to_name_table_anchor();
        while iter.valid() {
            let key = unsafe { iter.key_inner().unwrap() };
            if key == anchor {
                break;
            }
            let value = unsafe { iter.value_inner().unwrap() };
            let table_id = utils::u8s_to_u32(utils::extract_key(key));
            let table_name = std::str::from_utf8(value).unwrap().to_string();
            result.push((table_name, table_id));
            iter.next();
        }
        result
    }

    fn create_table(&self, name: &str) -> Result<Table, Error> {
        let name_to_id_table_inner_key = utils::build_name_to_id_table_inner_key(name);
        let id = self.find_next_id();
        let id_to_name_table_inner_key = utils::build_id_to_name_table_inner_key(id);
        self.register_table(
            name_to_id_table_inner_key,
            id,
            id_to_name_table_inner_key,
            name,
        )?;
        let anchor = utils::build_userland_table_anchor(id, MAX_KEY_LEN);
        self.put_userland_table_anchor(&anchor)?;
        Ok(Table::new(&self.engine, id, anchor))
    }

    fn find_next_id(&self) -> [u8; 4] {
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        let mut iter = self.engine.raw_iterator_opt(&opts);
        let anchor = utils::build_id_to_name_table_anchor();
        iter.seek(anchor);
        if iter.valid() {
            iter.prev();
            if iter.valid() {
                let table_id = iter.key().unwrap();
                let table_id = utils::extract_key(table_id.as_slice());
                let table_id = utils::u8s_to_u32(table_id);
                if utils::u32_to_u8x4(table_id) >= MAX_USERLAND_TABLE_ID {
                    panic!("Exceeded MAX_USERLAND_TABLE_ID!")
                }
                let table_id = table_id + 1;
                utils::u32_to_u8x4(table_id)
            } else {
                MIN_USERLAND_TABLE_ID
            }
        } else {
            panic!("Failed to find id_to_name_table_anchor!")
        }
    }

    #[inline]
    fn register_table<K: AsRef<[u8]>>(
        &self,
        name_to_id_table_inner_key: K,
        id: [u8; 4],
        id_to_name_table_inner_key: K,
        name: &str,
    ) -> Result<(), Error> {
        let mut batch = WriteBatch::default();
        batch.put(name_to_id_table_inner_key, id)?;
        batch.put(id_to_name_table_inner_key, name)?;
        self.engine.write(batch)
    }

    #[inline]
    fn put_userland_table_anchor(&self, anchor: &Bytes) -> Result<(), Error> {
        self.engine.put(anchor, anchor)
    }

    #[inline]
    fn put_metadata_table_anchor(&self) -> Result<(), Error> {
        let mut batch = rocksdb::WriteBatch::default();
        let anchor = utils::build_id_to_name_table_anchor();
        batch.put(&anchor, &anchor)?;
        let anchor2 = utils::build_name_to_id_table_anchor();
        batch.put(&anchor2, &anchor2)?;
        self.engine.write(batch)
    }
}

#[test]
fn test_new_table() {
    utils::run_test("test_new_table", |db| {
        assert!(db.new_table("huobi.btc.usdt.1min").is_ok())
    });
}

#[test]
fn test_rename_table() {
    utils::run_test("test_rename_table", |db| {
        let old_name = "huobi.btc.usdt.1min";
        let new_name = "huobi.btc.usdt.5min";
        let table = db.new_table(old_name).unwrap();
        assert!(db.rename_table(old_name, new_name).is_ok());

        let old_name_to_id_table_inner_key = utils::build_name_to_id_table_inner_key(&old_name);
        let id = table.engine.get(old_name_to_id_table_inner_key);
        assert!(id.unwrap().is_none());

        let new_name_to_id_table_inner_key = utils::build_name_to_id_table_inner_key(&new_name);
        let id = table.engine.get(new_name_to_id_table_inner_key);
        assert_eq!(id.unwrap().unwrap().as_ref(), table.id);

        let id_to_name_table_inner_key = utils::build_id_to_name_table_inner_key(table.id);
        let name = table.engine.get(id_to_name_table_inner_key);
        assert_eq!(name.unwrap().unwrap().to_utf8().unwrap(), new_name);
    });
}

#[test]
fn test_destroy_table() {
    utils::run_test("test_destroy_table", |db| {
        let name = "huobi.btc.usdt.1min";
        let table = db.new_table(name).unwrap();
        table.put(b"k111", b"v111").unwrap();
        let result = table.get(b"k111");
        assert_eq!(result.unwrap().unwrap().to_utf8().unwrap(), "v111");
        db.destroy_table(name).unwrap();
        let result = table.get(b"k111");
        assert!(result.unwrap().is_none());
    });
}

#[test]
fn test_get_tables() {
    utils::run_test("test_get_tables", |db| {
        let name0 = "huobi.btc.usdt.1min".to_owned();
        let name1 = "huobi.btc.usdt.3min".to_owned();
        let name2 = "huobi.btc.usdt.5min".to_owned();
        let table0 = db.new_table(&name0).unwrap();
        let table1 = db.new_table(&name1).unwrap();
        let table2 = db.new_table(&name2).unwrap();
        let id0 = utils::u8x4_to_u32(table0.id);
        let id1 = utils::u8x4_to_u32(table1.id);
        let id2 = utils::u8x4_to_u32(table2.id);
        let result = db.get_tables();
        assert_eq!(result, vec![(name0, id0), (name1, id1), (name2, id2)]);
    });
}

#[test]
fn test_create_table() {
    utils::run_test("test_create_table", |db| {
        let table = db.create_table("huobi.btc.usdt.1m").unwrap();
        assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
        let table = db.create_table("huobi.btc.usdt.5m").unwrap();
        assert_eq!(table.id, [0, 0, 4, 1]);
    })
}

#[test]
fn test_find_next_id() {
    utils::run_test("test_find_next_id", |db| {
        let id = db.find_next_id();
        assert_eq!(id, MIN_USERLAND_TABLE_ID);
    })
}

#[test]
fn test_register_table() {
    utils::run_test("test_register_table", |db| {
        let name = "huobi.btc.usdt.1m";
        let name_clone = name.clone();
        let table = db.new_table(name).unwrap();
        let name_to_id_table_inner_key = utils::build_name_to_id_table_inner_key(&name_clone);
        let id_to_name_table_inner_key =
            utils::build_id_to_name_table_inner_key(MIN_USERLAND_TABLE_ID);
        let result = db.register_table(
            &name_to_id_table_inner_key,
            MIN_USERLAND_TABLE_ID,
            &id_to_name_table_inner_key,
            &name_clone,
        );
        assert!(result.is_ok());

        let id = table.engine.get(name_to_id_table_inner_key);
        assert_eq!(id.unwrap().unwrap().as_ref(), [0, 0, 4, 0]);

        let name = table.engine.get(id_to_name_table_inner_key);
        assert_eq!(
            name.unwrap().unwrap().to_utf8().unwrap(),
            "huobi.btc.usdt.1m"
        );
    })
}
