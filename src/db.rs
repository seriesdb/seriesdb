use crate::consts::*;
use crate::options::Options;
use crate::table::Table;
use crate::types::*;
use crate::utils;
use crate::Engine;
use crate::Error;
use rocksdb::WriteBatch;
use std::path::Path;

pub struct Db {
    pub(in crate) engine: Engine,
}

impl Db {
    #[inline]
    pub fn new<P: AsRef<Path>>(path: P, opts: &Options) -> Result<Db, Error> {
        Ok(Db {
            engine: Engine::open(&opts.inner, path)?,
        })
    }

    #[inline]
    pub fn destroy<P: AsRef<Path>>(path: P) -> Result<(), Error> {
        Engine::destroy(&Options::new().inner, path)
    }

    pub fn new_table(&self, name: &str) -> Result<Table, Error> {
        if let Some(id) = self.get_table_id_by_name(name)? {
            Ok(Table {
                engine: &self.engine,
                id,
                anchor: utils::build_userland_table_anchor(id, MAX_USERLAND_KEY_LEN),
            })
        } else {
            Ok(self.create_table(name)?)
        }
    }

    pub fn rename_table(&self, old_name: &str, new_name: &str) -> Result<(), Error> {
        let mut batch = WriteBatch::default();
        if let Some(id) = self.get_table_id_by_name(old_name)? {
            let id_to_name_table_inner_key = utils::build_id_to_name_table_inner_key(id);
            batch.delete(&utils::build_name_to_id_table_inner_key(old_name))?;
            batch.delete(&id_to_name_table_inner_key)?;
            batch.put(utils::build_name_to_id_table_inner_key(new_name), id)?;
            batch.put(id_to_name_table_inner_key, new_name)?;
        }
        self.engine.write(batch)
    }

    pub fn destroy_table(&self, name: &str) -> Result<(), Error> {
        let mut batch = WriteBatch::default();
        if let Some(id) = self.get_table_id_by_name(name)? {
            batch.delete(&utils::build_name_to_id_table_inner_key(name))?;
            batch.delete(&utils::build_id_to_name_table_inner_key(id))?;
            batch.delete_range(
                id,
                utils::build_userland_table_anchor(id, MAX_USERLAND_KEY_LEN),
            )?;
        }
        self.engine.write(batch)
    }

    pub fn get_tables(&self) -> Vec<(String, u32)> {
        let mut result: Vec<(String, u32)> = Vec::new();
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        let mut iter = self.engine.raw_iterator_opt(&opts);
        iter.seek(ID_TO_NAME_TABLE_ID);
        while iter.valid() {
            let key = unsafe { iter.key_inner().unwrap() };
            let value = unsafe { iter.value_inner().unwrap() };
            let id = utils::u8s_to_u32(utils::extract_key(key));
            let name = std::str::from_utf8(value).unwrap().to_string();
            result.push((name, id));
            iter.next();
        }
        result
    }

    pub fn get_table_id_by_name(&self, name: &str) -> Result<Option<TableId>, Error> {
        let name_to_id_table_inner_key = utils::build_name_to_id_table_inner_key(name);
        if let Some(id) = self.engine.get(name_to_id_table_inner_key)? {
            Ok(Some(utils::u8s_to_table_id(id.as_ref())))
        } else {
            Ok(None)
        }
    }

    pub fn get_table_name_by_id(&self, id: TableId) -> Result<Option<String>, Error> {
        let id_to_name_table_inner_key = utils::build_id_to_name_table_inner_key(id);
        if let Some(name) = self.engine.get(id_to_name_table_inner_key)? {
            Ok(Some(
                std::str::from_utf8(name.as_ref()).unwrap().to_string(),
            ))
        } else {
            Ok(None)
        }
    }

    fn create_table(&self, name: &str) -> Result<Table, Error> {
        let name_to_id_table_inner_key = utils::build_name_to_id_table_inner_key(name);
        let id = self.generate_next_table_id()?;
        let id_to_name_table_inner_key = utils::build_id_to_name_table_inner_key(id);
        self.register_table(
            name_to_id_table_inner_key,
            id,
            id_to_name_table_inner_key,
            name,
        )?;
        let anchor = utils::build_userland_table_anchor(id, MAX_USERLAND_KEY_LEN);
        Ok(Table::new(&self.engine, id, anchor))
    }

    fn generate_next_table_id(&self) -> Result<TableId, Error> {
        let seed_key = utils::build_info_table_inner_key(SEED_ITEM_ID);
        if let Some(seed_value) = self.engine.get(&seed_key)? {
            let seed_value = utils::u8s_to_u32(seed_value.as_ref());
            if utils::u32_to_table_id(seed_value) >= MAX_USERLAND_TABLE_ID {
                panic!("Exceeded limit: {:?}", MAX_USERLAND_TABLE_ID)
            }
            let seed_value = seed_value + 1;
            let next_id = utils::u32_to_table_id(seed_value);
            self.engine.put(seed_key, next_id)?;
            Ok(next_id)
        } else {
            self.engine.put(seed_key, MIN_USERLAND_TABLE_ID)?;
            Ok(MIN_USERLAND_TABLE_ID)
        }
    }

    #[inline]
    fn register_table<K: AsRef<[u8]>>(
        &self,
        name_to_id_table_inner_key: K,
        id: TableId,
        id_to_name_table_inner_key: K,
        name: &str,
    ) -> Result<(), Error> {
        let mut batch = WriteBatch::default();
        batch.put(name_to_id_table_inner_key, id)?;
        batch.put(id_to_name_table_inner_key, name)?;
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
        let id0 = utils::table_id_to_u32(table0.id);
        let id1 = utils::table_id_to_u32(table1.id);
        let id2 = utils::table_id_to_u32(table2.id);
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
fn test_generate_next_table_id() {
    utils::run_test("test_generate_next_table_id", |db| {
        let id = db.generate_next_table_id().unwrap();
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
