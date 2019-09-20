pub mod batch;
mod consts;
pub mod db;
pub mod iterator;
pub mod options;
pub mod table;
pub mod utils;

pub type Engine = rocksdb::DB;
pub type Error = rocksdb::Error;
