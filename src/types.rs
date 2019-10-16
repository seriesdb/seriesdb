pub type Engine = rocksdb::DB;
pub type Error = rocksdb::Error;

pub type TableId = [u8; 4];
pub(in crate) type ItemId = [u8; 2];
