use crate::consts::*;
#[cfg(test)]
use crate::db::Db;
use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;

////////////////////////////////////////////////////////////////////////////////
/// conversion utils
////////////////////////////////////////////////////////////////////////////////
#[inline]
pub fn u32_to_u8x4(u32: u32) -> [u8; 4] {
    let mut buf = [0; 4];
    BigEndian::write_u32(&mut buf, u32);
    buf
}

#[inline]
pub fn u8x4_to_u32(u8x4: [u8; 4]) -> u32 {
    BigEndian::read_u32(&u8x4)
}

#[inline]
pub fn u8s_to_u32(u8s: &[u8]) -> u32 {
    BigEndian::read_u32(u8s)
}

////////////////////////////////////////////////////////////////////////////////
/// key utils
////////////////////////////////////////////////////////////////////////////////
#[inline]
pub fn build_inner_key<K: AsRef<[u8]>>(table_id: [u8; 4], key: K) -> Bytes {
    let key = key.as_ref();
    let table_id = table_id.as_ref();
    let mut buf = Bytes::with_capacity(table_id.len() + key.len());
    buf.extend_from_slice(table_id);
    buf.extend_from_slice(key);
    buf
}

#[inline]
pub fn build_name_to_id_table_inner_key<N: AsRef<[u8]>>(name: N) -> Bytes {
    build_inner_key(NAME_TO_ID_TABLE_ID, name)
}

#[inline]
pub fn build_id_to_name_table_inner_key(table_id: [u8; 4]) -> Bytes {
    build_inner_key(ID_TO_NAME_TABLE_ID, table_id)
}

#[inline]
pub fn build_id_to_name_table_anchor() -> Bytes {
    let key: [u8; 4] = [255, 255, 255, 255];
    build_inner_key(ID_TO_NAME_TABLE_ID, key)
}

#[inline]
pub fn build_name_to_id_table_anchor() -> Bytes {
    build_inner_key(NAME_TO_ID_TABLE_ID, set_every_bit_to_one(1025))
}

#[inline]
pub fn build_userland_table_anchor(table_id: [u8; 4], key_len: u8) -> Bytes {
    build_inner_key(table_id, set_every_bit_to_one((key_len + 1).into()))
}

#[inline]
pub fn extract_table_id<B: AsRef<[u8]>>(buf: B) -> [u8; 4] {
    let mut array: [u8; 4] = [0; 4];
    array.copy_from_slice(&buf.as_ref()[..4]);
    array
}

#[inline]
pub fn extract_key(buf: &[u8]) -> &[u8] {
    &buf[4..]
}

#[inline]
fn set_every_bit_to_one(key_len: u16) -> Bytes {
    Bytes::from(vec![255; key_len.into()])
}

////////////////////////////////////////////////////////////////////////////////
/// unit test utils
////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
pub(in crate) fn run_test<T>(db_name: &str, test: T) -> ()
where
    T: FnOnce(Db) -> () + std::panic::UnwindSafe,
{
    let mut path = String::from("./data/");
    path.push_str(db_name);
    let db = setup(&path);
    let result = std::panic::catch_unwind(|| {
        test(db);
    });
    teardown(&path);
    assert!(result.is_ok())
}

#[cfg(test)]
fn setup(path: &str) -> Db {
    let result = Db::new(path);
    assert!(result.is_ok());
    result.unwrap()
}

#[cfg(test)]
fn teardown(path: &str) {
    assert!(Db::destroy(path).is_ok())
}

////////////////////////////////////////////////////////////////////////////////
/// test cases
////////////////////////////////////////////////////////////////////////////////
#[test]
fn test_build_inner_key() {
    let inner_key = build_inner_key([0, 0, 4, 0], [0, 0, 0, 0]);
    assert_eq!(inner_key, vec![0, 0, 4, 0, 0, 0, 0, 0]);
}

#[test]
fn test_build_name_to_id_table_inner_key() {
    assert_eq!(
        build_name_to_id_table_inner_key("huobi.btc.usdt.1m"),
        vec![
            0, 0, 0, 0, 104, 117, 111, 98, 105, 46, 98, 116, 99, 46, 117, 115, 100, 116, 46, 49,
            109
        ]
    );
}

#[test]
fn test_build_id_to_name_table_inner_key() {
    assert_eq!(
        build_id_to_name_table_inner_key([0, 0, 4, 0]),
        vec![0, 0, 0, 1, 0, 0, 4, 0]
    );
}

#[test]
fn test_build_id_to_name_table_anchor() {
    assert_eq!(
        build_id_to_name_table_anchor(),
        vec![0, 0, 0, 1, 255, 255, 255, 255]
    );
}

#[test]
fn test_build_userland_table_anchor() {
    assert_eq!(
        build_userland_table_anchor([0, 0, 4, 0], 4),
        vec![0, 0, 4, 0, 255, 255, 255, 255, 255]
    );
}

#[test]
fn test_extract_table_id() {
    let inner_key = [0, 0, 4, 0, 0, 0, 0, 0];
    let table_id = extract_table_id(inner_key);
    assert_eq!(table_id, [0, 0, 4, 0]);
}

#[test]
fn test_extract_key() {
    let inner_key = [0, 0, 4, 0, 0, 0, 0, 128, 0, 254];
    let table_id = extract_key(&inner_key);
    assert_eq!(table_id, [0, 0, 0, 128, 0, 254]);
}
