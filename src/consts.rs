// 1024 as BigEndian
pub(in crate) const MIN_USERLAND_TABLE_ID: [u8; 4] = [0, 0, 4, 0];

// 4294967294 as BigEndian
pub(in crate) const MAX_USERLAND_TABLE_ID: [u8; 4] = [255, 255, 255, 254];

// 0 as BigEndian
pub(in crate) const NAME_TO_ID_TABLE_ID: [u8; 4] = [0, 0, 0, 0];

// 1 as BigEndian
pub(in crate) const ID_TO_NAME_TABLE_ID: [u8; 4] = [0, 0, 0, 1];
