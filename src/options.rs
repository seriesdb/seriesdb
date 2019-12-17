use crate::consts::*;
use rocksdb::{DBCompactionStyle, Options as InnerOptions, SliceTransform};

pub struct Options {
    pub(in crate) inner: InnerOptions,
}

impl Options {
    pub fn new() -> Self {
        Options {
            inner: Self::build_default_options(),
        }
    }

    pub fn set_write_buffer_size(&mut self, size: usize) {
        self.inner.set_write_buffer_size(size);
    }

    pub fn set_max_write_buffer_number(&mut self, size: i32) {
        self.inner.set_max_write_buffer_number(size);
    }

    pub fn set_min_write_buffer_number_to_merge(&mut self, size: i32) {
        self.inner.set_min_write_buffer_number_to_merge(size);
    }

    pub fn set_max_background_compactions(&mut self, size: i32) {
        self.inner.set_max_background_compactions(size);
    }

    pub fn set_max_background_flushes(&mut self, size: i32) {
        self.inner.set_max_background_flushes(size);
    }

    fn build_default_options() -> InnerOptions {
        let mut opts = InnerOptions::default();
        opts.create_if_missing(true);
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(TABLE_ID_LEN));
        opts.set_max_open_files(-1);
        opts.set_use_fsync(false);
        opts.set_table_cache_num_shard_bits(4);
        opts.set_write_buffer_size(134217728);
        opts.set_max_write_buffer_number(4);
        opts.set_min_write_buffer_number_to_merge(2);
        opts.set_max_bytes_for_level_base(1073741824);
        opts.set_max_bytes_for_level_multiplier(8.0);
        opts.set_target_file_size_base(134217728);
        opts.set_target_file_size_multiplier(8);
        opts.set_disable_auto_compactions(false);
        opts.set_compaction_style(DBCompactionStyle::Level);
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_max_background_compactions(4);
        opts.set_max_background_flushes(4);
        opts
    }
}
