use crate::consts::*;
use rocksdb::Options as InnerOptions;

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
        opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(TABLE_ID_LEN));
        opts.set_max_open_files(-1);
        opts.set_use_fsync(false);
        opts.set_bytes_per_sync(8388608);
        opts.set_table_cache_num_shard_bits(6);
        opts.set_write_buffer_size(268435456);
        opts.set_max_write_buffer_number(4);
        opts.set_min_write_buffer_number_to_merge(2);
        opts.set_target_file_size_base(1073741824);
        opts.set_level_zero_stop_writes_trigger(1024);
        opts.set_level_zero_slowdown_writes_trigger(800);
        opts.set_compaction_style(rocksdb::DBCompactionStyle::Universal);
        opts.set_max_background_compactions(4);
        opts.set_max_background_flushes(4);
        opts.set_disable_auto_compactions(true);
        opts
    }
}
