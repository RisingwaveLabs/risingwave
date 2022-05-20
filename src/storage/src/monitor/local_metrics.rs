use crate::monitor::StateStoreMetrics;

#[derive(Default)]
pub struct StoreLocalMetrics {
    pub cache_data_block_miss: u64,
    pub cache_data_block_total: u64,
    pub cache_meta_block_miss: u64,
    pub cache_meta_block_total: u64,

    // include multiple versions of one key.
    pub scan_key_count: u64,
    pub processed_key_count: u64,
}

impl StoreLocalMetrics {
    pub fn add(&mut self, other: &StoreLocalMetrics) {
        self.cache_meta_block_miss += other.cache_data_block_miss;
        self.cache_meta_block_total += other.cache_meta_block_total;

        self.cache_data_block_miss += other.cache_data_block_miss;
        self.cache_data_block_total += other.cache_data_block_total;

        self.scan_key_count += other.scan_key_count;
        self.processed_key_count += other.processed_key_count;
    }

    pub fn report(&self, metrics: &StateStoreMetrics) {
        metrics
            .sst_store_block_request_counts
            .with_label_values(&["data_total"])
            .inc_by(self.cache_data_block_total);
        metrics
            .sst_store_block_request_counts
            .with_label_values(&["data_miss"])
            .inc_by(self.cache_data_block_miss);
        metrics
            .sst_store_block_request_counts
            .with_label_values(&["meta_total"])
            .inc_by(self.cache_meta_block_total);
        metrics
            .sst_store_block_request_counts
            .with_label_values(&["meta_miss"])
            .inc_by(self.cache_meta_block_total);
    }
}
