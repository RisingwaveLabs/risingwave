use itertools::Itertools;

use crate::PrometheusConfig;

pub struct PrometheusGen;

impl PrometheusGen {
    pub fn gen_prometheus_yml(&self, config: &PrometheusConfig) -> String {
        let prometheus_host = &config.address;
        let prometheus_port = config.port;
        let compute_node_targets = config
            .provide_compute_node
            .as_ref()
            .unwrap()
            .iter()
            .map(|node| format!("\"{}:{}\"", node.exporter_address, node.exporter_port))
            .join(",");

        let meta_node_targets = config
            .provide_meta_node
            .as_ref()
            .unwrap()
            .iter()
            .map(|node| format!("\"{}:{}\"", node.exporter_address, node.exporter_port))
            .join(",");

        let minio_targets = config
            .provide_minio
            .as_ref()
            .unwrap()
            .iter()
            .map(|node| format!("\"{}:{}\"", node.address, node.port))
            .join(",");

        format!(
            r#"# --- THIS FILE IS AUTO GENERATED BY RISEDEV ---
global:
  scrape_interval: 1s
  evaluation_interval: 5s

scrape_configs:
  - job_name: "prometheus"
    static_configs:
      - targets: ["{prometheus_host}:{prometheus_port}"]

  - job_name: compute-job
    static_configs:
      - targets: [{compute_node_targets}]

  - job_name: meta-job
    static_configs:
      - targets: [{meta_node_targets}]
  
  - job_name: minio-job
    metrics_path: /minio/v2/metrics/cluster
    static_configs:
    - targets: [{minio_targets}]
"#,
        )
    }
}
