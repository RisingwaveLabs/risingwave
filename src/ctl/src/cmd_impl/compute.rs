use std::convert::TryFrom;

use risingwave_common::config::{BatchConfig, StreamingConfig};
use risingwave_common::util::addr::HostAddr;
use risingwave_rpc_client::ComputeClient;
use serde_json;

pub async fn show_config(host: &str) -> anyhow::Result<()> {
    let listen_address = HostAddr::try_from(host)?;
    let client = ComputeClient::new(listen_address).await?;
    let config_response = client.show_config().await?;
    let batch_config: BatchConfig = serde_json::from_str(&config_response.batch_config)?;
    let stream_config: StreamingConfig = serde_json::from_str(&config_response.stream_config)?;
    println!("{}", serde_json::to_string_pretty(&batch_config)?);
    println!("{}", serde_json::to_string_pretty(&stream_config)?);
    Ok(())
}
