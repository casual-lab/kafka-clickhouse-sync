// src/models/event.rs - 链上事件数据模型

use clickhouse::Row;
use serde::{Deserialize, Serialize};
use base64::Engine;

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct ChainEventRow {
    pub slot: u64,
    pub tx_signature: String,
    pub tx_index: u32,
    pub log_index: u32,
    pub program: String,
    pub event_type: String,
    pub discriminator: String,
    pub payload_json: String,
    pub raw_data: String,
    pub timestamp: u64,         // Unix timestamp
    pub ingested_at: u64,       // Unix timestamp
}

impl ChainEventRow {
    pub fn from_proto(
        event: &crate::proto::blockdaemon::solana::accountsdb_plugin_kafka::types::ChainEvent
    ) -> anyhow::Result<Self> {
        // 从 slot 推算时间戳 (slot * 400ms)
        let timestamp = (event.slot as f64 * 0.4) as u64;
        let ingested_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Ok(Self {
            slot: event.slot,
            tx_signature: hex::encode(&event.tx_signature),
            tx_index: event.tx_index,
            log_index: event.log_index,
            program: event.program_id.clone(),
            event_type: event.event_type.clone(),
            discriminator: hex::encode(&event.event_discriminator),
            payload_json: event.json_payload.clone().unwrap_or_default(),
            raw_data: base64::engine::general_purpose::STANDARD.encode(&event.raw_data),
            timestamp,
            ingested_at,
        })
    }
}
