// src/models/log.rs - 程序日志数据模型

use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct ProgramLogRow {
    pub slot: u64,
    pub tx_signature: String,
    pub tx_index: u32,
    pub log_index: u32,
    pub program_id: String,
    pub message: String,
    pub log_type: i32,
    pub depth: u32,
    pub timestamp: u64,         // Unix timestamp
    pub ingested_at: u64,       // Unix timestamp
}

impl ProgramLogRow {
    pub fn from_proto(
        log_event: &crate::proto::blockdaemon::solana::accountsdb_plugin_kafka::types::ProgramLogEvent
    ) -> anyhow::Result<Self> {
        // 从 slot 推算时间戳 (slot * 400ms)
        let timestamp = (log_event.slot as f64 * 0.4) as u64;
        let ingested_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Ok(Self {
            slot: log_event.slot,
            tx_signature: hex::encode(&log_event.tx_signature),
            tx_index: log_event.tx_index,
            log_index: log_event.log_index,
            program_id: log_event.program_id.clone(),
            message: log_event.message.clone(),
            log_type: log_event.log_type,
            depth: log_event.depth,
            timestamp,
            ingested_at,
        })
    }
}
