// src/models/block.rs - 区块数据模型

use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct BlockRow {
    pub slot: u64,
    pub blockhash: String,
    pub parent_slot: u64,
    pub parent_blockhash: String,
    pub leader: String,
    pub num_txs: u32,
    pub status: String,
    pub timestamp: u64,                    // Unix timestamp
    pub ingested_at: u64,                  // Unix timestamp
    pub executed_transaction_count: u32,
    pub total_compute_units: u64,
    pub total_fees: u64,
}

impl BlockRow {
    pub fn from_proto(
        block_event: &crate::proto::blockdaemon::solana::accountsdb_plugin_kafka::types::BlockMetadataEvent
    ) -> anyhow::Result<Self> {
        let timestamp = block_event.block_time.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
        }) as u64;
        
        let ingested_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Ok(Self {
            slot: block_event.slot,
            blockhash: block_event.block_hash.clone(),
            parent_slot: block_event.parent_slot,
            parent_blockhash: block_event.parent_block_hash.clone(),
            leader: block_event.leader.clone(),
            num_txs: block_event.executed_transaction_count,
            status: "finalized".to_string(),
            timestamp,
            ingested_at,
            executed_transaction_count: block_event.executed_transaction_count,
            total_compute_units: block_event.total_compute_units as u64,
            total_fees: block_event.total_fees,
        })
    }
}
