// src/models/account.rs - 账户数据模型

use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct AccountRow {
    // 字段顺序必须与 ClickHouse 表定义完全一致
    pub slot: u64,
    pub pubkey: String,
    pub owner_program: String,
    pub lamports: u64,
    pub executable: u8,
    pub rent_epoch: u64,
    pub data_len: u32,
    pub data_hash: String,
    pub write_version: u64,
    pub txn_signature: String,
    pub timestamp: u64,        // Unix timestamp
    pub ingested_at: u64,      // Unix timestamp
}

impl AccountRow {
    pub fn from_proto(
        account_event: &crate::proto::blockdaemon::solana::accountsdb_plugin_kafka::types::UpdateAccountEvent
    ) -> anyhow::Result<Self> {
        // 从 slot 推算时间戳 (slot * 400ms)
        let timestamp = (account_event.slot as f64 * 0.4) as u64;
        let ingested_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // 计算 data hash
        let data_hash = if !account_event.data.is_empty() {
            use sha2::{Sha256, Digest};
            let mut hasher = Sha256::new();
            hasher.update(&account_event.data);
            hex::encode(hasher.finalize())
        } else {
            String::new()
        };
        
        // 处理可选的交易签名
        let txn_signature = account_event.txn_signature.as_ref()
            .map(|sig| bs58::encode(sig).into_string())
            .unwrap_or_default();
        
        Ok(Self {
            slot: account_event.slot,
            pubkey: bs58::encode(&account_event.pubkey).into_string(),
            owner_program: bs58::encode(&account_event.owner).into_string(),
            lamports: account_event.lamports,
            executable: if account_event.executable { 1 } else { 0 },
            rent_epoch: account_event.rent_epoch,
            data_len: account_event.data.len() as u32,
            data_hash,
            write_version: account_event.write_version,
            txn_signature,
            timestamp,
            ingested_at,
        })
    }
}
