// src/models/transaction.rs - 交易数据模型

use clickhouse::Row;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct TransactionRow {
    pub slot: u64,
    pub tx_signature: String,
    pub tx_index: u64,
    pub signer: String,
    pub num_signers: u32,
    pub num_instructions: u32,
    pub num_accounts: u32,
    pub is_vote: bool,
    pub is_successful: bool,
    pub error: String,
    pub fee: u64,
    pub compute_units_consumed: u64,
    pub compute_units_price: u64,
    pub total_cost: u64,
    pub timestamp: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub instructions_json: String,
}

impl TransactionRow {
    pub fn from_proto(
        tx_event: &crate::proto::blockdaemon::solana::accountsdb_plugin_kafka::types::TransactionEvent
    ) -> anyhow::Result<Self> {
        // 从 slot 推算时间戳 (slot * 400ms)
        let timestamp_secs = (tx_event.slot as f64 * 0.4) as i64;
        let timestamp = DateTime::from_timestamp(timestamp_secs, 0)
            .unwrap_or_else(|| Utc::now());
        
        // 提取第一个签名者（fee payer）
        let signer = if let Some(tx) = &tx_event.transaction {
            if let Some(msg_payload) = &tx.message {
                match &msg_payload.message_payload {
                    Some(crate::proto::blockdaemon::solana::accountsdb_plugin_kafka::types::sanitized_message::MessagePayload::Legacy(legacy)) => {
                        if let Some(msg) = &legacy.message {
                            msg.account_keys.first()
                                .map(|key| bs58::encode(key).into_string())
                                .unwrap_or_default()
                        } else {
                            String::new()
                        }
                    }
                    Some(crate::proto::blockdaemon::solana::accountsdb_plugin_kafka::types::sanitized_message::MessagePayload::V0(v0)) => {
                        if let Some(msg) = &v0.message {
                            msg.account_keys.first()
                                .map(|key| bs58::encode(key).into_string())
                                .unwrap_or_default()
                        } else {
                            String::new()
                        }
                    }
                    None => String::new(),
                }
            } else {
                String::new()
            }
        } else {
            String::new()
        };
        
        // 提取指令数量
        let num_instructions = tx_event.instruction_count;
        
        // 提取账户数量
        let num_accounts = tx_event.account_count;
        
        // 提取错误信息
        let error = if !tx_event.is_successful {
            tx_event.error_details.join("; ")
        } else {
            String::new()
        };
        
        // 提取费用
        let fee = if let Some(meta) = &tx_event.transaction_status_meta {
            meta.fee
        } else {
            0
        };
        
        Ok(Self {
            slot: tx_event.slot,
            tx_signature: hex::encode(&tx_event.signature),
            tx_index: tx_event.index,
            signer,
            num_signers: tx_event.account_count,
            num_instructions,
            num_accounts,
            is_vote: tx_event.is_vote,
            is_successful: tx_event.is_successful,
            error,
            fee,
            compute_units_consumed: tx_event.compute_units_consumed as u64,
            compute_units_price: tx_event.compute_units_price,
            total_cost: tx_event.total_cost,
            timestamp,
            ingested_at: Utc::now(),
            instructions_json: serde_json::to_string(&tx_event.execution_logs)?,
        })
    }
}
