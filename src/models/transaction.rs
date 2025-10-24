// src/models/transaction.rs - 交易数据模型

use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct TransactionRow {
    // 字段顺序必须与 ClickHouse 表定义完全一致
    pub slot: u64,
    pub tx_signature: String,
    pub tx_index: u64,
    pub signer: String,
    pub num_signers: u32,
    pub num_instructions: u32,
    pub num_accounts: u32,
    pub is_vote: u8,
    pub is_successful: u8,
    pub error: String,
    pub fee: u64,
    pub compute_units_consumed: u64,
    pub compute_units_price: u64,
    pub total_cost: u64,
    pub timestamp: u64,              // Unix timestamp
    pub ingested_at: u64,            // Unix timestamp
    pub instructions_json: String,
}

impl TransactionRow {
    pub fn from_proto(
        tx_event: &crate::proto::blockdaemon::solana::accountsdb_plugin_kafka::types::TransactionEvent
    ) -> anyhow::Result<Self> {
        // 从 slot 推算时间戳 (slot * 400ms)
        let timestamp = (tx_event.slot as f64 * 0.4) as u64;
        let ingested_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // 提取错误信息
        let error = if !tx_event.is_successful {
            tx_event.error_details.join("; ")
        } else {
            String::new()
        };
        
        // 提取账户列表和指令信息
        let (account_keys, instructions) = 
            if let Some(tx) = &tx_event.transaction {
                if let Some(msg_payload) = &tx.message {
                    match &msg_payload.message_payload {
                        Some(crate::proto::blockdaemon::solana::accountsdb_plugin_kafka::types::sanitized_message::MessagePayload::Legacy(legacy)) => {
                            if let Some(msg) = &legacy.message {
                                let account_keys: Vec<String> = msg.account_keys.iter()
                                    .map(|key| bs58::encode(key).into_string())
                                    .collect();
                                
                                let instructions: Vec<serde_json::Value> = msg.instructions.iter()
                                    .enumerate()
                                    .map(|(idx, inst)| {
                                        let program_id_index = inst.program_id_index as usize;
                                        let program_id = account_keys.get(program_id_index)
                                            .cloned()
                                            .unwrap_or_else(|| "unknown".to_string());
                                        
                                        serde_json::json!({
                                            "index": idx,
                                            "program_id": program_id,
                                            "program_id_index": inst.program_id_index,
                                            "accounts": inst.accounts,
                                            "data": hex::encode(&inst.data)
                                        })
                                    })
                                    .collect();
                                
                                (account_keys, instructions)
                            } else {
                                (vec![], vec![])
                            }
                        }
                        Some(crate::proto::blockdaemon::solana::accountsdb_plugin_kafka::types::sanitized_message::MessagePayload::V0(v0)) => {
                            if let Some(msg) = &v0.message {
                                let account_keys: Vec<String> = msg.account_keys.iter()
                                    .map(|key| bs58::encode(key).into_string())
                                    .collect();
                                
                                let instructions: Vec<serde_json::Value> = msg.instructions.iter()
                                    .enumerate()
                                    .map(|(idx, inst)| {
                                        let program_id_index = inst.program_id_index as usize;
                                        let program_id = account_keys.get(program_id_index)
                                            .cloned()
                                            .unwrap_or_else(|| "unknown".to_string());
                                        
                                        serde_json::json!({
                                            "index": idx,
                                            "program_id": program_id,
                                            "program_id_index": inst.program_id_index,
                                            "accounts": inst.accounts,
                                            "data": hex::encode(&inst.data)
                                        })
                                    })
                                    .collect();
                                
                                (account_keys, instructions)
                            } else {
                                (vec![], vec![])
                            }
                        }
                        None => (vec![], vec![]),
                    }
                } else {
                    (vec![], vec![])
                }
            } else {
                (vec![], vec![])
            };
        
        // 提取第一个签名者（主签名者）
        let signer = account_keys.first().cloned().unwrap_or_default();
        
        // 计算签名者数量（从 header 中获取）
        let num_signers = if let Some(tx) = &tx_event.transaction {
            if let Some(msg_payload) = &tx.message {
                match &msg_payload.message_payload {
                    Some(crate::proto::blockdaemon::solana::accountsdb_plugin_kafka::types::sanitized_message::MessagePayload::Legacy(legacy)) => {
                        if let Some(msg) = &legacy.message {
                            msg.header.as_ref().map(|h| h.num_required_signatures).unwrap_or(0)
                        } else {
                            0
                        }
                    }
                    Some(crate::proto::blockdaemon::solana::accountsdb_plugin_kafka::types::sanitized_message::MessagePayload::V0(v0)) => {
                        if let Some(msg) = &v0.message {
                            msg.header.as_ref().map(|h| h.num_required_signatures).unwrap_or(0)
                        } else {
                            0
                        }
                    }
                    None => 0,
                }
            } else {
                0
            }
        } else {
            0
        };
        
        // 提取计算单元和费用信息
        let (fee, compute_units_consumed, compute_units_price) = 
            if let Some(meta) = &tx_event.transaction_status_meta {
                let cu_consumed = meta.compute_units_consumed as u64;
                let cu_price = if cu_consumed > 0 && meta.fee > 0 {
                    meta.fee / cu_consumed
                } else {
                    0
                };
                (meta.fee, cu_consumed, cu_price)
            } else {
                (0, 0, 0)
            };
        
        // 计算总成本（费用）
        let total_cost = fee;
        
        // 生成 instructions JSON
        let instructions_json = serde_json::to_string(&instructions)?;
        
        Ok(Self {
            slot: tx_event.slot,
            tx_signature: bs58::encode(&tx_event.signature).into_string(),
            tx_index: tx_event.index,
            signer,
            num_signers,
            num_instructions: tx_event.instruction_count,
            num_accounts: account_keys.len() as u32,
            is_vote: if tx_event.is_vote { 1 } else { 0 },
            is_successful: if tx_event.is_successful { 1 } else { 0 },
            error,
            fee,
            compute_units_consumed,
            compute_units_price,
            total_cost,
            timestamp,
            ingested_at,
            instructions_json,
        })
    }
}
