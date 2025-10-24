// src/kafka/message_handler.rs - Kafka 消息处理器

use anyhow::{Context, Result};
use prost::Message;
use tracing::{debug, error, info, warn};

use crate::clickhouse::ClickHouseWriter;
use crate::models::{AccountRow, BlockRow, ChainEventRow, ProgramLogRow, TransactionRow};
use crate::proto::blockdaemon::solana::accountsdb_plugin_kafka::types::{
    BlockMetadataEvent, ChainEvent, ProgramLogEvent, TransactionEvent, UpdateAccountEvent,
};

/// 消息处理统计
#[derive(Debug, Clone, Default)]
pub struct MessageStats {
    pub blocks_processed: u64,
    pub transactions_processed: u64,
    pub accounts_processed: u64,
    pub logs_processed: u64,
    pub events_processed: u64,
    pub errors: u64,
}

/// Kafka 消息处理器
pub struct MessageHandler {
    stats: MessageStats,
}

impl MessageHandler {
    pub fn new() -> Self {
        Self {
            stats: MessageStats::default(),
        }
    }

    /// 处理单个 Kafka 消息
    pub async fn handle_message(
        &mut self,
        topic: &str,
        payload: &[u8],
        writer: &mut ClickHouseWriter,
    ) -> Result<()> {
        debug!("Processing message from topic: {}, size: {} bytes", topic, payload.len());

        let result = match topic {
            t if t.contains("chain_blocks") => self.handle_block_message(payload, writer).await,
            t if t.contains("chain_txs") => self.handle_transaction_message(payload, writer).await,
            t if t.contains("chain_accounts") => self.handle_account_message(payload, writer).await,
            t if t.contains("chain_program_logs") => self.handle_log_message(payload, writer).await,
            t if t.contains("chain_events") => self.handle_event_message(payload, writer).await,
            _ => {
                warn!("Unknown topic: {}", topic);
                Err(anyhow::anyhow!("Unknown topic: {}", topic))
            }
        };

        if let Err(e) = &result {
            self.stats.errors += 1;
            error!("Error processing message from topic {}: {:?}", topic, e);
        }

        result
    }

    /// 处理区块消息
    async fn handle_block_message(
        &mut self,
        payload: &[u8],
        writer: &mut ClickHouseWriter,
    ) -> Result<()> {
        let event = BlockMetadataEvent::decode(payload)
            .context("Failed to decode BlockMetadataEvent")?;
        
        debug!("Decoded BlockMetadataEvent for slot {}", event.slot);
        
        let row = BlockRow::from_proto(&event)
            .context("Failed to convert BlockMetadataEvent to BlockRow")?;
        
        writer.insert_block(row).await?;
        self.stats.blocks_processed += 1;
        
        Ok(())
    }

    /// 处理交易消息
    async fn handle_transaction_message(
        &mut self,
        payload: &[u8],
        writer: &mut ClickHouseWriter,
    ) -> Result<()> {
        let event = TransactionEvent::decode(payload)
            .context("Failed to decode TransactionEvent")?;
        
        debug!("Decoded TransactionEvent for slot {} index {}", event.slot, event.index);
        
        let row = TransactionRow::from_proto(&event)
            .context("Failed to convert TransactionEvent to TransactionRow")?;
        
        writer.insert_transaction(row).await?;
        self.stats.transactions_processed += 1;
        
        Ok(())
    }

    /// 处理账户消息
    async fn handle_account_message(
        &mut self,
        payload: &[u8],
        writer: &mut ClickHouseWriter,
    ) -> Result<()> {
        let event = UpdateAccountEvent::decode(payload)
            .context("Failed to decode UpdateAccountEvent")?;
        
        debug!("Decoded UpdateAccountEvent for slot {} pubkey {}", 
            event.slot, bs58::encode(&event.pubkey).into_string());
        
        let row = AccountRow::from_proto(&event)
            .context("Failed to convert UpdateAccountEvent to AccountRow")?;
        
        writer.insert_account(row).await?;
        self.stats.accounts_processed += 1;
        
        Ok(())
    }

    /// 处理程序日志消息
    async fn handle_log_message(
        &mut self,
        payload: &[u8],
        writer: &mut ClickHouseWriter,
    ) -> Result<()> {
        let event = ProgramLogEvent::decode(payload)
            .context("Failed to decode ProgramLogEvent")?;
        
        debug!("Decoded ProgramLogEvent for slot {} tx_index {}", event.slot, event.tx_index);
        
        let row = ProgramLogRow::from_proto(&event)
            .context("Failed to convert ProgramLogEvent to ProgramLogRow")?;
        
        writer.insert_program_log(row).await?;
        self.stats.logs_processed += 1;
        
        Ok(())
    }

    /// 处理链上事件消息
    async fn handle_event_message(
        &mut self,
        payload: &[u8],
        writer: &mut ClickHouseWriter,
    ) -> Result<()> {
        let event = ChainEvent::decode(payload)
            .context("Failed to decode ChainEvent")?;
        
        debug!("Decoded ChainEvent for slot {} program {}", event.slot, event.program_id);
        
        let row = ChainEventRow::from_proto(&event)
            .context("Failed to convert ChainEvent to ChainEventRow")?;
        
        writer.insert_chain_event(row).await?;
        self.stats.events_processed += 1;
        
        Ok(())
    }

    /// 获取统计信息
    pub fn stats(&self) -> &MessageStats {
        &self.stats
    }

    /// 重置统计信息
    pub fn reset_stats(&mut self) {
        self.stats = MessageStats::default();
    }

    /// 打印统计信息
    pub fn log_stats(&self) {
        info!(
            "Message processing stats - Blocks: {}, Transactions: {}, Accounts: {}, Logs: {}, Events: {}, Errors: {}",
            self.stats.blocks_processed,
            self.stats.transactions_processed,
            self.stats.accounts_processed,
            self.stats.logs_processed,
            self.stats.events_processed,
            self.stats.errors
        );
    }
}

impl Default for MessageHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_handler_creation() {
        let handler = MessageHandler::new();
        assert_eq!(handler.stats.blocks_processed, 0);
        assert_eq!(handler.stats.transactions_processed, 0);
        assert_eq!(handler.stats.accounts_processed, 0);
        assert_eq!(handler.stats.logs_processed, 0);
        assert_eq!(handler.stats.events_processed, 0);
        assert_eq!(handler.stats.errors, 0);
    }

    #[test]
    fn test_stats_reset() {
        let mut handler = MessageHandler::new();
        handler.stats.blocks_processed = 10;
        handler.stats.errors = 5;
        
        handler.reset_stats();
        
        assert_eq!(handler.stats.blocks_processed, 0);
        assert_eq!(handler.stats.errors, 0);
    }
}
