// src/clickhouse/writer.rs - ClickHouse 批量写入器

use crate::config::ClickHouseConfig;
use crate::models::*;
use clickhouse::Client;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

pub struct ClickHouseWriter {
    client: Client,
    batch_size: usize,
    flush_interval: Duration,
    max_retries: u32,
    
    // 批量缓冲区
    block_buffer: Vec<BlockRow>,
    tx_buffer: Vec<TransactionRow>,
    account_buffer: Vec<AccountRow>,
    log_buffer: Vec<ProgramLogRow>,
    event_buffer: Vec<ChainEventRow>,
}

impl ClickHouseWriter {
    pub fn new(config: &ClickHouseConfig) -> anyhow::Result<Self> {
        let client = Client::default()
            .with_url(&config.url)
            .with_database(&config.database)
            .with_user(&config.user)
            .with_password(&config.password);
        
        Ok(Self {
            client,
            batch_size: config.batch_size,
            flush_interval: Duration::from_secs(config.flush_interval_secs),
            max_retries: config.max_retries,
            block_buffer: Vec::with_capacity(config.batch_size),
            tx_buffer: Vec::with_capacity(config.batch_size),
            account_buffer: Vec::with_capacity(config.batch_size),
            log_buffer: Vec::with_capacity(config.batch_size),
            event_buffer: Vec::with_capacity(config.batch_size),
        })
    }
    
    /// 插入区块数据
    pub async fn insert_block(&mut self, row: BlockRow) -> anyhow::Result<()> {
        self.block_buffer.push(row);
        
        if self.block_buffer.len() >= self.batch_size {
            self.flush_blocks().await?;
        }
        
        Ok(())
    }
    
    /// 插入交易数据
    pub async fn insert_transaction(&mut self, row: TransactionRow) -> anyhow::Result<()> {
        self.tx_buffer.push(row);
        
        if self.tx_buffer.len() >= self.batch_size {
            self.flush_transactions().await?;
        }
        
        Ok(())
    }
    
    /// 插入账户数据
    pub async fn insert_account(&mut self, row: AccountRow) -> anyhow::Result<()> {
        self.account_buffer.push(row);
        
        if self.account_buffer.len() >= self.batch_size {
            self.flush_accounts().await?;
        }
        
        Ok(())
    }
    
    /// 插入程序日志
    pub async fn insert_program_log(&mut self, row: ProgramLogRow) -> anyhow::Result<()> {
        self.log_buffer.push(row);
        
        if self.log_buffer.len() >= self.batch_size {
            self.flush_program_logs().await?;
        }
        
        Ok(())
    }
    
    /// 插入链事件
    pub async fn insert_chain_event(&mut self, row: ChainEventRow) -> anyhow::Result<()> {
        self.event_buffer.push(row);
        
        if self.event_buffer.len() >= self.batch_size {
            self.flush_chain_events().await?;
        }
        
        Ok(())
    }
    
    /// 刷新区块数据
    pub async fn flush_blocks(&mut self) -> anyhow::Result<()> {
        if self.block_buffer.is_empty() {
            return Ok(());
        }
        
        let count = self.block_buffer.len();
        debug!("Flushing {} blocks to ClickHouse", count);
        
        self.retry_insert("chain_blocks", &self.block_buffer).await?;
        
        self.block_buffer.clear();
        info!("Successfully flushed {} blocks", count);
        
        Ok(())
    }
    
    /// 刷新交易数据
    pub async fn flush_transactions(&mut self) -> anyhow::Result<()> {
        if self.tx_buffer.is_empty() {
            return Ok(());
        }
        
        let count = self.tx_buffer.len();
        debug!("Flushing {} transactions to ClickHouse", count);
        
        self.retry_insert("chain_txs", &self.tx_buffer).await?;
        
        self.tx_buffer.clear();
        info!("Successfully flushed {} transactions", count);
        
        Ok(())
    }
    
    /// 刷新账户数据
    pub async fn flush_accounts(&mut self) -> anyhow::Result<()> {
        if self.account_buffer.is_empty() {
            return Ok(());
        }
        
        let count = self.account_buffer.len();
        debug!("Flushing {} accounts to ClickHouse", count);
        
        self.retry_insert("chain_accounts", &self.account_buffer).await?;
        
        self.account_buffer.clear();
        info!("Successfully flushed {} accounts", count);
        
        Ok(())
    }
    
    /// 刷新程序日志
    pub async fn flush_program_logs(&mut self) -> anyhow::Result<()> {
        if self.log_buffer.is_empty() {
            return Ok(());
        }
        
        let count = self.log_buffer.len();
        debug!("Flushing {} program logs to ClickHouse", count);
        
        self.retry_insert("chain_program_logs", &self.log_buffer).await?;
        
        self.log_buffer.clear();
        info!("Successfully flushed {} program logs", count);
        
        Ok(())
    }
    
    /// 刷新链事件
    pub async fn flush_chain_events(&mut self) -> anyhow::Result<()> {
        if self.event_buffer.is_empty() {
            return Ok(());
        }
        
        let count = self.event_buffer.len();
        debug!("Flushing {} chain events to ClickHouse", count);
        
        self.retry_insert("chain_events", &self.event_buffer).await?;
        
        self.event_buffer.clear();
        info!("Successfully flushed {} chain events", count);
        
        Ok(())
    }
    
    /// 刷新所有缓冲区
    pub async fn flush_all(&mut self) -> anyhow::Result<()> {
        self.flush_blocks().await?;
        self.flush_transactions().await?;
        self.flush_accounts().await?;
        self.flush_program_logs().await?;
        self.flush_chain_events().await?;
        Ok(())
    }
    
    /// 带重试的插入
    async fn retry_insert<T>(&self, table: &str, rows: &[T]) -> anyhow::Result<()>
    where
        T: clickhouse::Row + serde::Serialize + Clone,
    {
        let mut retries = 0;
        
        loop {
            match self.do_insert(table, rows).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    retries += 1;
                    if retries > self.max_retries {
                        error!("Failed to insert into {} after {} retries: {}", table, retries, e);
                        return Err(e);
                    }
                    
                    warn!("Insert into {} failed (attempt {}/{}): {}", table, retries, self.max_retries, e);
                    time::sleep(Duration::from_secs(retries as u64)).await;
                }
            }
        }
    }
    
    /// 执行插入
    async fn do_insert<T>(&self, table: &str, rows: &[T]) -> anyhow::Result<()>
    where
        T: clickhouse::Row + serde::Serialize,
    {
        let mut insert = self.client.insert(table)?;
        
        for row in rows {
            insert.write(row).await?;
        }
        
        insert.end().await?;
        
        Ok(())
    }
    
    /// 启动定时刷新任务
    pub fn spawn_flush_task(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = time::interval(self.flush_interval);
            
            loop {
                interval.tick().await;
                
                if let Err(e) = self.flush_all().await {
                    error!("Error flushing buffers: {}", e);
                }
            }
        })
    }
    
    /// 获取缓冲区大小
    pub fn buffer_sizes(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.block_buffer.len(),
            self.tx_buffer.len(),
            self.account_buffer.len(),
            self.log_buffer.len(),
            self.event_buffer.len(),
        )
    }
}
