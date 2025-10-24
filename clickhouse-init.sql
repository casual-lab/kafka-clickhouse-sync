-- ClickHouse 数据库初始化脚本
-- 用于 Solana Geyser Plugin Kafka 数据同步

-- 1. 创建数据库
DROP DATABASE IF EXISTS solana;
CREATE DATABASE IF NOT EXISTS solana;

USE solana;

-- ========================================
-- 2. 创建表结构
-- ========================================

-- 2.1 区块表 (chain_blocks)
DROP TABLE IF EXISTS chain_blocks;
CREATE TABLE chain_blocks (
    -- 主键
    slot UInt64,
    
    -- 区块信息
    blockhash String,
    parent_slot UInt64,
    parent_blockhash String,
    leader String,                      -- 区块生产者公钥
    num_txs UInt32,                     -- 交易数量
    
    -- 区块状态
    status String,                      -- 区块状态 (finalized, confirmed, etc.)
    
    -- 时间戳
    timestamp UInt64,                   -- 区块时间 (Unix timestamp)
    ingested_at UInt64,                 -- 数据入库时间 (Unix timestamp)
    
    -- 统计信息（可选）
    executed_transaction_count UInt32,
    total_compute_units UInt64,
    total_fees UInt64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(toDateTime(timestamp))
ORDER BY (slot, blockhash)
SETTINGS index_granularity = 8192;

-- 索引
CREATE INDEX IF NOT EXISTS idx_leader ON chain_blocks (leader) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_timestamp ON chain_blocks (timestamp) TYPE minmax GRANULARITY 1;

-- 2.2 交易表 (chain_txs)
DROP TABLE IF EXISTS chain_txs;
CREATE TABLE chain_txs (
    -- 主键
    slot UInt64,
    tx_signature String,
    tx_index UInt64,
    
    -- 签名者信息
    signer String,
    num_signers UInt32,
    
    -- 指令和账户统计
    num_instructions UInt32,
    num_accounts UInt32,
    
    -- 交易标志
    is_vote UInt8,
    is_successful UInt8,
    
    -- 错误信息
    error String,
    
    -- 资源消耗
    fee UInt64,
    compute_units_consumed UInt64,
    compute_units_price UInt64,
    total_cost UInt64,
    
    -- 时间戳
    timestamp UInt64,
    ingested_at UInt64,
    
    -- 指令详情 (JSON 格式)
    instructions_json String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(timestamp))
ORDER BY (slot, tx_index, tx_signature)
SETTINGS index_granularity = 8192;

-- 索引
CREATE INDEX IF NOT EXISTS idx_signature ON chain_txs (tx_signature) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_signer ON chain_txs (signer) TYPE bloom_filter GRANULARITY 1;

-- 2.3 账户表 (chain_accounts)
DROP TABLE IF EXISTS chain_accounts;
CREATE TABLE chain_accounts (
    -- 主键字段
    slot UInt64,
    pubkey String,
    owner_program String,
    
    -- 账户数据
    lamports UInt64,
    executable UInt8,
    rent_epoch UInt64,
    data_len UInt32,
    data_hash String,                   -- SHA256 hash of account data
    write_version UInt64,
    
    -- 元数据
    txn_signature String,
    
    -- 时间戳
    timestamp UInt64,
    ingested_at UInt64
) ENGINE = ReplacingMergeTree(write_version)
PARTITION BY toYYYYMM(toDateTime(timestamp))
ORDER BY (pubkey, slot, write_version)
SETTINGS index_granularity = 8192;

-- 索引
CREATE INDEX IF NOT EXISTS idx_owner_program ON chain_accounts (owner_program) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_slot ON chain_accounts (slot) TYPE minmax GRANULARITY 1;

-- 2.4 程序日志表 (chain_program_logs)
DROP TABLE IF EXISTS chain_program_logs;
CREATE TABLE chain_program_logs (
    -- 主键
    slot UInt64,
    tx_signature String,
    tx_index UInt32,
    log_index UInt32,
    
    -- 日志内容
    program_id String,
    message String,
    log_type Int32,                     -- 日志类型代码
    depth UInt32,
    
    -- 时间戳
    timestamp UInt64,
    ingested_at UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(timestamp))
ORDER BY (slot, tx_index, log_index)
SETTINGS index_granularity = 8192;

-- 索引
CREATE INDEX IF NOT EXISTS idx_program_id ON chain_program_logs (program_id) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_tx_signature ON chain_program_logs (tx_signature) TYPE bloom_filter GRANULARITY 1;

-- 2.5 链事件表 (chain_events)
DROP TABLE IF EXISTS chain_events;
CREATE TABLE chain_events (
    -- 主键
    slot UInt64,
    tx_signature String,
    tx_index UInt32,
    log_index UInt32,
    
    -- 事件信息
    program String,
    event_type String,
    discriminator String,
    
    -- 事件数据
    payload_json String,                -- JSON 格式
    raw_data String,                    -- Base64 编码的原始数据
    
    -- 时间戳
    timestamp UInt64,
    ingested_at UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(timestamp))
ORDER BY (slot, tx_index, log_index, program)
SETTINGS index_granularity = 8192;

-- 索引
CREATE INDEX IF NOT EXISTS idx_program ON chain_events (program) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_event_type ON chain_events (event_type) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_discriminator ON chain_events (discriminator) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_tx_signature ON chain_events (tx_signature) TYPE bloom_filter GRANULARITY 1;

-- ========================================
-- 3. 创建物化视图（可选，用于加速常见查询）
-- ========================================

-- 3.1 每日交易统计
DROP TABLE IF EXISTS chain_daily_tx_stats;
CREATE MATERIALIZED VIEW IF NOT EXISTS chain_daily_tx_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, is_successful)
POPULATE
AS SELECT
    toDate(toDateTime(timestamp)) as date,
    is_successful,
    count() as tx_count,
    sum(fee) as total_fees,
    sum(compute_units_consumed) as total_compute_units
FROM chain_txs
GROUP BY date, is_successful;

-- 3.2 每小时区块统计
DROP TABLE IF EXISTS chain_hourly_block_stats;
CREATE MATERIALIZED VIEW IF NOT EXISTS chain_hourly_block_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, leader)
POPULATE
AS SELECT
    toStartOfHour(toDateTime(timestamp)) as hour,
    leader,
    count() as block_count,
    sum(num_txs) as total_txs,
    sum(total_fees) as total_fees
FROM chain_blocks
GROUP BY hour, leader;

-- 3.3 程序调用频率统计
DROP TABLE IF EXISTS chain_program_call_stats;
CREATE MATERIALIZED VIEW IF NOT EXISTS chain_program_call_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, program_id)
POPULATE
AS SELECT
    toDate(toDateTime(timestamp)) as date,
    program_id,
    count() as call_count,
    countIf(log_type = 1) as invoke_count,
    countIf(log_type = 5) as failed_count,
    countIf(log_type = 4) as success_count
FROM chain_program_logs
GROUP BY date, program_id;

-- ========================================
-- 4. TTL 策略（可选，自动清理历史数据）
-- ========================================

-- 保留 6 个月的数据
-- ALTER TABLE chain_blocks MODIFY TTL timestamp + INTERVAL 6 MONTH;
-- ALTER TABLE chain_txs MODIFY TTL timestamp + INTERVAL 6 MONTH;
-- ALTER TABLE chain_accounts MODIFY TTL timestamp + INTERVAL 6 MONTH;
-- ALTER TABLE chain_program_logs MODIFY TTL timestamp + INTERVAL 3 MONTH;
-- ALTER TABLE chain_events MODIFY TTL timestamp + INTERVAL 3 MONTH;

-- ========================================
-- 5. 验证表创建
-- ========================================

SHOW TABLES;

SELECT 
    table,
    engine,
    partition_key,
    sorting_key,
    total_rows
FROM system.tables
WHERE database = 'solana'
ORDER BY table;
