# Kafka to ClickHouse Sync Service

将 Solana Geyser Plugin 发出的 Kafka 消息实时同步到 ClickHouse 数据库的服务。

## 功能特性

- ✅ 从 5 个 Kafka Topics 消费消息：
  - `solana.chain_blocks` - 区块元数据
  - `solana.chain_txs` - 交易数据
  - `solana.chain_accounts` - 账户更新
  - `solana.chain_program_logs` - 程序日志
  - `solana.chain_events` - 链上事件（Anchor Events）

- ✅ 批量写入 ClickHouse（可配置批次大小）
- ✅ 自动定期刷新缓冲区（5秒）
- ✅ 重试机制（失败自动重试 3 次）
- ✅ Protobuf 消息解析
- ✅ 统计日志（每分钟输出处理统计）
- ✅ 优雅关闭（Ctrl+C 时刷新所有缓冲数据）

## 架构

```
Kafka Topics
    │
    ├─ chain_blocks ──────┐
    ├─ chain_txs ─────────┤
    ├─ chain_accounts ────┼──► Kafka Consumer
    ├─ chain_program_logs ┤      │
    └─ chain_events ──────┘      │
                                 ▼
                          Message Handler
                          (Protobuf Decode)
                                 │
                                 ▼
                          Data Models
                          (5 Row Types)
                                 │
                                 ▼
                          ClickHouse Writer
                          (Batch Buffer)
                                 │
                                 ▼
                          ClickHouse Database
                          (5 Tables)
```

## 数据流

1. **Kafka Consumer** 订阅 5 个 topics，接收 Protobuf 编码的消息
2. **Message Handler** 根据 topic 路由消息，解码 Protobuf
3. **Data Models** 将 Protobuf 消息转换为 ClickHouse Row 结构
4. **ClickHouse Writer** 批量缓冲数据（1000条/批次），定期刷新到数据库

## 配置文件

创建 `config/config.toml`：

```toml
[kafka]
bootstrap_servers = "localhost:9092"
group_id = "clickhouse-sync-group"
topics = [
    "solana.chain_blocks",
    "solana.chain_txs",
    "solana.chain_accounts",
    "solana.chain_program_logs",
    "solana.chain_events"
]

[clickhouse]
url = "http://localhost:8123"
database = "solana"
user = "default"
password = ""
batch_size = 1000
flush_interval_secs = 5
max_retries = 3

[processing]
worker_threads = 4
enable_deduplication = true
dedup_cache_size = 100000

[metrics]
enabled = false
port = 9090
```

## 环境变量覆盖

支持通过环境变量覆盖配置：

```bash
export KAFKA_BOOTSTRAP_SERVERS="kafka1:9092,kafka2:9092"
export CLICKHOUSE_URL="http://clickhouse:8123"
export CLICKHOUSE_USER="admin"
export CLICKHOUSE_PASSWORD="secret"
export BATCH_SIZE="2000"
```

## 编译

```bash
cd kafka-clickhouse-sync
cargo build --release
```

## 运行

```bash
# 使用默认配置文件
./target/release/kafka-clickhouse-sync

# 指定配置文件路径
CONFIG_PATH=/path/to/config.toml ./target/release/kafka-clickhouse-sync
```

## 日志级别

通过环境变量控制日志级别：

```bash
# INFO 级别（默认）
RUST_LOG=info ./target/release/kafka-clickhouse-sync

# DEBUG 级别（查看详细消息处理）
RUST_LOG=debug ./target/release/kafka-clickhouse-sync

# 只显示错误
RUST_LOG=error ./target/release/kafka-clickhouse-sync
```

## ClickHouse 数据库初始化

运行服务前，需要先初始化 ClickHouse 数据库：

```bash
# 连接到 ClickHouse
clickhouse-client

# 执行初始化脚本
source /path/to/clickhouse-init.sql
```

或使用命令行：

```bash
clickhouse-client < clickhouse-init.sql
```

## 数据表结构

### 1. chain_blocks - 区块数据
```sql
CREATE TABLE chain_blocks (
    slot UInt64,
    blockhash String,
    parent_slot UInt64,
    parent_blockhash String,
    leader String,
    num_txs UInt32,
    status String,
    timestamp DateTime,
    ingested_at DateTime,
    executed_transaction_count UInt32,
    total_compute_units UInt64,
    total_fees UInt64
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY slot;
```

### 2. chain_txs - 交易数据
```sql
CREATE TABLE chain_txs (
    slot UInt64,
    tx_signature String,
    tx_index UInt64,
    signer String,
    num_signers UInt32,
    num_instructions UInt32,
    num_accounts UInt32,
    is_vote Bool,
    is_successful Bool,
    error String,
    fee UInt64,
    compute_units_consumed UInt64,
    compute_units_price UInt64,
    total_cost UInt64,
    timestamp DateTime,
    ingested_at DateTime,
    instructions_json String
) ENGINE = MergeTree()
ORDER BY (slot, tx_index);
```

### 3. chain_accounts - 账户数据
```sql
CREATE TABLE chain_accounts (
    slot UInt64,
    pubkey String,
    owner_program String,
    lamports UInt64,
    executable Bool,
    rent_epoch UInt64,
    data_len UInt32,
    data_hash String,
    write_version UInt64,
    txn_signature String,
    timestamp DateTime,
    ingested_at DateTime,
    data_encoding String
) ENGINE = ReplacingMergeTree(write_version)
ORDER BY pubkey;
```

### 4. chain_program_logs - 程序日志
```sql
CREATE TABLE chain_program_logs (
    slot UInt64,
    tx_signature String,
    tx_index UInt32,
    log_index UInt32,
    program_id String,
    message String,
    log_type Int32,
    depth UInt32,
    timestamp DateTime,
    ingested_at DateTime
) ENGINE = MergeTree()
ORDER BY (slot, tx_index, log_index);
```

### 5. chain_events - 链上事件
```sql
CREATE TABLE chain_events (
    slot UInt64,
    tx_signature String,
    tx_index UInt32,
    log_index UInt32,
    program String,
    event_type String,
    discriminator String,
    payload_json String,
    raw_data String,
    timestamp DateTime,
    ingested_at DateTime
) ENGINE = MergeTree()
ORDER BY (slot, tx_index, log_index);
```

## 性能调优

### Kafka Consumer 参数

```toml
[kafka]
# 增加 poll 超时时间
max_poll_interval_ms = 300000  # 5分钟

# 调整会话超时
session_timeout_ms = 45000

# 心跳间隔
heartbeat_interval_ms = 3000
```

### ClickHouse 批量写入

```toml
[clickhouse]
# 增大批次大小提高吞吐量
batch_size = 5000

# 减少刷新间隔降低延迟
flush_interval_secs = 2

# 增加重试次数
max_retries = 5
```

### 并发处理

```toml
[processing]
# 增加工作线程数
worker_threads = 8
```

## 监控指标

服务会每分钟输出处理统计：

```
Message processing stats - Blocks: 1234, Transactions: 56789, Accounts: 12345, Logs: 98765, Events: 4567, Errors: 0
```

## 故障处理

### 消息处理失败
- 错误日志会记录失败的 topic 和原因
- 失败消息不会阻塞其他消息处理
- 检查 `Errors` 计数判断系统健康状态

### ClickHouse 写入失败
- 自动重试 3 次（可配置）
- 重试采用指数退避策略
- 持续失败会记录错误日志

### Kafka 连接中断
- 自动重连（rdkafka 内置机制）
- 1秒后重试接收消息

## 开发

### 项目结构

```
kafka-clickhouse-sync/
├── Cargo.toml              # Rust 依赖配置
├── build.rs                # Protobuf 编译脚本
├── proto/
│   └── event.proto         # Protobuf 定义
├── config/
│   └── config.toml         # 配置模板
└── src/
    ├── lib.rs              # 库入口
    ├── main.rs             # 主程序
    ├── config.rs           # 配置管理
    ├── models/             # 数据模型
    │   ├── mod.rs
    │   ├── block.rs
    │   ├── transaction.rs
    │   ├── account.rs
    │   ├── log.rs
    │   └── event.rs
    ├── clickhouse/         # ClickHouse 客户端
    │   ├── mod.rs
    │   ├── client.rs
    │   └── writer.rs
    ├── kafka/              # Kafka 消费者
    │   ├── mod.rs
    │   ├── consumer.rs
    │   └── message_handler.rs
    ├── metrics/            # 监控指标（TODO）
    │   └── mod.rs
    └── utils/              # 工具函数
        └── mod.rs
```

### 运行测试

```bash
cargo test
```

### 代码格式化

```bash
cargo fmt
```

### 静态检查

```bash
cargo clippy
```

## Docker 部署

（TODO：创建 Dockerfile 和 docker-compose.yml）

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！
