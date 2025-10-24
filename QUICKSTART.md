# Kafka to ClickHouse Sync - 快速启动指南

## 前置条件

- Rust 1.75+ (用于编译)
- ClickHouse (运行中)
- Kafka 或 Redpanda (运行中)
- Solana Geyser Plugin (发送消息到 Kafka)

## 第一步：初始化 ClickHouse 数据库

```bash
# 连接到 ClickHouse
clickhouse-client

# 创建数据库
CREATE DATABASE IF NOT EXISTS solana;

# 执行初始化脚本
USE solana;
source /home/ubuntu/ytest/clickhouse-init.sql;

# 验证表创建成功
SHOW TABLES;
```

预期输出：
```
┌─name────────────────┐
│ chain_accounts      │
│ chain_blocks        │
│ chain_events        │
│ chain_program_logs  │
│ chain_txs           │
└─────────────────────┘
```

## 第二步：配置服务

创建配置文件 `config/config.toml`：

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
enable_deduplication = false
dedup_cache_size = 100000

[metrics]
enabled = false
port = 9090
```

## 第三步：编译服务

```bash
cd /home/ubuntu/ytest/kafka-clickhouse-sync

# Debug 版本（快速编译，用于开发）
cargo build

# Release 版本（优化性能，用于生产）
cargo build --release
```

## 第四步：运行服务

### 方式 1: 直接运行二进制

```bash
# Debug 版本
./target/debug/kafka-clickhouse-sync

# Release 版本
./target/release/kafka-clickhouse-sync
```

### 方式 2: 使用 cargo run

```bash
# Debug 模式
RUST_LOG=info cargo run

# 带详细日志
RUST_LOG=debug cargo run
```

### 方式 3: 使用 Docker Compose

```bash
# 启动所有服务（ClickHouse + Kafka + Sync Service）
docker-compose up -d

# 查看日志
docker-compose logs -f sync-service

# 停止服务
docker-compose down
```

## 第五步：验证数据同步

### 检查 Kafka Topics

```bash
# 如果使用 Kafka
kafka-topics --bootstrap-server localhost:9092 --list

# 如果使用 Redpanda
rpk topic list
```

### 检查 ClickHouse 数据

```bash
# 连接到 ClickHouse
clickhouse-client

# 查看各表数据量
USE solana;

SELECT 'blocks' as table, count(*) as count FROM chain_blocks
UNION ALL
SELECT 'transactions', count(*) FROM chain_txs
UNION ALL
SELECT 'accounts', count(*) FROM chain_accounts
UNION ALL
SELECT 'logs', count(*) FROM chain_program_logs
UNION ALL
SELECT 'events', count(*) FROM chain_events;
```

### 查询最新数据

```bash
# 最新区块
SELECT * FROM chain_blocks ORDER BY slot DESC LIMIT 10;

# 最新交易
SELECT slot, tx_signature, is_successful, fee 
FROM chain_txs 
ORDER BY slot DESC, tx_index DESC 
LIMIT 10;

# 最新账户更新
SELECT slot, pubkey, lamports, owner_program 
FROM chain_accounts 
ORDER BY slot DESC 
LIMIT 10;

# 最新程序日志
SELECT slot, program_id, message 
FROM chain_program_logs 
ORDER BY slot DESC, log_index DESC 
LIMIT 10;

# 最新事件
SELECT slot, program, event_type, payload_json 
FROM chain_events 
ORDER BY slot DESC 
LIMIT 10;
```

## 监控和调试

### 查看服务日志

服务会每分钟输出统计信息：

```
[2024-10-24T08:00:00Z INFO] Message processing stats - Blocks: 1234, Transactions: 56789, Accounts: 12345, Logs: 98765, Events: 4567, Errors: 0
```

### 调试模式

```bash
# 开启 DEBUG 日志查看详细的消息处理
RUST_LOG=debug ./target/debug/kafka-clickhouse-sync
```

Debug 日志会显示：
- 每个消息的 topic 和大小
- Protobuf 解码的详细信息
- ClickHouse 批量插入操作

### 性能监控

```bash
# 查看 ClickHouse 查询统计
SELECT 
    query_duration_ms,
    read_rows,
    result_rows,
    query
FROM system.query_log
WHERE type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 10;
```

## 常见问题

### Q1: 服务启动失败，提示 "Failed to connect to Kafka"

**解决方案：**
1. 检查 Kafka/Redpanda 是否运行：`telnet localhost 9092`
2. 验证配置文件中的 `bootstrap_servers` 地址是否正确
3. 检查防火墙设置

### Q2: 消息消费正常但 ClickHouse 没有数据

**解决方案：**
1. 检查数据库和表是否存在：`SHOW TABLES`
2. 检查 ClickHouse 连接：`curl http://localhost:8123/ping`
3. 查看服务日志中的错误信息
4. 验证批次大小和刷新间隔配置

### Q3: 数据延迟很高

**解决方案：**
1. 减小 `flush_interval_secs`（如改为 2 秒）
2. 减小 `batch_size`（如改为 500）
3. 增加 `worker_threads`
4. 检查 ClickHouse 性能

### Q4: 错误计数持续增加

**解决方案：**
1. 查看详细错误日志：`RUST_LOG=error,warn cargo run`
2. 检查 Protobuf 消息格式是否匹配
3. 验证 ClickHouse 表结构是否正确
4. 检查数据类型转换问题

## 性能优化建议

### 高吞吐量场景

```toml
[clickhouse]
batch_size = 5000          # 增大批次
flush_interval_secs = 10   # 延长刷新间隔

[processing]
worker_threads = 8         # 增加线程数
```

### 低延迟场景

```toml
[clickhouse]
batch_size = 100           # 减小批次
flush_interval_secs = 1    # 缩短刷新间隔
```

### 高可靠性场景

```toml
[clickhouse]
max_retries = 10           # 增加重试次数

[kafka]
# 禁用自动提交，手动管理 offset
enable_auto_commit = false
```

## 生产部署检查清单

- [ ] ClickHouse 数据库已初始化
- [ ] 配置文件中的密码已设置
- [ ] 已配置日志轮转
- [ ] 已设置系统资源限制（内存、CPU）
- [ ] 已配置监控和告警
- [ ] 已测试故障恢复流程
- [ ] 已备份配置文件
- [ ] 已设置自动重启（systemd/supervisor）

## 下一步

- [ ] 实现 Prometheus 监控指标
- [ ] 添加健康检查 HTTP 端点
- [ ] 实现去重机制
- [ ] 添加数据验证和完整性检查
- [ ] 优化批量插入性能
- [ ] 添加更多错误处理和重试策略

## 支持

如有问题，请查看：
- 完整 README: `/home/ubuntu/ytest/kafka-clickhouse-sync/README.md`
- 设计文档: `/home/ubuntu/ytest/KAFKA_CLICKHOUSE_DESIGN.md`
- 源代码: `/home/ubuntu/ytest/kafka-clickhouse-sync/src/`
