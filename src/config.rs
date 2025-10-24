// src/config.rs - 配置管理

use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub kafka: KafkaConfig,
    pub clickhouse: ClickHouseConfig,
    pub processing: ProcessingConfig,
    pub metrics: MetricsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub topics: Vec<String>,
    
    // SASL 认证配置
    #[serde(default)]
    pub sasl_mechanism: String,
    
    #[serde(default)]
    pub sasl_username: String,
    
    #[serde(default)]
    pub sasl_password: String,
    
    #[serde(default)]
    pub security_protocol: String,
    
    #[serde(default = "default_false")]
    pub enable_auto_commit: bool,
    
    #[serde(default = "default_session_timeout")]
    pub session_timeout_ms: u32,
    
    #[serde(default = "default_max_poll_records")]
    pub max_poll_records: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
    
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    
    #[serde(default = "default_flush_interval")]
    pub flush_interval_secs: u64,
    
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    #[serde(default = "default_num_workers")]
    pub num_workers: usize,
    
    #[serde(default = "default_true")]
    pub enable_deduplication: bool,
    
    #[serde(default = "default_dedupe_window")]
    pub dedupe_window_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    #[serde(default = "default_metrics_port")]
    pub port: u16,
}

// 默认值函数
fn default_false() -> bool {
    false
}

fn default_true() -> bool {
    true
}

fn default_session_timeout() -> u32 {
    30000
}

fn default_max_poll_records() -> u32 {
    500
}

fn default_batch_size() -> usize {
    1000
}

fn default_flush_interval() -> u64 {
    5
}

fn default_max_retries() -> u32 {
    3
}

fn default_num_workers() -> usize {
    4
}

fn default_dedupe_window() -> u64 {
    60
}

fn default_metrics_port() -> u16 {
    9090
}

impl Config {
    /// 从 TOML 文件加载配置
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
    
    /// 从环境变量覆盖配置
    pub fn from_env(&mut self) {
        if let Ok(servers) = std::env::var("KAFKA_BOOTSTRAP_SERVERS") {
            self.kafka.bootstrap_servers = servers;
        }
        if let Ok(url) = std::env::var("CLICKHOUSE_URL") {
            self.clickhouse.url = url;
        }
        if let Ok(user) = std::env::var("CLICKHOUSE_USER") {
            self.clickhouse.user = user;
        }
        if let Ok(password) = std::env::var("CLICKHOUSE_PASSWORD") {
            self.clickhouse.password = password;
        }
    }
    
    /// 验证配置
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.kafka.bootstrap_servers.is_empty() {
            anyhow::bail!("Kafka bootstrap_servers cannot be empty");
        }
        if self.kafka.topics.is_empty() {
            anyhow::bail!("Kafka topics cannot be empty");
        }
        if self.clickhouse.url.is_empty() {
            anyhow::bail!("ClickHouse URL cannot be empty");
        }
        if self.clickhouse.database.is_empty() {
            anyhow::bail!("ClickHouse database cannot be empty");
        }
        if self.clickhouse.batch_size == 0 {
            anyhow::bail!("ClickHouse batch_size must be > 0");
        }
        Ok(())
    }
}
