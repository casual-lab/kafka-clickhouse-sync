// src/kafka/consumer.rs - Kafka 消费者

use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance};
use rdkafka::ClientContext;
use tracing::{error, info};

/// 自定义 Kafka Context 用于日志记录
pub struct LoggingConsumerContext;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                info!("Kafka rebalance: Assigned partitions: {:?}", tpl);
            }
            Rebalance::Revoke(tpl) => {
                info!("Kafka rebalance: Partitions revoked: {:?}", tpl);
            }
            Rebalance::Error(err) => {
                error!("Kafka rebalance error: {:?}", err);
            }
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                info!("Kafka rebalance completed: {:?}", tpl);
            }
            Rebalance::Revoke(tpl) => {
                info!("Kafka rebalance revoke completed: {:?}", tpl);
            }
            Rebalance::Error(err) => {
                error!("Kafka rebalance post error: {:?}", err);
            }
        }
    }
}

pub type KafkaConsumer = StreamConsumer<LoggingConsumerContext>;

/// Kafka 消费者构建器
pub struct KafkaConsumerBuilder {
    bootstrap_servers: String,
    group_id: String,
    topics: Vec<String>,
    enable_auto_commit: bool,
    auto_offset_reset: String,
    max_poll_interval_ms: String,
    
    // SASL 认证
    sasl_mechanism: Option<String>,
    sasl_username: Option<String>,
    sasl_password: Option<String>,
    security_protocol: Option<String>,
}

impl KafkaConsumerBuilder {
    pub fn new(bootstrap_servers: String, group_id: String) -> Self {
        Self {
            bootstrap_servers,
            group_id,
            topics: Vec::new(),
            enable_auto_commit: true,
            auto_offset_reset: "earliest".to_string(),
            max_poll_interval_ms: "300000".to_string(), // 5 分钟
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            security_protocol: None,
        }
    }

    pub fn topics(mut self, topics: Vec<String>) -> Self {
        self.topics = topics;
        self
    }

    pub fn enable_auto_commit(mut self, enable: bool) -> Self {
        self.enable_auto_commit = enable;
        self
    }

    pub fn auto_offset_reset(mut self, reset: String) -> Self {
        self.auto_offset_reset = reset;
        self
    }
    
    pub fn sasl_auth(
        mut self,
        mechanism: String,
        username: String,
        password: String,
    ) -> Self {
        self.sasl_mechanism = Some(mechanism);
        self.sasl_username = Some(username);
        self.sasl_password = Some(password);
        self
    }
    
    pub fn security_protocol(mut self, protocol: String) -> Self {
        self.security_protocol = Some(protocol);
        self
    }

    pub fn build(self) -> Result<KafkaConsumer> {
        if self.topics.is_empty() {
            return Err(anyhow::anyhow!("At least one topic must be specified"));
        }

        info!("Creating Kafka consumer for topics: {:?}", self.topics);
        info!("Bootstrap servers: {}", self.bootstrap_servers);
        info!("Consumer group: {}", self.group_id);

        let context = LoggingConsumerContext;
        
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", &self.group_id)
            .set("enable.auto.commit", self.enable_auto_commit.to_string())
            .set("auto.offset.reset", &self.auto_offset_reset)
            .set("max.poll.interval.ms", &self.max_poll_interval_ms)
            .set("session.timeout.ms", "45000")
            .set("heartbeat.interval.ms", "3000")
            .set("enable.partition.eof", "false")
            .set("api.version.request", "true");
        
        // 设置安全协议
        if let Some(protocol) = &self.security_protocol {
            if !protocol.is_empty() {
                info!("Using security protocol: {}", protocol);
                config.set("security.protocol", protocol);
            }
        }
        
        // 设置 SASL 认证
        if let Some(mechanism) = &self.sasl_mechanism {
            if !mechanism.is_empty() {
                info!("Using SASL mechanism: {}", mechanism);
                config.set("sasl.mechanism", mechanism);
                
                if let Some(username) = &self.sasl_username {
                    config.set("sasl.username", username);
                }
                
                if let Some(password) = &self.sasl_password {
                    config.set("sasl.password", password);
                }
            }
        }
        
        let consumer: StreamConsumer<LoggingConsumerContext> = config.create_with_context(context)?;

        // 订阅 topics
        let topic_refs: Vec<&str> = self.topics.iter().map(|s| s.as_str()).collect();
        consumer.subscribe(&topic_refs)?;

        info!("Kafka consumer created successfully");
        Ok(consumer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_builder() {
        let builder = KafkaConsumerBuilder::new(
            "localhost:9092".to_string(),
            "test-group".to_string(),
        )
        .topics(vec!["test-topic".to_string()])
        .enable_auto_commit(false);

        // 注意：实际创建消费者需要 Kafka 运行，这里只测试构建器
        assert_eq!(builder.bootstrap_servers, "localhost:9092");
        assert_eq!(builder.group_id, "test-group");
        assert_eq!(builder.topics.len(), 1);
        assert!(!builder.enable_auto_commit);
    }
}
