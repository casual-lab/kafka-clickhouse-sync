// src/kafka/mod.rs - Kafka 模块（待实现）

// src/kafka/mod.rs - Kafka 模块

pub mod consumer;
pub mod message_handler;

pub use consumer::{KafkaConsumer, KafkaConsumerBuilder};
pub use message_handler::{MessageHandler, MessageStats};
