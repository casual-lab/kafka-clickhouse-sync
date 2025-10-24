// src/main.rs - 主程序入口

use kafka_clickhouse_sync::{
    ClickHouseClient, ClickHouseWriter, Config, KafkaConsumerBuilder, MessageHandler,
};
use rdkafka::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{error, info, warn};
use tracing_subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();
    
    info!("Starting Kafka to ClickHouse sync service...");
    
    // 加载配置
    let config_path = std::env::var("CONFIG_PATH")
        .unwrap_or_else(|_| "config/config.toml".to_string());
    
    let mut config = Config::from_file(&config_path)?;
    config.from_env();
    config.validate()?;
    
    info!("Configuration loaded successfully");
    info!("Kafka brokers: {}", config.kafka.bootstrap_servers);
    info!("ClickHouse URL: {}", config.clickhouse.url);
    info!("Topics: {:?}", config.kafka.topics);
    
    // 初始化 ClickHouse Writer
    info!("Initializing ClickHouse writer...");
    let writer = ClickHouseWriter::new(&config.clickhouse)?;
    info!("ClickHouse writer initialized with batch size: {}", config.clickhouse.batch_size);

    // 初始化 Kafka 消费者
    info!("Initializing Kafka consumer...");
    let consumer = KafkaConsumerBuilder::new(
        config.kafka.bootstrap_servers.clone(),
        config.kafka.group_id.clone(),
    )
    .topics(config.kafka.topics.clone())
    .enable_auto_commit(true)
    .build()?;

    info!("Kafka consumer initialized successfully");

    // 初始化消息处理器
    let handler = Arc::new(Mutex::new(MessageHandler::new()));
    let writer = Arc::new(Mutex::new(writer));

    // 启动统计日志任务
    let handler_clone = handler.clone();
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            let handler = handler_clone.lock().await;
            handler.log_stats();
        }
    });

    // 启动定期刷新任务
    let writer_flush = writer.clone();
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let mut writer = writer_flush.lock().await;
            if let Err(e) = writer.flush_all().await {
                error!("Error flushing buffers: {:?}", e);
            }
        }
    });

    // 启动消息处理循环
    info!("Starting message processing loop...");
    let writer_clone = writer.clone();
    let handler_clone2 = handler.clone();
    let shutdown = tokio::spawn(async move {
        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        let topic = msg.topic();
                        
                        // 处理消息
                        let mut handler = handler_clone2.lock().await;
                        let mut writer = writer_clone.lock().await;
                        
                        if let Err(e) = handler.handle_message(topic, payload, &mut *writer).await {
                            error!("Failed to process message from topic {}: {:?}", topic, e);
                        }
                    } else {
                        warn!("Received empty message");
                    }
                }
                Err(e) => {
                    error!("Kafka consumer error: {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });

    info!("Service started successfully!");
    
    // 等待关闭信号
    tokio::signal::ctrl_c().await?;
    info!("Received shutdown signal, exiting gracefully");

    // 取消消息处理任务
    shutdown.abort();

    // 最后刷新所有缓冲数据
    info!("Flushing remaining data to ClickHouse...");
    let mut writer = writer.lock().await;
    if let Err(e) = writer.flush_all().await {
        error!("Error flushing data during shutdown: {:?}", e);
    }

    info!("Shutdown complete");
    Ok(())
}
