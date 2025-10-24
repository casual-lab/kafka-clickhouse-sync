// src/lib.rs - 库入口

// 导入生成的 Protobuf 代码
pub mod proto {
    pub mod blockdaemon {
        pub mod solana {
            pub mod accountsdb_plugin_kafka {
                pub mod types {
                    include!(concat!(env!("OUT_DIR"), "/blockdaemon.solana.accountsdb_plugin_kafka.types.rs"));
                }
            }
        }
    }
}

// 核心模块
pub mod config;
pub mod models;
pub mod clickhouse;
pub mod kafka;
pub mod metrics;
pub mod utils;

// 重导出常用类型
pub use config::Config;
pub use clickhouse::{ClickHouseClient, ClickHouseWriter};
pub use models::{BlockRow, TransactionRow, AccountRow, ProgramLogRow, ChainEventRow};
pub use kafka::{KafkaConsumer, KafkaConsumerBuilder, MessageHandler, MessageStats};
