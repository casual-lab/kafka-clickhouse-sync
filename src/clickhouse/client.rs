// src/clickhouse/client.rs - ClickHouse 客户端

use crate::config::ClickHouseConfig;
use clickhouse::Client;

pub struct ClickHouseClient {
    client: Client,
}

impl ClickHouseClient {
    pub fn new(config: &ClickHouseConfig) -> anyhow::Result<Self> {
        let client = Client::default()
            .with_url(&config.url)
            .with_database(&config.database)
            .with_user(&config.user)
            .with_password(&config.password);
        
        Ok(Self { client })
    }
    
    pub fn client(&self) -> &Client {
        &self.client
    }
    
    /// 测试连接
    pub async fn ping(&self) -> anyhow::Result<()> {
        self.client.query("SELECT 1").fetch_one::<u8>().await?;
        Ok(())
    }
    
    /// 检查表是否存在
    pub async fn table_exists(&self, table_name: &str) -> anyhow::Result<bool> {
        let query = format!(
            "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '{}'",
            table_name
        );
        let count: u64 = self.client.query(&query).fetch_one().await?;
        Ok(count > 0)
    }
}
