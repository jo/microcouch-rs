use async_trait::async_trait;
use clap::lazy_static::lazy_static;
use serde_json::value::Value;
use tokio::time::Instant;

lazy_static! {
    static ref START_TIME: Instant = Instant::now();
}

#[derive(Debug)]
pub struct ReplicationBatch {
    pub last_seq: String,
    pub changes: Vec<Change>,
}

// just for debugging
impl ReplicationBatch {
    pub fn nr(&self) -> String {
        let (nr, _) = self.last_seq.split_once("-").unwrap();
        nr.to_string()
    }
}

#[derive(Debug)]
pub struct Change {
    pub id: String,
    pub revs: Vec<Rev>,
}

#[derive(Debug)]
pub struct Rev {
    pub rev: String,
    pub doc: Option<Value>,
}

#[derive(serde::Deserialize, Debug)]
pub struct ServerInfo {
    pub uuid: String,
}

#[derive(serde::Deserialize, Debug)]
pub struct DatabaseInfo {
    pub update_seq: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ReplicationLog {
    pub _id: String,
    pub source_last_seq: String,
    pub session_id: String,
}

#[async_trait]
pub trait Database {
    async fn get_server_info(&self) -> ServerInfo;

    async fn get_replication_log(&self, replication_id: &str) -> Option<ReplicationLog>;
    async fn save_replication_log(&self, replication_log: ReplicationLog) -> ();

    async fn get_changes(&self, since: Option<String>, limit: usize) -> ReplicationBatch;
    async fn get_diff(&self, mut batch: ReplicationBatch) -> ReplicationBatch;
    async fn get_revs(&self, mut batch: ReplicationBatch) -> ReplicationBatch;
    async fn save_revs(&self, mut batch: ReplicationBatch) -> ReplicationBatch;
}
