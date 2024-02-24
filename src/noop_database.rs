use async_trait::async_trait;
use uuid::Uuid;

use crate::database::{Change, Database, ReplicationBatch, ReplicationLog, Rev, Doc, RevisionsTree, ServerInfo};

pub struct NoopDatabase {
    id: String,
}

impl NoopDatabase {
    pub fn new(id: &str) -> Self {
        Self { id: id.to_owned() }
    }
}

#[async_trait]
impl Database for NoopDatabase {
    async fn get_server_info(&self) -> ServerInfo {
        let uuid = Uuid::new_v4();

        ServerInfo {
            uuid: uuid.to_string()
        }
    }

    async fn get_replication_log(&self, replication_id: &str) -> Option<ReplicationLog> {
        None
    }

    async fn save_replication_log(&self, replication_log: ReplicationLog) -> () {}

    async fn get_changes(&self, since: Option<String>, batch_size: usize) -> ReplicationBatch {
        ReplicationBatch {
            last_seq: "0".to_owned(),
            changes: vec![]
        }
    }

    async fn get_diff(&self, mut batch: ReplicationBatch) -> ReplicationBatch {
        batch
    }

    async fn get_revs(&self, mut batch: ReplicationBatch) -> ReplicationBatch {
        batch
    }

    async fn save_revs(&self, mut batch: ReplicationBatch) -> ReplicationBatch {
        batch
    }
}
