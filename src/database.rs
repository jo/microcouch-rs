use async_trait::async_trait;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_json::value::Value;
use std::collections::HashMap;
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
        match self.last_seq.split_once("-") {
            Some((nr, _)) => nr.to_string(),
            None => self.last_seq.to_owned()
        }
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
    pub doc: Option<Doc>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Doc {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _rev: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _deleted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _attachments: Option<HashMap<String, Attachment>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _conflicts: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub _deleted_conflicts: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _local_seq: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _revs_info: Option<Vec<RevInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _revisions: Option<RevisionsTree>,

    #[serde(flatten)]
    pub body: HashMap<String, Value>,
}

impl Doc {
    pub fn new(_id: Option<String>, body: HashMap<String, Value>) -> Self {
        Self {
            _id,

            _rev: None,
            _deleted: None,
            _attachments: None,
            _conflicts: None,

            _deleted_conflicts: None,
            _local_seq: None,
            _revs_info: None,
            _revisions: None,

            body,
        }
    }

    pub fn new_with_rev(_id: Option<String>, _rev: Option<String>, body: HashMap<String, Value>) -> Self {
        Self {
            _id,

            _rev,
            _deleted: None,
            _attachments: None,
            _conflicts: None,

            _deleted_conflicts: None,
            _local_seq: None,
            _revs_info: None,
            _revisions: None,

            body,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Attachment {
    pub content_type: String,
    pub digest: Option<String>,
    pub length: Option<u32>,
    pub revpos: Option<u32>,
    pub stub: Option<bool>,
    pub data: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RevisionsTree {
    pub ids: Vec<String>,
    pub start: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RevInfo {
    pub rev: String,
    pub status: String,
}

#[derive(Deserialize, Debug)]
pub struct ServerInfo {
    pub uuid: String,
}

#[derive(Deserialize, Debug)]
pub struct DatabaseInfo {
    pub update_seq: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicationLog {
    pub _id: String,
    pub source_last_seq: String,
    pub session_id: String,
}

#[async_trait]
pub trait Database {
    async fn get_server_info(&self) -> ServerInfo;

    async fn save_doc(&self, doc: Doc) -> ();
    async fn get_doc(&self, id: &str) -> Option<Doc>;

    // TODO: can't this be a reference?
    async fn get_replication_log(&self, replication_id: &str) -> Option<ReplicationLog>;
    async fn save_replication_log(&self, replication_log: ReplicationLog) -> ();

    async fn get_changes(&self, since: Option<String>, limit: usize) -> ReplicationBatch;
    async fn get_diff(&self, mut batch: ReplicationBatch) -> ReplicationBatch;
    async fn get_revs(&self, mut batch: ReplicationBatch) -> ReplicationBatch;
    async fn save_revs(&self, mut batch: ReplicationBatch) -> ReplicationBatch;
}
