use crate::database::Database;

use crate::database::{ReplicationBatch, ReplicationLog, ServerInfo};
use lazy_static::lazy_static;
use futures::future;
use futures::{join, stream, StreamExt};
use std::sync::{Arc, Mutex};
use tokio::time::Instant;
use uuid::Uuid;

lazy_static! {
    static ref START_TIME: Instant = Instant::now();
}

#[derive(Debug)]
pub struct ReplicationStats {}

struct ChangesOptions {
    since: Option<String>,
}

pub async fn replicate(
    source: &impl Database,
    target: &impl Database,
    concurrency: usize,
    batch_size: usize,
) -> ReplicationStats {
    let infos = get_infos(source, target).await;
    let source_server_info = infos.0;
    let target_server_info = infos.1;

    let replication_id = replication_id(source_server_info, target_server_info);
    let session_id = Uuid::new_v4();

    let logs = get_replication_logs(source, target, &replication_id).await;
    let source_replication_log = logs.0;
    let target_replication_log = logs.1;
    let since = find_common_ancestor(&source_replication_log, &target_replication_log);

    let options = ChangesOptions { since };
    let options = Arc::new(Mutex::new(options));

    let batches: Vec<ReplicationBatch> = stream::iter(0..)
        .then(|_i| async {
            let mut options = options.lock().unwrap();

            let changes = source.get_changes(options.since.clone(), batch_size).await;

            let seq = changes.last_seq.clone();
            options.since = Some(seq);

            changes
        })
        // TODO: abort if changes are less than requested
        .take_while(|x| future::ready(x.changes.len() > 0))
        .map(|x| replicate_batch(source, target, x))
        .buffered(concurrency)
        .then(|x| save_replication_logs(source, target, &replication_id, &session_id, x))
        .collect()
        .await;

    println!(
        "[{}] replication completed in {} batches",
        START_TIME.elapsed().as_millis(),
        batches.len()
    );

    ReplicationStats {}
}

async fn replicate_batch(
    source: &impl Database,
    target: &impl Database,
    batch: ReplicationBatch,
) -> ReplicationBatch {
    let no = batch.nr();
    println!(
        "[{}] # replicate_batch {}",
        START_TIME.elapsed().as_millis(),
        no
    );

    let diffs = target.get_diff(batch).await;
    let revs = source.get_revs(diffs).await;
    let save = target.save_revs(revs).await;

    println!(
        "[{}] # replicate_batch {} completed",
        START_TIME.elapsed().as_millis(),
        no
    );

    ReplicationBatch {
        last_seq: save.last_seq,
        changes: vec![],
    }
}

async fn get_infos(source: &impl Database, target: &impl Database) -> (ServerInfo, ServerInfo) {
    let source_server_info = source.get_server_info();
    let target_server_info = target.get_server_info();
    join!(source_server_info, target_server_info)
}

fn replication_id(source_server_info: ServerInfo, target_server_info: ServerInfo) -> String {
    let replication_id_data = vec![source_server_info.uuid, target_server_info.uuid];
    let replication_digest = md5::compute(replication_id_data.join(""));
    format!("{:x}", replication_digest)
}

async fn get_replication_logs(
    source: &impl Database,
    target: &impl Database,
    replication_id: &str,
) -> (Option<ReplicationLog>, Option<ReplicationLog>) {
    let source_replication_log = source.get_replication_log(replication_id);
    let target_replication_log = target.get_replication_log(replication_id);
    join!(source_replication_log, target_replication_log)
}

async fn save_replication_logs(
    source: &impl Database,
    target: &impl Database,
    replication_id: &str,
    session_id: &Uuid,
    batch: ReplicationBatch,
) -> ReplicationBatch {
    // TODO: can't we do this in parallel?
    let id = format!("_local/{}", replication_id);

    let source_replication_log = ReplicationLog {
        _id: id.to_string(),
        source_last_seq: batch.last_seq.to_string(),
        session_id: session_id.to_string(),
    };
    source.save_replication_log(source_replication_log).await;

    let target_replication_log = ReplicationLog {
        _id: id.to_string(),
        source_last_seq: batch.last_seq.to_string(),
        session_id: session_id.to_string(),
    };
    target.save_replication_log(target_replication_log).await;

    batch
}

fn find_common_ancestor(
    source_replication_log: &Option<ReplicationLog>,
    target_replication_log: &Option<ReplicationLog>,
) -> Option<String> {
    match source_replication_log {
        Some(source_replication_log) => match target_replication_log {
            Some(target_replication_log) => {
                match source_replication_log.session_id == target_replication_log.session_id {
                    true => match source_replication_log.source_last_seq
                        == target_replication_log.source_last_seq
                    {
                        true => Some(source_replication_log.source_last_seq.to_string()),
                        false => None,
                    },
                    false => None,
                }
            }
            None => None,
        },
        None => None,
    }
}
