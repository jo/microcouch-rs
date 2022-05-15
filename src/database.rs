use core::future::ready;
use core::time::Duration;
use futures::{join, stream, Stream, StreamExt, TryStreamExt};
use futures_batch::ChunksTimeoutStreamExt;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::value::Value;
use std::collections::HashMap;
use std::error::Error;
use tokio::io::AsyncBufReadExt;
use tokio_stream::wrappers::LinesStream;
use tokio_util::io::StreamReader;
use url::Url;
use uuid::Uuid;

// TODO: maybe we don't need this and can use a HashMap directly
#[derive(Deserialize, Debug)]
struct ChangeRow {
    id: String,
    seq: Option<String>,
    changes: Vec<Rev>,
}

// TODO: maybe we don't need this and can use HashMap directly
#[derive(serde::Deserialize, Debug)]
struct RevsDiffEntry {
    missing: Vec<String>,
}

#[derive(serde::Serialize, Debug)]
struct DocsRequest {
    docs: Vec<DocsRequestEntry>,
}

#[derive(serde::Serialize, Debug)]
struct DocsRequestEntry {
    id: String,
    rev: String,
}

#[derive(serde::Deserialize, Debug)]
struct DocsResponse {
    results: Vec<DocsResponseEntry>,
}

#[derive(serde::Deserialize, Debug)]
struct DocsResponseEntry {
    id: String,
    docs: Vec<DocsResponseDoc>,
}

#[derive(serde::Deserialize, Debug)]
struct DocsResponseDoc {
    ok: Value,
}

#[derive(serde::Serialize, Debug)]
struct BulkDocsRequest {
    docs: Vec<Value>,
    new_edits: bool,
}

#[derive(serde::Deserialize, Debug)]
pub struct ServerInfo {
    pub uuid: String,
}

#[derive(serde::Deserialize, Debug)]
pub struct DatabaseInfo {
    pub update_seq: String,
}

// TODO: get rid of _rev here
#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct ReplicationLog {
    _id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    _rev: Option<String>,
    source_last_seq: Option<String>,
    session_id: Option<Uuid>,
}

#[derive(Debug)]
pub struct Revs {
    id: String,
    seq: Option<String>,
    revs: Vec<Rev>,
}

#[derive(Deserialize, Debug)]
pub struct Rev {
    rev: String,
    doc: Option<Value>,
}

#[derive(Debug)]
pub struct ReplicationStats {}

async fn replicate(
    source: &Database,
    target: &Database,
) -> Result<Option<ReplicationStats>, Box<dyn Error>> {
    let infos = get_infos(source, target).await;

    let source_server_info = infos.0?.unwrap();
    let target_server_info = infos.1?.unwrap();
    let source_db_info = infos.2?.unwrap();
    let target_db_info = infos.3?.unwrap();

    let replication_id = replication_id(source_server_info, target_server_info);

    let logs = get_replication_logs(source, target, &replication_id).await;

    let mut source_replication_log = logs.0?;
    let mut target_replication_log = logs.1?;

    let since = find_common_ancestor(&source_replication_log, &target_replication_log);
    println!("replicating since {:?}...", since);

    // TODO: do we want a new session id for each batch?
    let session_id = Uuid::new_v4();
    source_replication_log.session_id = Some(session_id);
    target_replication_log.session_id = Some(session_id);

    let concurrency = 8;
    let batch_size = 128;

    let mut stream = source
        // get changes
        .get_changes(&since)
        .await?
        // replicate batches
        .chunks_timeout(batch_size, Duration::new(0, 200 * 1000 * 1000))
        .map(|batch| {
            replicate_batch(
                &source,
                &target,
                &source_replication_log,
                &target_replication_log,
                batch,
            )
        })
        .buffered(concurrency)
        // store checkpoints
        .map(|batch| batch.unwrap())
        .flat_map(|batch| stream::iter(batch))
        .chunks_timeout(batch_size * concurrency, Duration::new(10, 0))
        .map(|mut batch| {
            let revs = batch.pop().unwrap();

            let source_replication_log = ReplicationLog {
                _id: source_replication_log._id.clone(),
                _rev: source_replication_log._rev.clone(),
                source_last_seq: revs.seq.clone(),
                session_id: Some(session_id.clone()),
            };
            let target_replication_log = ReplicationLog {
                _id: target_replication_log._id.clone(),
                _rev: target_replication_log._rev.clone(),
                source_last_seq: revs.seq.clone(),
                session_id: Some(session_id.clone()),
            };
            save_replication_logs(
                &source,
                &target,
                source_replication_log,
                target_replication_log,
            )
        })
        // drain stream
        .filter_map(|x| async move { x.await.ok().map(Ok) })
        .forward(futures::sink::drain())
        .await
        .unwrap();

    println!("replication since {:?} done", since);

    Ok(Some(ReplicationStats {}))
}

async fn replicate_batch(
    source: &Database,
    target: &Database,
    source_replication_log: &ReplicationLog,
    target_replication_log: &ReplicationLog,
    batch: Vec<Revs>,
) -> Result<Vec<Revs>, Box<dyn Error>> {
    let diffs = target.get_diff(batch).await?;
    let revs = source.get_revs(diffs).await?;
    let save = target.save_revs(revs).await?;

    Ok(save)
}

async fn get_infos(
    source: &Database,
    target: &Database,
) -> (
    Result<std::option::Option<ServerInfo>, Box<dyn Error>>,
    Result<std::option::Option<ServerInfo>, Box<dyn Error>>,
    Result<std::option::Option<DatabaseInfo>, Box<dyn Error>>,
    Result<std::option::Option<DatabaseInfo>, Box<dyn Error>>,
) {
    let source_server_info = source.get_server_info();
    let target_server_info = target.get_server_info();
    let source_db_info = source.get_database_info();
    let target_db_info = target.get_database_info();
    join!(
        source_server_info,
        target_server_info,
        source_db_info,
        target_db_info
    )
}

fn replication_id(source_server_info: ServerInfo, target_server_info: ServerInfo) -> String {
    let replication_id_data = vec![source_server_info.uuid, target_server_info.uuid];
    let replication_digest = md5::compute(replication_id_data.join(""));
    format!("{:x}", replication_digest)
}

async fn get_replication_logs(
    source: &Database,
    target: &Database,
    replication_id: &str,
) -> (
    Result<ReplicationLog, Box<dyn Error>>,
    Result<ReplicationLog, Box<dyn Error>>,
) {
    let source_replication_log = source.get_replication_log(replication_id);
    let target_replication_log = target.get_replication_log(replication_id);
    join!(source_replication_log, target_replication_log)
}

async fn save_replication_logs(
    source: &Database,
    target: &Database,
    source_replication_log: ReplicationLog,
    target_replication_log: ReplicationLog,
) -> Result<(), Box<dyn Error>> {
    // TODO: can we save them in parallel?
    // let save_source_replication_log = source.save_replication_log(&source_replication_log);
    // let save_target_replication_log = target.save_replication_log(&target_replication_log);
    // let request = join!(save_source_replication_log, save_target_replication_log);

    source.save_replication_log(&source_replication_log).await?;
    target.save_replication_log(&target_replication_log).await?;

    Ok(())
}

fn find_common_ancestor(
    source_replication_log: &ReplicationLog,
    target_replication_log: &ReplicationLog,
) -> Option<String> {
    match source_replication_log.session_id == target_replication_log.session_id {
        true => match &source_replication_log.source_last_seq {
            Some(source_last_seq) => match &target_replication_log.source_last_seq {
                Some(target_source_last_seq) => match source_last_seq == target_source_last_seq {
                    true => Some(source_last_seq.to_string()),
                    false => None,
                },
                None => None,
            },
            None => None,
        },
        false => None,
    }
}

pub struct Database {
    url: Url,
}

impl Database {
    pub fn new(url: Url) -> Self {
        Self { url }
    }

    pub async fn pull(
        &self,
        source: &Database,
    ) -> Result<Option<ReplicationStats>, Box<dyn Error>> {
        replicate(source, self).await
    }

    pub async fn push(
        &self,
        target: &Database,
    ) -> Result<Option<ReplicationStats>, Box<dyn Error>> {
        replicate(self, target).await
    }

    pub async fn get_server_info(&self) -> Result<Option<ServerInfo>, Box<dyn Error>> {
        let mut url = self.url.join("/").unwrap();
        url.set_path("/");

        let response = reqwest::get(url).await?;
        match response.status() {
            StatusCode::OK => {
                let data = response.json::<ServerInfo>().await?;
                Ok(Some(data))
            }
            _ => {
                let text = response.text().await?;
                panic!("Problem reading server info: {}", text)
            }
        }
    }

    pub async fn get_database_info(&self) -> Result<Option<DatabaseInfo>, Box<dyn Error>> {
        let url = self.url.clone();

        let response = reqwest::get(url).await?;
        match response.status() {
            StatusCode::OK => {
                let data = response.json::<DatabaseInfo>().await?;
                Ok(Some(data))
            }
            _ => {
                let text = response.text().await?;
                panic!("Problem reading database info: {}", text)
            }
        }
    }

    pub async fn get_replication_log(
        &self,
        replication_id: &str,
    ) -> Result<ReplicationLog, Box<dyn Error>> {
        println!("get replication log...");

        let id = format!("_local/{}", replication_id);
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push(&id);

        let response = reqwest::get(url).await?;
        match response.status() {
            StatusCode::OK => {
                let data = response.json::<ReplicationLog>().await?;
                Ok(data)
            }
            StatusCode::NOT_FOUND => Ok(ReplicationLog {
                _id: id,
                _rev: None,
                source_last_seq: None,
                session_id: None,
            }),
            _ => {
                let text = response.text().await?;
                panic!("Problem reading replication log: {}", text)
            }
        }
    }

    pub async fn save_replication_log(
        &self,
        replication_log: &ReplicationLog,
    ) -> Result<(), Box<dyn Error>> {
        println!(
            "save replication log at checkpoint {:?}",
            replication_log.source_last_seq
        );

        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push(&replication_log._id);

        let client = reqwest::Client::new();
        let response = client.put(url).json(&replication_log).send().await?;
        match response.status() {
            StatusCode::CREATED => {
                println!(
                    "save replication log at checkpoint {:?} done",
                    replication_log.source_last_seq
                );
                Ok(())
            }
            _ => {
                let text = response.text().await?;
                panic!("Problem writing replication log: {}", text)
            }
        }
    }

    pub async fn get_changes(
        &self,
        since: &Option<String>,
    ) -> Result<impl Stream<Item = Revs>, Box<dyn Error>> {
        println!("get changes...");

        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_changes");
        url.query_pairs_mut().append_pair("feed", "continuous");
        url.query_pairs_mut().append_pair("timeout", "0");
        url.query_pairs_mut().append_pair("style", "all_docs");
        // url.query_pairs_mut().append_pair("seq_interval", "100");
        match since {
            Some(since) => {
                url.query_pairs_mut().append_pair("since", since);
            }
            None => {}
        };

        let client = reqwest::Client::new();

        let response = client.get(url).send().await?.bytes_stream();

        fn convert_err(err: reqwest::Error) -> std::io::Error {
            todo!()
        }
        let reader = StreamReader::new(response.map_err(convert_err));

        let lines = reader.lines();
        let lines_stream = LinesStream::new(lines);

        let changes_stream = lines_stream.map(|line| match line {
            Ok(json) => {
                let row: Option<ChangeRow> = serde_json::from_str(&json).unwrap_or(None);
                match row {
                    Some(row) => {
                        let change = Revs {
                            id: row.id,
                            seq: row.seq,
                            revs: row.changes,
                        };
                        Some(change)
                    }
                    None => None,
                }
            }
            _ => None,
        });

        // filter out None values
        let filtered_stream = changes_stream.filter(|x| match x {
            Some(_) => ready(true),
            None => ready(false),
        });

        // unwrap Option
        let unwrapped_stream = filtered_stream.map(|x| x.unwrap());

        Ok(unwrapped_stream)
    }

    pub async fn get_diff(&self, batch: Vec<Revs>) -> Result<Vec<Revs>, Box<dyn Error>> {
        println!("get diff for {} revs...", batch.len());

        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_revs_diff");

        let mut revs: HashMap<String, Vec<String>> = HashMap::new();
        for change in batch.iter() {
            let id = change.id.clone();
            let r = change.revs.iter().map(|c| c.rev.clone()).collect();
            revs.insert(id, r);
        }
        let client = reqwest::Client::new();

        let response = client.post(url).json(&revs).send().await?;

        match response.status() {
            StatusCode::OK => {
                let body = response.json::<HashMap<String, RevsDiffEntry>>().await?;

                let mut changes = vec![];

                // TODO: make it nice

                for change in batch.iter() {
                    if body.contains_key(&change.id) {
                        let entry = &body[&change.id];
                        let c = Revs {
                            id: change.id.clone(),
                            seq: change.seq.clone(),
                            revs: entry
                                .missing
                                .iter()
                                .map(|rev| Rev {
                                    rev: rev.to_string(),
                                    doc: None,
                                })
                                .collect(),
                        };
                        changes.push(c);
                    } else {
                        let c = Revs {
                            id: change.id.clone(),
                            seq: change.seq.clone(),
                            revs: vec![],
                        };
                        changes.push(c);
                    };
                }

                println!("get diff for {} revs done", batch.len());
                Ok(changes)
            }
            _ => {
                let text = response.text().await?;
                panic!("Problem reading revs diff: {}", text)
            }
        }
    }

    pub async fn get_revs(&self, mut batch: Vec<Revs>) -> Result<Vec<Revs>, Box<dyn Error>> {
        println!("get revs for {} revs...", batch.len());

        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_bulk_get");
        url.query_pairs_mut().append_pair("revs", "true");
        url.query_pairs_mut().append_pair("attachments", "true");

        let mut docs = vec![];
        for change in batch.iter() {
            for rev in change.revs.iter() {
                let e = DocsRequestEntry {
                    id: change.id.to_string(),
                    rev: rev.rev.to_string(),
                };
                docs.push(e);
            }
        }
        let docs_request = DocsRequest { docs };

        let client = reqwest::Client::new();
        let response = client.post(url).json(&docs_request).send().await?;

        match response.status() {
            StatusCode::OK => {
                let body = response.json::<DocsResponse>().await?;

                // TODO: make it nice

                // seqs by id hash
                let mut seqs_by_id = HashMap::new();
                for change in batch.iter() {
                    seqs_by_id.insert(change.id.to_string(), change.seq.clone());
                }

                // docs by id hash
                let mut docs_by_id = HashMap::new();
                for entry in body.results.iter() {
                    let id = entry.id.to_string();
                    let revs: Vec<Rev> = vec![];
                    docs_by_id.insert(id, revs);
                }

                // collect docs and group by id
                for entry in body.results.iter() {
                    for doc_ok in entry.docs.iter() {
                        let id = entry.id.to_string();
                        let doc = doc_ok.ok.clone();
                        let rev = doc["_rev"].to_string();

                        docs_by_id.entry(id).and_modify(|e| {
                            let r = Rev {
                                rev,
                                doc: Some(doc),
                            };
                            e.push(r)
                        });
                    }
                }

                // build result
                let mut result = vec![];
                while let Some(change) = batch.pop() {
                    if docs_by_id.contains_key(&change.id) {
                        let seq = change.seq.clone();
                        let revs = docs_by_id.remove(&change.id).unwrap();
                        let r = Revs {
                            id: change.id.clone(),
                            seq,
                            revs,
                        };
                        result.push(r);
                    } else {
                        result.push(change);
                    }
                }

                result.reverse();
                println!("get revs for {} revs done", result.len());
                Ok(result)
            }
            _ => {
                let text = response.text().await?;
                panic!("Problem reading docs: {}", text)
            }
        }
    }

    pub async fn save_revs(&self, batch: Vec<Revs>) -> Result<Vec<Revs>, Box<dyn Error>> {
        println!("save revs for {} revs...", batch.len());

        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_bulk_docs");

        let mut docs = vec![];
        for change in batch.iter() {
            for rev in change.revs.iter() {
                let doc = rev.doc.clone().unwrap();
                docs.push(doc);
            }
        }

        let bulk_docs_request = BulkDocsRequest {
            docs,
            new_edits: false,
        };

        let client = reqwest::Client::new();
        let response = client.post(url).json(&bulk_docs_request).send().await?;

        match response.status() {
            StatusCode::CREATED => {
                println!("save revs for {} revs done", batch.len());
                Ok(batch)
            }
            _ => {
                let text = response.text().await?;
                panic!("Problem writing docs: {}", text)
            }
        }
    }
}
