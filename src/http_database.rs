use async_trait::async_trait;
use lazy_static::lazy_static;
use reqwest::StatusCode;
use std::collections::{HashMap, HashSet};
use tokio::time::Instant;
use url::Url;

lazy_static! {
    static ref START_TIME: Instant = Instant::now();
}

use crate::database::{
    Change, Database, Doc, ReplicationBatch, ReplicationLog, Rev, RevisionsTree, ServerInfo,
};

#[derive(serde::Serialize, Debug)]
struct Revisions {
    start: usize,
    ids: Vec<String>,
}

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
    docs: Vec<DocsResponseEntryOk>,
}

#[derive(serde::Deserialize, Debug)]
struct DocsResponseEntryOk {
    ok: Option<Doc>,
}

#[derive(serde::Deserialize, Debug)]
struct ChangesResponse {
    last_seq: String,
    results: Vec<ChangesResponseResultEntry>,
}

#[derive(serde::Deserialize, Debug)]
struct ChangesResponseResultEntry {
    id: String,
    doc: Option<Doc>,
    changes: Vec<ChangesResponseResultChangesEntry>,
}

#[derive(serde::Deserialize, Debug)]
struct ChangesResponseResultChangesEntry {
    rev: String,
}

#[derive(serde::Serialize, Debug)]
struct BulkDocsRequest<'a> {
    new_edits: bool,
    docs: Vec<&'a Doc>,
}

pub struct HttpDatabase {
    url: Url,
}

impl HttpDatabase {
    pub fn new(url: Url) -> Self {
        Self { url }
    }
}

#[async_trait]
impl Database for HttpDatabase {
    async fn get_server_info(&self) -> ServerInfo {
        let mut url = self.url.join("/").unwrap();
        url.set_path("/");

        let response = reqwest::get(url).await;
        match response {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    let result = response.json::<ServerInfo>().await;
                    match result {
                        Ok(info) => info,
                        _ => panic!("error reading server info"),
                    }
                }
                _ => {
                    let text = response
                        .text()
                        .await
                        .expect("wow, cannot event read the response");
                    panic!("Problem reading server info: {}", text)
                }
            },
            _ => panic!("could not connect to server"),
        }
    }

    async fn save_doc(&self, doc: Doc) -> () {
        let url = self.url.clone();

        let client = reqwest::Client::new();
        client
            .post(url)
            .json(&doc)
            .send()
            .await
            .expect("Cannot save doc");
        // TODO: update doc with rev
    }

    async fn get_doc(&self, id: &str) -> Option<Doc> {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push(id);

        let response = reqwest::get(url).await;
        match response {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    let result = response.json::<Doc>().await;
                    match result {
                        Ok(doc) => Some(doc),
                        _ => panic!("error reading doc"),
                    }
                }
                StatusCode::NOT_FOUND => None,
                _ => {
                    let text = response
                        .text()
                        .await
                        .expect("wow, cannot even read save doc response");
                    panic!("Problem reading doc: {}", text)
                }
            },
            _ => panic!("could not connect to server to get doc"),
        }
    }

    async fn get_replication_log(&self, replication_id: &str) -> Option<ReplicationLog> {
        println!(
            "[{}] # {} get replication log...",
            START_TIME.elapsed().as_millis(),
            &self.url.path(),
        );

        let id = format!("_local/{}", replication_id);
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push(&id);

        let response = reqwest::get(url).await;
        match response {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    let result = response.json::<ReplicationLog>().await;
                    match result {
                        Ok(log) => Some(log),
                        _ => panic!("error reading replication log"),
                    }
                }
                StatusCode::NOT_FOUND => None,
                _ => {
                    let text = response
                        .text()
                        .await
                        .expect("wow, cannot even read save replication log response");
                    panic!("Problem reading replication log: {}", text)
                }
            },
            _ => panic!("could not connect to server to get replication log"),
        }
    }

    async fn save_replication_log(&self, replication_log: ReplicationLog) -> () {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push(&replication_log._id);

        println!(
            "[{}] # {} save replication log at checkpoint {}",
            START_TIME.elapsed().as_millis(),
            &self.url.path(),
            replication_log.source_last_seq,
        );

        let client = reqwest::Client::new();
        let response = client
            .put(url)
            .json(&replication_log)
            .send()
            .await
            .expect("Cannot save replication log");
        match response.status() {
            StatusCode::CREATED => {
                println!(
                    "[{}] # {} save replication log at checkpoint {} done",
                    START_TIME.elapsed().as_millis(),
                    &self.url.path(),
                    replication_log.source_last_seq
                );
            }
            _ => {
                let text = response
                    .text()
                    .await
                    .expect("cannot read response for save replication log");
                panic!("Problem writing replication log: {}", text)
            }
        }
    }

    async fn get_changes(&self, since: Option<String>, batch_size: usize) -> ReplicationBatch {
        println!(
            "[{}] # {} get_changes {:?}",
            START_TIME.elapsed().as_millis(),
            &self.url.path(),
            since
        );

        let limit = batch_size.to_string();
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_changes");
        url.query_pairs_mut().append_pair("feed", "normal");
        url.query_pairs_mut().append_pair("style", "all_docs");
        url.query_pairs_mut().append_pair("limit", &limit);
        url.query_pairs_mut().append_pair("seq_interval", &limit);

        // fetch docs and attachments inline
        url.query_pairs_mut().append_pair("include_docs", "true");
        url.query_pairs_mut().append_pair("attachments", "true");

        if let Some(ref since) = since {
            url.query_pairs_mut().append_pair("since", since);
        }

        let client = reqwest::Client::new();

        let response = client.get(url).send().await;

        match response {
            Ok(response) => {
                match response.status() {
                    StatusCode::OK => {
                        let result = response.json::<ChangesResponse>().await;
                        match result {
                            Ok(mut result) => {
                                let last_seq = result.last_seq;

                                let changes: Vec<Change> = result
                                    .results
                                    .iter_mut()
                                    .map(|row| {
                                        let id = row.id.clone();

                                        let mut docs_by_rev: HashMap<String, Doc> = HashMap::new();

                                        if let Some(mut doc) = row.doc.take() {
                                            match doc._rev.clone() {
                                                Some(rev) => {
                                                    // only use revision-one documents from changes feed
                                                    let (revpos, revid) =
                                                        rev.split_once('-').unwrap();
                                                    if revpos == "1" {
                                                        doc._revisions = Some(RevisionsTree {
                                                            start: 1,
                                                            ids: vec![revid.to_string()],
                                                        });
                                                        docs_by_rev.insert(rev, doc);
                                                    }
                                                }
                                                None => panic!(
                                                    "Very strange, got a doc without rev: {:?}",
                                                    &doc
                                                ),
                                            }
                                        }

                                        let revs = row
                                            .changes
                                            .iter_mut()
                                            .map(|change| {
                                                let rev = change.rev.clone();
                                                let doc = docs_by_rev.remove(&rev);
                                                Rev { rev, doc }
                                            })
                                            .collect();
                                        Change { id, revs }
                                    })
                                    .collect();

                                println!(
                                    "[{}] # {} get_changes {:?} completed, got {} changes",
                                    START_TIME.elapsed().as_millis(),
                                    &self.url.path(),
                                    since,
                                    changes.len()
                                );
                                ReplicationBatch { last_seq, changes }
                            }
                            _ => panic!("error reading changes response"),
                        }
                    }
                    _ => {
                        let text = response.text().await;
                        match text {
                            Ok(text) => panic!("Problem reading changes: {}", text),
                            _ => panic!("lol changes, no error response even"),
                        }
                    }
                }
            }
            _ => panic!("could not connect to changes"),
        }
    }

    async fn get_diff(&self, mut batch: ReplicationBatch) -> ReplicationBatch {
        println!(
            "[{}]   # {} get_diff {}",
            START_TIME.elapsed().as_millis(),
            &self.url.path(),
            batch.nr()
        );

        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_revs_diff");

        let mut revs: HashMap<String, Vec<String>> = HashMap::new();
        for change in batch.changes.iter() {
            let id = change.id.to_string();
            let r = change.revs.iter().map(|c| c.rev.to_string()).collect();
            revs.insert(id, r);
        }
        match revs.len() {
            // hmmm - really?
            1 => {
                println!(
                    "[{}]   # {} get_diff {} completed: nothing to diff",
                    START_TIME.elapsed().as_millis(),
                    &self.url.path(),
                    batch.nr()
                );
                batch
            }
            size => {
                let client = reqwest::Client::new();

                // println!("seinding revs diff request: {:?}", revs);

                let response = client.post(url).json(&revs).send().await;

                match response {
                    Ok(response) => match response.status() {
                        StatusCode::OK => {
                            let result = response.json::<HashMap<String, RevsDiffEntry>>().await;
                            match result {
                                Ok(body) => {
                                    for change in &mut batch.changes {
                                        let mut missing_revs = HashSet::new();

                                        if body.contains_key(&change.id) {
                                            let entry = &body[&change.id];

                                            for rev in entry.missing.iter() {
                                                missing_revs.insert(rev.to_string());
                                            }
                                        }

                                        change.revs.retain(|rev| missing_revs.contains(&rev.rev));
                                    }

                                    println!(
                                        "[{}]   # {} get_diff {} completed: diffed {} docs",
                                        START_TIME.elapsed().as_millis(),
                                        &self.url.path(),
                                        batch.nr(),
                                        size
                                    );

                                    batch
                                }
                                _ => panic!("lol diff result, no response"),
                            }
                        }
                        _ => {
                            let text = response.text().await;
                            match text {
                                Ok(text) => panic!("Problem reading diff: {}", text),
                                _ => panic!("lol diff, even no error response"),
                            }
                        }
                    },
                    _ => panic!("could not connect to diff"),
                }
            }
        }
    }

    async fn get_revs(&self, mut batch: ReplicationBatch) -> ReplicationBatch {
        println!(
            "[{}]   # {} get_revs {}",
            START_TIME.elapsed().as_millis(),
            &self.url.path(),
            batch.nr()
        );

        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_bulk_get");
        url.query_pairs_mut().append_pair("revs", "true");
        url.query_pairs_mut().append_pair("attachments", "true");

        let mut docs = vec![];
        for change in batch.changes.iter() {
            for rev in change.revs.iter() {
                match &rev.doc {
                    None => {
                        let e = DocsRequestEntry {
                            id: change.id.to_string(),
                            rev: rev.rev.to_string(),
                        };
                        docs.push(e);
                    }
                    Some(_) => {}
                }
            }
        }
        match docs.len() {
            0 => {
                println!(
                    "[{}]   # {} get_revs {} completed: nothing to fetch",
                    START_TIME.elapsed().as_millis(),
                    &self.url.path(),
                    batch.nr()
                );
                batch
            }
            size => {
                let docs_request = DocsRequest { docs };

                let client = reqwest::Client::new();
                let response = client.post(url).json(&docs_request).send().await;

                match response {
                    Ok(response) => {
                        match response.status() {
                            StatusCode::OK => {
                                let result = response.json::<DocsResponse>().await;
                                match result {
                                    Ok(mut body) => {
                                        // docs by id hash
                                        let mut docs_by_id = HashMap::new();
                                        for entry in &mut body.results {
                                            let id = entry.id.to_string();

                                            // docs by rev hash
                                            let mut docs_by_rev = HashMap::new();

                                            for doc_ok in &mut entry.docs {
                                                if let Some(doc) = doc_ok.ok.take() {
                                                    match doc._rev.clone() {
                                                        Some(rev) => {
                                                            docs_by_rev.insert(rev, doc);
                                                        },
                                                        None => panic!("Very strange, got a doc without rev: {:?}", &doc)
                                                    }
                                                }
                                            }

                                            docs_by_id.insert(id, docs_by_rev);
                                        }

                                        for change in &mut batch.changes {
                                            if docs_by_id.contains_key(&change.id) {
                                                let mut docs_by_rev =
                                                    docs_by_id.remove(&change.id).unwrap();
                                                for rev in &mut change.revs {
                                                    if docs_by_rev.contains_key(&rev.rev) {
                                                        let doc =
                                                            docs_by_rev.remove(&rev.rev).unwrap();
                                                        rev.doc = Some(doc);
                                                    }
                                                }
                                            }
                                        }

                                        println!(
                                            "[{}]   # {} get_revs {} completed: got {} revs",
                                            START_TIME.elapsed().as_millis(),
                                            &self.url.path(),
                                            batch.nr(),
                                            size
                                        );

                                        batch
                                    }
                                    _ => panic!("lol get docs result, no response"),
                                }
                            }
                            _ => {
                                let text = response.text().await;
                                match text {
                                    Ok(text) => panic!("Problem reading docs: {}", text),
                                    _ => panic!("lol get docs, no error response even"),
                                }
                            }
                        }
                    }
                    _ => panic!("could not connect to get docs"),
                }
            }
        }
    }

    async fn save_revs(&self, mut batch: ReplicationBatch) -> ReplicationBatch {
        println!(
            "[{}]   # {} save_revs {}",
            START_TIME.elapsed().as_millis(),
            &self.url.path(),
            batch.nr()
        );

        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_bulk_docs");

        let mut docs = vec![];
        for change in batch.changes.iter() {
            for rev in change.revs.iter() {
                match &rev.doc {
                    Some(doc) => docs.push(doc),
                    None => {}
                }
            }
        }

        match docs.len() {
            0 => {
                println!(
                    "[{}]   # {} save_revs {} completed",
                    START_TIME.elapsed().as_millis(),
                    &self.url.path(),
                    batch.nr()
                );
                batch
            }
            size => {
                let bulk_docs_request = BulkDocsRequest {
                    docs,
                    new_edits: false,
                };

                let client = reqwest::Client::new();
                let response = client.post(url).json(&bulk_docs_request).send().await;

                match response {
                    Ok(response) => match response.status() {
                        StatusCode::CREATED => {
                            println!(
                                "[{}]   # {} save_revs {} completed: saved {} revs",
                                START_TIME.elapsed().as_millis(),
                                &self.url.path(),
                                batch.nr(),
                                size
                            );

                            batch.changes = vec![];
                            batch
                        }
                        _ => {
                            let text = response.text().await;
                            match text {
                                Ok(text) => panic!("Problem saving docs: {}", text),
                                _ => panic!("lol save docs, no error response even"),
                            }
                        }
                    },
                    _ => panic!("could not connect to save docs"),
                }
            }
        }
    }
}
