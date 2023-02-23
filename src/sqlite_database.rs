use async_trait::async_trait;
use rusqlite::{named_params, Connection};
use uuid::Uuid;

use crate::database::{Change, Database, Doc, ReplicationBatch, ReplicationLog, Rev, ServerInfo};

pub struct SqliteDatabase {
    uuid: String,
    filename: String,
}

impl SqliteDatabase {
    // TODO: make this a filename
    pub fn new(id: &str) -> Self {
        let filename = format!("{}.db", id);

        let conn = Connection::open(&filename).unwrap();

        // setup schema
        conn.execute(
            "create table if not exists meta (
                 id text primary key,
                 uuid text not null
             )",
            [],
        )
        .expect("Cannot create meta table");

        conn.execute(
            "create table if not exists replication_logs (
                 id text primary key,
                 source_last_seq text not null,
                 session_id text not null
             )",
            [],
        )
        .expect("Cannot create replication_logs table");

        conn.execute(
            "create table if not exists revs (
                 id text not null,
                 rev text not null,
                 seq integer not null unique,
                 body text not null,
                 primary key (id, rev)
             )",
            [],
        )
        .expect("Cannot create revs table");

        // uuid
        let mut stmt = conn
            .prepare("select uuid from meta where id = ?;")
            .expect("could not prepare statement");

        let mut rows = stmt
            .query([&id])
            .expect("could not execute select statement");

        let uuid = match rows.next().expect("could not get row") {
            Some(row) => row.get(0).unwrap(),
            None => {
                let uuid = Uuid::new_v4().to_string();

                let mut stmt = conn
                    .prepare(
                        "insert into meta(id, uuid) values (:id, :uuid)
                             on conflict(id) do update set uuid=excluded.uuid;",
                    )
                    .expect("could not prepare statement");

                stmt.execute(&[(":id", &id), (":uuid", &uuid.as_str())])
                    .expect("could not execute upsert statement");

                uuid
            }
        };

        Self { uuid, filename }
    }

    fn connection(&self) -> Connection {
        Connection::open(&self.filename).unwrap()
    }
}

#[async_trait]
impl Database for SqliteDatabase {
    async fn get_server_info(&self) -> ServerInfo {
        ServerInfo {
            uuid: self.uuid.to_owned(),
        }
    }

    async fn save_doc(&self, mut doc: Doc) -> () {
        doc._rev = Some("1-made-up-rev".to_owned());

        let mut conn = self.connection();
        let tx = conn.transaction().expect("Could not start transaction");

        let mut seq: i32 = tx
            .query_row("select max(seq) from revs;", [], |row| row.get(0))
            .unwrap_or(0);

        seq += 1;
        tx.execute(
            "insert into revs (id, rev, seq, body) values (:id, :rev, :seq, :body)
                    on conflict(id, rev) do update set seq=excluded.seq, body=excluded.body;",
            named_params! {
                ":id": &doc._id,
                ":rev": &doc._rev,
                ":seq": &seq,
                ":body": &serde_json::to_string(&doc.body).expect("Could not serialize doc")
            },
        )
        .expect("Could not store rev");

        tx.commit().expect("Could not commit transaction");
    }

    async fn get_doc(&self, _id: &str) -> Option<Doc> {
        match self
            .connection()
            .query_row("select rev, body from revs;", [], |row| {
                let body: String = row.get(1).unwrap();
                Ok(Doc::new_with_rev(
                    Some(_id.to_owned()),
                    Some(row.get(0).unwrap()),
                    serde_json::from_str(&body).unwrap(),
                ))
            }) {
            Ok(doc) => Some(doc),
            _ => None,
        }
    }

    async fn get_replication_log(&self, replication_id: &str) -> Option<ReplicationLog> {
        let id = format!("_local/{}", &replication_id);

        let conn = self.connection();

        let mut stmt = conn
            .prepare("select source_last_seq, session_id from replication_logs where id = ?;")
            .expect("could not prepare statement");

        stmt.query_row([&id], |row| {
            Ok(Some(ReplicationLog {
                _id: id.to_owned(),
                source_last_seq: row.get(0).unwrap(),
                session_id: row.get(1).unwrap(),
            }))
        })
        .unwrap_or(None)
    }

    async fn save_replication_log(&self, replication_log: ReplicationLog) -> () {
        let conn = self.connection();

        let mut stmt = conn
            .prepare("insert into replication_logs(id, source_last_seq, session_id) values (:id, :source_last_seq, :session_id)
                     on conflict(id) do update set source_last_seq=excluded.source_last_seq, session_id=excluded.session_id;")
            .expect("could not prepare statement");

        stmt.execute(&[
            (":id", &replication_log._id),
            (":source_last_seq", &replication_log.source_last_seq),
            (":session_id", &replication_log.session_id),
        ])
        .expect("could not execute upsert statement");
    }

    async fn get_changes(&self, since: Option<String>, _batch_size: usize) -> ReplicationBatch {
        let conn = self.connection();

        let since = match since {
            Some(seq) => seq.parse::<i32>().unwrap(),
            None => 0,
        };

        let mut stmt = conn
            .prepare(
                "select seq, id, rev, body from revs
                      where seq > ?
                      order by seq;",
            )
            .expect("could not prepare statement");

        let mut changes: Vec<Change> = vec![];
        let mut last_seq = "0".to_string();
        let rows = stmt
            .query_map([since], |row| {
                let seq_i: i32 = row.get(0).unwrap();
                last_seq = seq_i.to_string();
                let id: String = row.get(1).unwrap();
                let rev: String = row.get(2).unwrap();
                let body: String = row.get(3).unwrap();

                let doc = Doc::new_with_rev(
                    Some(id.to_owned()),
                    Some(rev.to_owned()),
                    serde_json::from_str(&body).unwrap(),
                );
                Ok(Change {
                    id,
                    revs: vec![Rev {
                        rev,
                        doc: Some(doc),
                    }],
                })
            })
            .expect("could not execute select statement");

        for row in rows {
            changes.push(row.unwrap());
        }

        ReplicationBatch { last_seq, changes }
    }

    async fn get_diff(&self, batch: ReplicationBatch) -> ReplicationBatch {
        batch
    }

    async fn get_revs(&self, batch: ReplicationBatch) -> ReplicationBatch {
        batch
    }

    async fn save_revs(&self, batch: ReplicationBatch) -> ReplicationBatch {
        let mut conn = self.connection();
        let tx = conn.transaction().expect("Could not start transaction");

        let mut seq: i32 = tx
            .query_row("select max(seq) from revs;", [], |row| row.get(0))
            .unwrap_or(0);

        for change in batch.changes.iter() {
            for rev in change.revs.iter() {
                match &rev.doc {
                    Some(doc) => {
                        seq += 1;

                        tx.execute("insert into revs (id, rev, seq, body) values (:id, :rev, :seq, :body)
                                    on conflict(id, rev) do update set seq=excluded.seq, body=excluded.body;",
                                    named_params! {
                                        ":id": &doc._id,
                                        ":rev": &doc._rev,
                                        ":seq": &seq,
                                        ":body": &serde_json::to_string(&doc.body).expect("Could not serialize doc")
                                    }).expect("Could not store rev");
                    }
                    None => {}
                }
            }
        }

        tx.commit().expect("Could not commit transaction");
        batch
    }
}
