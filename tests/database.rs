// Run the tests by exporting a `COUCHDB_URL` environment variable like so:
// ```
// COUCHDB_URL=https://admin:password@couchdb.example.com cargo test --test database
// ```

#[cfg(test)]
mod tests {
    use microcouch::{Database, Doc, HttpDatabase, SqliteDatabase};
    use reqwest::Url;
    use std::collections::HashMap;
    use std::env;
    use std::fs;

    struct Fixture<'a> {
        db: &'a (dyn Database + 'static),
    }

    fn setup_http_database(name: &str) -> HttpDatabase {
        let couchdb_url =
            env::var("COUCHDB_URL").expect("missing COUCHDB_URL environment variable");
        let couchdb_url = Url::parse(&couchdb_url).expect("invalid source url");
        let url = couchdb_url.join(name).unwrap();

        let client = reqwest::blocking::Client::new();
        client
            .delete(url.clone())
            .send()
            .expect("should be able to connect to server");
        client
            .put(url.clone())
            .send()
            .expect("should be able to connect to server");

        HttpDatabase::new(url)
    }

    fn setup_sqlite_database(name: &str) -> SqliteDatabase {
        let filename = format!("{}.db", name);
        fs::remove_file(&filename).unwrap_or(());

        SqliteDatabase::new(name)
    }

    fn test<F: Fn(Fixture)>(f: F) {
        let http_db = setup_http_database("test-database");

        f(Fixture { db: &http_db });

        let sqlite_db = setup_sqlite_database("test-database");

        f(Fixture { db: &sqlite_db });
    }

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    #[test]
    fn save_document() {
        test(|f| {
            let doc = Doc::new(Some("mydoc".to_string()), HashMap::new());

            aw!(f.db.save_doc(doc));
        })
    }

    #[test]
    fn save_and_get_document() {
        test(|f| {
            let doc = Doc::new(Some("mydoc".to_string()), HashMap::new());

            aw!(f.db.save_doc(doc));
            let retrieved_doc = aw!(f.db.get_doc("mydoc")).unwrap();

            assert_eq!(retrieved_doc._id, Some("mydoc".to_string()));
        })
    }
}
