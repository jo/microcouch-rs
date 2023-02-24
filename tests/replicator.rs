// Run the tests by exporting a `COUCHDB_URL` environment variable like so:
// ```
// COUCHDB_URL=https://admin:password@couchdb.example.com cargo test --test replicator
// ```

#[cfg(test)]
mod tests {
    use microcouch::{replicate, Database, Doc, HttpDatabase, SqliteDatabase};
    use reqwest::Url;
    use std::collections::HashMap;
    use std::env;
    use std::fs;

    // // FIXME: it does not work - doesn't have a size known at compile-time
    // // https://stackoverflow.com/questions/26212397/how-do-i-specify-that-a-struct-field-must-implement-a-trait
    // // try boxes again
    // struct Fixture<'a> {
    //     source: &'a (dyn Database),
    //     target: &'a (dyn Database),
    // }

    fn setup_http_database(dbname: &str) -> HttpDatabase {
        let couchdb_url =
            env::var("COUCHDB_URL").expect("missing COUCHDB_URL environment variable");
        let couchdb_url = Url::parse(&couchdb_url).expect("invalid source url");
        let url = couchdb_url.join(dbname).unwrap();

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

    fn setup_sqlite_database(filename: &str) -> SqliteDatabase {
        fs::remove_file(&filename).unwrap_or(());

        SqliteDatabase::new(filename)
    }

    // fn test<F: Fn(Fixture)>(f: F) {
    //     // http -> http
    //     f(Fixture {
    //         source: &setup_http_database("test-source"),
    //         target: &setup_http_database("test-target"),
    //     });
    //
    //     // http -> sqlite
    //     f(Fixture {
    //         source: &setup_http_database("test-source"),
    //         target: &setup_sqlite_database("tests/dbs/test-target.db"),
    //     });
    //
    //     // not implemented yet
    //     // sqlite -> http
    //     // f(Fixture {
    //     //     source: &setup_sqlite_database("tests/dbs/test-source.db"),
    //     //     target: &setup_http_database("test-target"),
    //     // });
    //     //
    //     // sqlite -> sqlite
    //     // f(Fixture {
    //     //     source: &setup_sqlite_database("tests/dbs/test-source.db"),
    //     //     target: &setup_sqlite_database("tests/dbs/test-target.db"),
    //     // });
    // }

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    // #[test]
    // fn replicate_single_document() {
    //     test(|f| {
    //         let doc = Doc::new(Some("mydoc".to_string()), None, HashMap::new());
    //         aw!(f.source.save_doc(doc));

    //         let source = f.source;
    //         let target = f.target;
    //         aw!(replicate(source, target, 4, 64));
    //
    //          let target_result = aw!(target.get_doc("mydoc"));
    //          assert!(target_result.is_some());
    //          let target_doc = target_result.unwrap();

    //         assert_eq!(target_doc._id, Some("mydoc".to_string()));
    //
    //         let source_doc = aw!(f.source.get_doc("mydoc")).unwrap();
    //         assert_eq!(target_doc._rev, source_doc._rev);
    //     })
    // }

    #[test]
    fn replicate_single_document_from_http_to_http() {
        let source = setup_http_database("test-source");
        let target = setup_http_database("test-target");

        let doc = Doc::new(Some("mydoc".to_string()), None, HashMap::new());
        aw!(source.save_doc(doc));

        aw!(replicate(&source, &target, 4, 64));

        let target_result = aw!(target.get_doc("mydoc"));
        assert!(target_result.is_some());
        let target_doc = target_result.unwrap();

        assert_eq!(target_doc._id, Some("mydoc".to_string()));

        let source_result = aw!(source.get_doc("mydoc"));
        assert!(source_result.is_some());
        let source_doc = source_result.unwrap();
        assert_eq!(target_doc._rev, source_doc._rev);
    }

    #[test]
    fn replicate_single_document_from_http_to_sqlite() {
        let source = setup_http_database("test-source");
        let target = setup_sqlite_database("tests/dbs/test-target.db");

        let doc = Doc::new(Some("mydoc".to_string()), None, HashMap::new());
        aw!(source.save_doc(doc));

        aw!(replicate(&source, &target, 4, 64));

        let target_result = aw!(target.get_doc("mydoc"));
        assert!(target_result.is_some());
        let target_doc = target_result.unwrap();

        assert_eq!(target_doc._id, Some("mydoc".to_string()));

        let source_result = aw!(source.get_doc("mydoc"));
        assert!(source_result.is_some());
        let source_doc = source_result.unwrap();
        assert_eq!(target_doc._rev, source_doc._rev);
    }

    #[test]
    fn replicate_single_document_from_sqlite_to_http() {
        let source = setup_sqlite_database("tests/dbs/test-source.db");
        let target = setup_http_database("test-target");

        let doc = Doc::new(Some("mydoc".to_string()), None, HashMap::new());
        aw!(source.save_doc(doc));

        aw!(replicate(&source, &target, 4, 64));

        let target_result = aw!(target.get_doc("mydoc"));
        assert!(target_result.is_some());
        let target_doc = target_result.unwrap();

        assert_eq!(target_doc._id, Some("mydoc".to_string()));

        let source_result = aw!(source.get_doc("mydoc"));
        assert!(source_result.is_some());
        let source_doc = source_result.unwrap();
        assert_eq!(target_doc._rev, source_doc._rev);
    }

    #[test]
    fn replicate_single_document_from_sqlite_to_sqlite() {
        let source = setup_sqlite_database("tests/dbs/test-source.db");
        let target = setup_sqlite_database("tests/dbs/test-target.db");

        let doc = Doc::new(Some("mydoc".to_string()), None, HashMap::new());
        aw!(source.save_doc(doc));

        aw!(replicate(&source, &target, 4, 64));

        let target_result = aw!(target.get_doc("mydoc"));
        assert!(target_result.is_some());
        let target_doc = target_result.unwrap();

        assert_eq!(target_doc._id, Some("mydoc".to_string()));

        let source_result = aw!(source.get_doc("mydoc"));
        assert!(source_result.is_some());
        let source_doc = source_result.unwrap();
        assert_eq!(target_doc._rev, source_doc._rev);
    }
}
