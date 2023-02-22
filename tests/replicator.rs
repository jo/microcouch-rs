// Run the tests by exporting a `COUCHDB_URL` environment variable like so:
// ```
// COUCHDB_URL=https://admin:password@couchdb.example.com cargo test --test replicator
// ```

#[cfg(test)]
mod tests {
    use microcouch::{Database, Doc, HttpDatabase, SqliteDatabase, replicate};
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use reqwest::Url;

    // FIXME: it does not work - doesn't have a size known at compile-time
    // https://stackoverflow.com/questions/26212397/how-do-i-specify-that-a-struct-field-must-implement-a-trait
    // try boxes again
    struct Fixture<'a> {
        source: &'a (dyn Database + 'a),
        target: &'a (dyn Database + 'a),
    }

    fn setup_http_database(name: &str) -> HttpDatabase {
        let couchdb_url = env::var("COUCHDB_URL").expect("missing COUCHDB_URL environment variable");
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
        fs::remove_file(&filename);

        SqliteDatabase::new(name)
    }

    fn test<F: Fn(Fixture)>(f: F) {
        // http -> http
        f(Fixture {
            source: &setup_http_database("test-source"),
            target: &setup_http_database("test-target"),
        });
        
        // http -> sqlite
        f(Fixture {
            source: &setup_http_database("test-source"),
            target: &setup_sqlite_database("test-target"),
        });
        
        // not implemented yet
        // sqlite -> http
        // f(Fixture {
        //     source: &setup_sqlite_database("test-source"),
        //     target: &setup_http_database("test-target"),
        // });
        // 
        // sqlite -> sqlite
        // f(Fixture {
        //     source: &setup_sqlite_database("test-source"),
        //     target: &setup_sqlite_database("test-target"),
        // });
    }

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    #[test]
    fn replicate_single_document() {
        test(|f| {
            let doc = Doc::new(Some("mydoc".to_string()), HashMap::new());
            aw!(f.source.save_doc(doc));

            aw!(replicate(f.source, f.target, 4, 64));
            
            let target_doc = aw!(f.target.get_doc("mydoc")).unwrap();

            assert_eq!(target_doc._id, Some("mydoc".to_string()));
            
            let source_doc = aw!(f.source.get_doc("mydoc")).unwrap();
            assert_eq!(target_doc._rev, source_doc._rev);
        })
    }
}
