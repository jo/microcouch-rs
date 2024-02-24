# Microcouch Rust

I'm stupid and megalomaniacal enough to strumble into developing a
stripped-down, minimalist CouchDB implementation in Rust. I call it Microcouch.
I've already got it [pretty far in
JavaScript](https://github.com/jo/microcouch-js).

There are HTTP and SQLite adapters, and a replicator. Replication already works
with quiet well performance and memory characteristics between two HTTP
databases, including attachments. However, these are processed inline (as
base64).

## Tests
To run the test, a running CouchDB server is needed:

```sh
docker run -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=password -p 5984:5984 couchdb
```

in another terminal, run
```sh
COUCHDB_URL=http://admin:password@127.0.0.1:5984 cargo test
```

(c) 2022-2024 Johannes J. Schmidt
