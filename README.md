# Microcouch Rust
This is a Rust-based microcouch. It is still a work in progress. The design was
worked out in the [JavaScript microcouch](https://github.com/jo/microcouch-js)
and this Rust version will follow it.

Currently there is only one HTTP adapter. Replication works between two HTTP
databases, including attachments. However, these are processed inline (as
base64).

Next steps:

* implement rev tree merging and the missing sqlite db methods

And there's so much more to do :/ Let's see what this becomes.

## Tests
To run the test, a running CouchDB server is needed:

```sh
COUCHDB_URL=https://admin:password@couchdb.example.com cargo test 
```

(c) 2022-2023 Johannes J. Schmidt
