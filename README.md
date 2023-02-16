# Microcouch Rust
This is a Rust-based microcouch. It is still a work in progress. The design was
worked out in the [JavaScript microcouch](https://github.com/jo/microcouch-js)
and this Rust version will follow it.

Currently there is only one HTTP adapter. Replication works between two HTTP
databases, including attachments. However, these are processed inline (as
base64).

Next steps:

* Implement sqlite-based local database structure, maybe in-memory first.

And there's so much more to do :/ Let's see what this becomes.

(c) 2022-2023 Johannes J. Schmidt
