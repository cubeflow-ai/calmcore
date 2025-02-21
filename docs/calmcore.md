# calmcore

the core engine for data indexing and storage

partitions within a collection are self-contained storage spaces.

each space corresponds to a calmcore instance, consisting of three main components:

* a key-value store for the objects
* an inverted index
* an implementation of HNSW vector index supporting CRUD


## internals

* name -> id, id -> record, as two pairs of key-values in rocksdb
* the inverted index can be in-memory or implemented using an LSM-Tree approach
* the vector index supports multi-vector documents and online insert/delete

## v0.1

* calmcore = data store + inverted index + vector index

datastore is based on rocksdb; both scalar and vector index are totally in memory



