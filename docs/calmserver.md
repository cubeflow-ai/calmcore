# calmserver

server -> spaces, 1:n

space -> core -> store(rocksdb by default), 1:1:1

space is the basic unit of storage, indexing, and replication


calmserver can be run on top of cubefs, or configured as async replication based on WAL.


