# concepts

## data model

collections/indexes -> partitions/spaces -> objects & links -> fields

objects are also called records or documents

a collection is a record type that contains fields and is connected by links.

only indexed fields need to be defined; any field can be missing.

a space is a physcial partition of a collection.

a field can be a scalar or vector; multi-vector support

### summary

* cluster
* collection
* space
* document
* field
* scalar index
* vector index


## components

* calmkeeper - the control layer
* calmserver - the index layer
* calmrouter - the routing layer
* calmanswer - the agent layer


## API

* AddCollection
* AddSpace
* AddRecords
* AddLinks
* Search
* Assistant

## query language

CQL

## cluster metadata

* servers, id -> info

* collections, name -> id -> mappings

* spaces, name -> id -> servers

note both collection id and space id are globally unique.

## architecture

several optional modes:

* embedded, the calmcore engine library called in the client
* standalone, a single calmserver
* distributed, compute-storage separation
* distributed, shared-nothing replication


### embedded

calmcore library, no server, no inter-process call

### singleserver

standalone calmserver with gRPC & REST interface, calmserver -> calmcore

### compute-storage separation

leverage CubeFS to decouple compute and storage

special WAL files on CubeFS for performance optimization

### shared-nothing replication

spaces work as the replication unit, async WAL replication

### calmkeeper


### calmserver

### calmrouter

### calmanswer


## schema management - semi-schemaless

* not strict - only indexed fields need to be defined

* sparse fields - any field can be missing

* the automatic detection and addition of new indexed fields


## multitenancy

Goal:

* each tenant has a dedicate, lighweight space
* add new nodes to the cluster to scale out - new tenants are hosted on the nodes with lowest workload
* handle tens of thousands active tenants per node, dynamically loading/offloading active/inactive spaces - lazy loading
* spaces can be further sharded or migrated between node groups

### mapping from spaces to servers

a straight solution is fixed partition count for each collection, hashing of spaces ids to partitions - limited scale, load unbalancing

so we adopts that the manager maintains a routing from spaces to servers and the clients hold an LRU cache of it

### space migration

scheduled by the calmkeeper in both cases:
* compute/storage separtion
* multi-raft replication


## hybrid search

a combination of semantic, structured and keyword-based search

two approaches of reranking: score-based, relevance-based

no one-size-fits-all solution, so need to support different rerankers

### filtering

pre-filtering before vector search

### semantic search

two key elements: embedding generation and kNN search.



