# product overview

* generative AI answer engine

* AI search

* open source and cloud-based service


## features

* out of the box: storage, embedding generation, retrieval, answer generation, conversation history, et al.

* multi-tenancy: each tenant has an isolated space

* multilingual support, optimized for English and Chinese

* fulltext search, vector search, hybrid search, and conversational search

* integrated data chunking and vectorization, and AI enrichment for content extraction and transformation

* semantic ranking


## data model

* collections, spaces, objects, fields, embeddings, links

objects are represented as JSON documents, composed of multiple fields, which are attribute-value pairs.

cross-references as 'links'

documents are grouped into collections, which can be partitioned into spaces.

collection settings define options for customizing search behavior.

collections can have multiple named vectors.


## API reference

### collections

### spaces

### objects

### search

### assistant

## SDKs

you can use the API wrappers in your programming language: 
Go, Java, Python, Rust


## tools

* crawler
* database sync


## search preview

## settings

* attributes
* stopwords
* pagination
* faceted search
* embedders

## monitoring & analytics

## security

## upgrade

## data backup

dumps vs snapshots

* dumps - just documents, no indexes
* snapshots - both documents and their indexes

## tokenizers

## suggesters

to enable typeahead/autocomplete or 'search-as-you-type'

## language analyzers

## Relevance

### semantic ranking

### BM25 ranking for keyword queries

### reciprocal rank fusion in hybrid search


