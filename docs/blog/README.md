# calmcore_indicat

## 2024-04-29 00:13

calmagent开发分步走，step by step：

* 1, 首先实现路由功能，作为calmserver的router能够工作；
* 2，可以merge多个space；
* 3，reranking，支持常用的reranker模型；
* 4，QA，单轮问答；
* 5，多轮对话，query rewriting；
* 6，长期记忆功能，等等。

## 2024年4月22日 15点18分

* 需要进行一致性协议调研
* 主要针对raft的使用情况

## 2024年4月20日 15点18分

内核角度 这些特性必需

* [ ] Typo tolerance
* [x] Synonyms
* [x] All languages
* [x] Stop words
* [x] Stemming
* [ ] Geo search
* [x] Auto record ID generation
* [x] Schemaless
* [x] Phrase search
* [ ] Query suggestions

## 2024年4月10日 22点39分

* 功能对标不是一下完成的
* 现阶段不是堆砌功能而是把核心做扎实

## 2024年3月28日 22点49分

* calmsearch 是云原生的分布式meilisearch。
* 面向的场景电商场景。不是以向量为中心。
* keeper存储采用rocksdb。负责meta信息。
