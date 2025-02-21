# motivation

rethink search infra with generative AI

why? 

in 2019 we created an open source vector search called 'vearch', originally built for massive product image search.

during the recent several years with the emerging Gen AI and LLM-based applications, we have seen there is a scale shift: a large number of indivial end-users or called tenants with their own data, knowledge, and memory. 

moreover, people's approach to search evolves: multimodality, and conversations


so we are building an AI-powered search system step by step:

* full-text search
* vector search
* hybrid search
* conversational search

morever it has several key features from the systems perspective:

* native multi-tenancy
* compute-storage separation
* fast in-memory indexes, written in Rust

