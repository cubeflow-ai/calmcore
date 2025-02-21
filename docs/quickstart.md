# Quickstart

```
import calmsea

cs = calmsea.Client(url="http://localhost:1024")

c = cs.create_collection(schema, "collection-A")

s = c.create_space("space-0")

c = cs.collection("collection-B")

s = c.space("space-1")

s.batch_insert(objects)


//both space and collection can be searched

resultsFulltext = s.search(mode="fulltext", q="beijing")

resultsVector = c.search(mode="vector", q="books for little boys interested in history")

resultsHybrid = s.search(mode="hybrid", q="books about the Africa history")


result = s.search(term="asia", properties=["location", "place"], exact=true)

result = s.search(term="hangzhou", properties=["city"], tolerance:2, limit: 3,)

result = c.search(
	mode="hybrid",
	term="hobby",
	vector={
		value=[0.88,...],
		property="embedding",
	},
	weights={
		text=0.7,
		vector=0.3,
	}
)

result = c.search(term="henry", boost={"title":3})


```

* semi-schemaless: a schema is required to represent the properties you want to index and search through, but doesn't have to contain every property; allowing for different properties to be stored in different documents

* automatic embeddings generation

* supported types

schema: property names --> property types

string/number/bool/enum/geo, array of that, vector

* facets

string/number/bool facets


