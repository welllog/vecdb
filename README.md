# vecdb

vecdb is an embedded vector store for Go.

完整接口说明见 [API.md](API.md)。

It is designed to feel like a small reusable library:

- single local file persistence with bbolt
- optional sidecar HNSW index reused on reopen when up to date
- persisted metadata postings index for exact-match filtering
- simple document API: open, put, get, delete, count
- HNSW approximate nearest-neighbor search with exact-search fallback
- batch writes for loading and ingestion
- small, explicit types for options and results

## Quick start

```go
import (
	"log"

	vecdb "github.com/welllog/vecdb"
)

store, err := vecdb.Open("./docs.vecdb", 384)
if err != nil {
	log.Fatal(err)
}
defer store.Close()

err = store.Put("doc-1", embedding, vecdb.Metadata{
	"title": "Intro to vector search",
})
if err != nil {
	log.Fatal(err)
}

doc, err := store.Get("doc-1")
if err != nil {
	log.Fatal(err)
}

results, err := store.FindSimilar(queryEmbedding, vecdb.SearchOptions{
	Limit:    5,
	MinScore: 0.7,
})
if err != nil {
	log.Fatal(err)
}

exactResults, err := store.SearchExact(queryEmbedding, 5)
if err != nil {
	log.Fatal(err)
}

_ = doc
_ = results
_ = exactResults
```

## API surface

- Open(path, dimension)
- OpenWithOptions(options)
- Put(id, vector, metadata)
- Upsert(document)
- UpsertMany(documents)
- Get(id)
- Delete(id)
- Count()
- Search(query, limit)
- SearchExact(query, limit)
- FindSimilar(query, options)
- RefreshIndex()
- Dimension()
- Close()

## Search behavior

- `Search` uses HNSW candidate retrieval and exact cosine reranking.
- `SearchExact` performs a full exact scan.
- `FindSimilar` lets you choose exact mode and candidate count explicitly.
- `FindSimilar` with `Filter` stays exact for correctness, but now narrows work through the persisted metadata postings index instead of scanning the whole collection.
- Destructive writes such as overwrite and delete mark the HNSW index stale; while stale, approximate search safely falls back to exact scan until `RefreshIndex` or `Close` persists a rebuilt index.
- Metadata-only overwrites that keep the vector unchanged leave the HNSW index fresh and keep the sidecar reusable across reopen.

```go
results, err := store.FindSimilar(queryEmbedding, vecdb.SearchOptions{
	Limit:      10,
	Candidates: 100,
	MinScore:   0.6,
})

filtered, err := store.FindSimilar(queryEmbedding, vecdb.SearchOptions{
	Limit: 2,
	Filter: vecdb.Metadata{
		"topic": "go",
	},
})
```

## Index tuning

You can tune the HNSW graph when opening the store:

```go
store, err := vecdb.OpenWithOptions(vecdb.Options{
	Path:      "./docs.vecdb",
	IndexPath: "./docs.vecdb.hnsw",
	Dimension: 384,
	HNSW: vecdb.HNSWOptions{
		M:        24,
		EfSearch: 64,
	},
})
```

After destructive updates, you can rebuild the in-memory index explicitly:

```go
if err := store.RefreshIndex(); err != nil {
	log.Fatal(err)
}
```

## Demo

```bash
go run ./cmd/demo
```

The demo seeds a few documents and prints the top matching results.

## Current scope

vecdb uses HNSW for its default indexed retrieval path and reranks the returned candidates with exact cosine similarity. Exact scan is also available as an explicit API. Filtered queries use a persisted metadata postings index to reduce the candidate set before exact scoring. The bbolt file remains the source of truth, while an optional sidecar HNSW file is reused on reopen when its generation matches the database. Overwrite and delete operations that change vectors still mark the ANN index stale instead of rebuilding synchronously, while metadata-only overwrites keep the existing ANN index and sidecar generation valid.
