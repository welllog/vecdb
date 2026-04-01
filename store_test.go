package vecdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	bbolt "go.etcd.io/bbolt"
)

func TestStoreDocumentLifecycle(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() {
		_ = store.Close()
	})

	err := store.Put("doc-1", []float32{1, 0, 0}, Metadata{"title": "first"})
	if err != nil {
		t.Fatalf("put document: %v", err)
	}

	doc, err := store.Get("doc-1")
	if err != nil {
		t.Fatalf("get document: %v", err)
	}

	if doc.ID != "doc-1" {
		t.Fatalf("unexpected document id: %s", doc.ID)
	}
	if len(doc.Vector) != 3 || doc.Vector[0] != 1 {
		t.Fatalf("unexpected vector: %#v", doc.Vector)
	}
	if doc.Metadata["title"] != "first" {
		t.Fatalf("unexpected metadata: %#v", doc.Metadata)
	}

	count, err := store.Count()
	if err != nil {
		t.Fatalf("count documents: %v", err)
	}
	if count != 1 {
		t.Fatalf("unexpected count: %d", count)
	}

	err = store.Delete("doc-1")
	if err != nil {
		t.Fatalf("delete document: %v", err)
	}

	_, err = store.Get("doc-1")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestStoreUpsertManyAndSearch(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() {
		_ = store.Close()
	})

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{0.91, 0.07, 0.01, 0.01}, Metadata: Metadata{"title": "golang embedding intro"}},
		{ID: "doc-2", Vector: []float32{0.85, 0.11, 0.03, 0.01}, Metadata: Metadata{"title": "vector search basics"}},
		{ID: "doc-3", Vector: []float32{0.10, 0.10, 0.70, 0.10}, Metadata: Metadata{"title": "sqlite persistence"}},
	})
	if err != nil {
		t.Fatalf("upsert many: %v", err)
	}

	results, err := store.FindSimilar([]float32{0.88, 0.10, 0.01, 0.01}, SearchOptions{Limit: 2, MinScore: 0.5})
	if err != nil {
		t.Fatalf("search documents: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("unexpected number of results: %d", len(results))
	}
	if results[0].ID != "doc-2" {
		t.Fatalf("expected doc-2 first, got %s", results[0].ID)
	}
	if results[1].ID != "doc-1" {
		t.Fatalf("expected doc-1 second, got %s", results[1].ID)
	}
}

func TestStorePersistsBinaryPayloadFormat(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() {
		_ = store.Close()
	})

	if err := store.Put("doc-1", []float32{1, 0, 0}, Metadata{"title": "first"}); err != nil {
		t.Fatalf("put document: %v", err)
	}

	var payload []byte
	err := store.db.View(func(tx *bbolt.Tx) error {
		raw := tx.Bucket(documentsBucket).Get([]byte("doc-1"))
		if raw == nil {
			return ErrNotFound
		}
		payload = append(payload[:0], raw...)
		return nil
	})
	if err != nil {
		t.Fatalf("read stored payload: %v", err)
	}
	if !bytes.HasPrefix(payload, binaryDocumentMagic) {
		t.Fatalf("expected binary payload prefix %q, got %q", binaryDocumentMagic, payload[:min(len(payload), len(binaryDocumentMagic))])
	}
	if bytes.HasPrefix(payload, []byte("{")) {
		t.Fatalf("expected non-JSON payload, got %q", payload)
	}
	if _, err := store.Get("doc-1"); err != nil {
		t.Fatalf("read binary document: %v", err)
	}
}

func TestSearchExactReturnsExpectedResult(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() {
		_ = store.Close()
	})

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0, 0}, Metadata: Metadata{"topic": "go"}},
		{ID: "doc-2", Vector: []float32{0, 1, 0, 0}, Metadata: Metadata{"topic": "db"}},
	})
	if err != nil {
		t.Fatalf("upsert many: %v", err)
	}

	results, err := store.SearchExact([]float32{0, 1, 0, 0}, 1)
	if err != nil {
		t.Fatalf("exact search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-2" {
		t.Fatalf("unexpected exact results: %#v", results)
	}
}

func TestSearchExactOrdersTiesByID(t *testing.T) {
	store := openTestStore(t, 2)
	t.Cleanup(func() {
		_ = store.Close()
	})

	err := store.UpsertMany([]Document{
		{ID: "doc-b", Vector: []float32{1, 0}, Metadata: Metadata{"title": "b"}},
		{ID: "doc-a", Vector: []float32{1, 0}, Metadata: Metadata{"title": "a"}},
		{ID: "doc-c", Vector: []float32{0, 1}, Metadata: Metadata{"title": "c"}},
	})
	if err != nil {
		t.Fatalf("upsert many: %v", err)
	}

	results, err := store.SearchExact([]float32{1, 0}, 2)
	if err != nil {
		t.Fatalf("exact search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("unexpected result count: %d", len(results))
	}
	if results[0].ID != "doc-a" || results[1].ID != "doc-b" {
		t.Fatalf("expected tie order by id, got %#v", results)
	}
}

func TestFindSimilarWithFilterUsesMetadataIndexCandidates(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() {
		_ = store.Close()
	})

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0, 0}, Metadata: Metadata{"topic": "go", "lang": "en"}},
		{ID: "doc-2", Vector: []float32{0.95, 0.05, 0, 0}, Metadata: Metadata{"topic": "vector", "lang": "en"}},
		{ID: "doc-3", Vector: []float32{0, 1, 0, 0}, Metadata: Metadata{"topic": "go", "lang": "zh"}},
	})
	if err != nil {
		t.Fatalf("upsert many: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{
		Limit:  2,
		Filter: Metadata{"topic": "go", "lang": "zh"},
	})
	if err != nil {
		t.Fatalf("filtered search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-3" {
		t.Fatalf("unexpected filtered results: %#v", results)
	}
}

func TestFindSimilarWithFilterSkipsNonMatchingCorruptedPayloads(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() {
		_ = store.Close()
	})

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0, 0}, Metadata: Metadata{"topic": "go", "lang": "zh"}},
		{ID: "doc-2", Vector: []float32{0, 1, 0, 0}, Metadata: Metadata{"topic": "db", "lang": "en"}},
	})
	if err != nil {
		t.Fatalf("upsert many: %v", err)
	}

	corrupted, err := store.marshalDocument(Document{ID: "doc-2", Vector: []float32{0, 1, 0, 0}, Metadata: Metadata{"topic": "db", "lang": "en"}})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	corrupted = corrupted[:len(corrupted)-4]

	err = store.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(documentsBucket).Put([]byte("doc-2"), corrupted)
	})
	if err != nil {
		t.Fatalf("corrupt non-matching payload: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{
		Limit:  1,
		Filter: Metadata{"topic": "go", "lang": "zh"},
	})
	if err != nil {
		t.Fatalf("filtered search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("unexpected filtered results with corrupted non-match: %#v", results)
	}
}

func TestFindSimilarApproximateDoesNotDecodeStoredVector(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() {
		_ = store.Close()
	})

	doc := Document{ID: "doc-1", Vector: []float32{1, 0, 0, 0}, Metadata: Metadata{"title": "kept"}}
	if err := store.Upsert(doc); err != nil {
		t.Fatalf("upsert document: %v", err)
	}

	corrupted, err := store.marshalDocument(doc)
	if err != nil {
		t.Fatalf("marshal corrupted payload source: %v", err)
	}
	corrupted = corrupted[:len(corrupted)-4]

	err = store.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(documentsBucket).Put([]byte(doc.ID), corrupted)
	})
	if err != nil {
		t.Fatalf("corrupt stored payload: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{Limit: 1, MinScore: 0.9})
	if err != nil {
		t.Fatalf("approximate search with corrupted payload: %v", err)
	}
	if len(results) != 1 || results[0].ID != doc.ID {
		t.Fatalf("unexpected approximate results: %#v", results)
	}
	if results[0].Metadata["title"] != "kept" {
		t.Fatalf("expected metadata to survive approximate search, got %#v", results[0].Metadata)
	}

	if _, err := store.SearchExact([]float32{1, 0, 0, 0}, 1); err == nil {
		t.Fatal("expected exact search to fail on corrupted stored vector")
	}
}

func TestSearchExactRejectsOversizedMetadataLengthPayload(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() {
		_ = store.Close()
	})

	if err := store.Put("doc-1", []float32{1, 0, 0, 0}, Metadata{"topic": "go"}); err != nil {
		t.Fatalf("put document: %v", err)
	}

	payload := append([]byte{}, binaryDocumentMagic...)
	payload = appendBinaryUvarint(payload, 1)
	payload = appendBinaryUvarint(payload, math.MaxUint64)

	err := store.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(documentsBucket).Put([]byte("doc-1"), payload)
	})
	if err != nil {
		t.Fatalf("write corrupted payload: %v", err)
	}

	_, err = store.SearchExact([]float32{1, 0, 0, 0}, 1)
	if err == nil {
		t.Fatal("expected exact search to fail on oversized metadata length")
	}
	if !strings.Contains(err.Error(), "truncated metadata key") {
		t.Fatalf("expected truncated metadata key error, got %v", err)
	}
}

func TestGetRejectsOversizedMetadataCountPayload(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() {
		_ = store.Close()
	})

	payload := append([]byte{}, binaryDocumentMagic...)
	payload = appendBinaryUvarint(payload, math.MaxUint64)

	err := store.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(documentsBucket).Put([]byte("doc-1"), payload)
	})
	if err != nil {
		t.Fatalf("write corrupted payload: %v", err)
	}

	_, err = store.Get("doc-1")
	if err == nil {
		t.Fatal("expected get to fail on oversized metadata count")
	}
	if !strings.Contains(err.Error(), "invalid metadata count") {
		t.Fatalf("expected invalid metadata count error, got %v", err)
	}
}

func TestSearchExactRejectsOversizedMetadataCountPayload(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() {
		_ = store.Close()
	})

	payload := append([]byte{}, binaryDocumentMagic...)
	payload = appendBinaryUvarint(payload, math.MaxUint64)

	err := store.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(documentsBucket).Put([]byte("doc-1"), payload)
	})
	if err != nil {
		t.Fatalf("write corrupted payload: %v", err)
	}

	_, err = store.SearchExact([]float32{1, 0, 0, 0}, 1)
	if err == nil {
		t.Fatal("expected exact search to fail on oversized metadata count")
	}
	if !strings.Contains(err.Error(), "invalid metadata count") {
		t.Fatalf("expected invalid metadata count error, got %v", err)
	}
}

func TestGetRejectsOversizedVectorLengthPayload(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() {
		_ = store.Close()
	})

	payload := append([]byte{}, binaryDocumentMagic...)
	payload = appendBinaryUvarint(payload, 0)
	payload = appendBinaryUvarint(payload, math.MaxUint64)

	err := store.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(documentsBucket).Put([]byte("doc-1"), payload)
	})
	if err != nil {
		t.Fatalf("write corrupted payload: %v", err)
	}

	_, err = store.Get("doc-1")
	if err == nil {
		t.Fatal("expected get to fail on oversized vector length")
	}
	if !strings.Contains(err.Error(), "vector dimension mismatch") {
		t.Fatalf("expected vector dimension mismatch error, got %v", err)
	}
}

func TestLegacyJSONPayloadRemainsReadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.vecdb")

	store, err := Open(path, 4)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	legacyPayload, err := json.Marshal(storedDocument{
		Metadata: Metadata{"title": "legacy"},
		Vector:   encodeVector([]float32{1, 0, 0, 0}),
	})
	if err != nil {
		t.Fatalf("marshal legacy payload: %v", err)
	}

	err = store.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.Bucket(documentsBucket).Put([]byte("doc-1"), legacyPayload); err != nil {
			return err
		}
		_, err := incrementUint64Meta(tx.Bucket(metaBucket), generationKey)
		return err
	})
	if err != nil {
		t.Fatalf("write legacy payload: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(path, 4)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() {
		_ = reopened.Close()
	})

	doc, err := reopened.Get("doc-1")
	if err != nil {
		t.Fatalf("get legacy document: %v", err)
	}
	if doc.Metadata["title"] != "legacy" {
		t.Fatalf("unexpected legacy metadata: %#v", doc.Metadata)
	}

	results, err := reopened.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{Limit: 1, MinScore: 0.9})
	if err != nil {
		t.Fatalf("search legacy document: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("unexpected legacy search results: %#v", results)
	}

	filtered, err := reopened.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{
		Limit:  1,
		Filter: Metadata{"title": "legacy"},
	})
	if err != nil {
		t.Fatalf("filtered search legacy document: %v", err)
	}
	if len(filtered) != 1 || filtered[0].ID != "doc-1" {
		t.Fatalf("unexpected legacy filtered results: %#v", filtered)
	}
}

func TestMetadataOnlyUpdateKeepsIndexFresh(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.vecdb")

	store, err := Open(path, 4)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	vector := []float32{1, 0, 0, 0}
	if err := store.Put("doc-1", vector, Metadata{"topic": "go", "title": "old"}); err != nil {
		t.Fatalf("put document: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(path, 4)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}

	if err := reopened.Put("doc-1", vector, Metadata{"topic": "go", "title": "new"}); err != nil {
		t.Fatalf("metadata-only overwrite: %v", err)
	}
	if reopened.indexStale {
		t.Fatal("expected metadata-only overwrite to keep HNSW fresh")
	}
	if reopened.indexDirty {
		t.Fatal("expected metadata-only overwrite to keep sidecar generation current")
	}

	results, err := reopened.FindSimilar(vector, SearchOptions{Limit: 1})
	if err != nil {
		t.Fatalf("search updated metadata: %v", err)
	}
	if len(results) != 1 || results[0].Metadata["title"] != "new" {
		t.Fatalf("unexpected metadata after overwrite: %#v", results)
	}

	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}

	refreshed, err := Open(path, 4)
	if err != nil {
		t.Fatalf("open refreshed store: %v", err)
	}
	t.Cleanup(func() {
		_ = refreshed.Close()
	})

	if refreshed.indexStale {
		t.Fatal("expected reopened metadata-only update index to stay fresh")
	}
	if refreshed.indexDirty {
		t.Fatal("expected reopened metadata-only update sidecar to remain reusable")
	}

	filtered, err := refreshed.FindSimilar(vector, SearchOptions{
		Limit:  1,
		Filter: Metadata{"topic": "go"},
	})
	if err != nil {
		t.Fatalf("filtered search after metadata-only overwrite: %v", err)
	}
	if len(filtered) != 1 || filtered[0].Metadata["title"] != "new" {
		t.Fatalf("unexpected filtered metadata after overwrite: %#v", filtered)
	}
}

func TestSearchAfterDeleteDoesNotReturnRemovedDocument(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() {
		_ = store.Close()
	})

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0, 0}},
		{ID: "doc-2", Vector: []float32{0.9, 0.1, 0, 0}},
	})
	if err != nil {
		t.Fatalf("upsert many: %v", err)
	}

	err = store.Delete("doc-1")
	if err != nil {
		t.Fatalf("delete document: %v", err)
	}
	if !store.indexStale {
		t.Fatal("expected delete to mark index stale")
	}

	results, err := store.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{Limit: 2})
	if err != nil {
		t.Fatalf("search documents: %v", err)
	}

	for _, result := range results {
		if result.ID == "doc-1" {
			t.Fatal("deleted document was returned by HNSW search")
		}
	}
}

func TestUpsertReplacesIndexedVectorAndMetadata(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() {
		_ = store.Close()
	})

	err := store.Put("doc-1", []float32{1, 0, 0, 0}, Metadata{"title": "old"})
	if err != nil {
		t.Fatalf("put original document: %v", err)
	}

	err = store.Put("doc-1", []float32{0, 1, 0, 0}, Metadata{"title": "new"})
	if err != nil {
		t.Fatalf("replace document: %v", err)
	}
	if !store.indexStale {
		t.Fatal("expected overwrite to mark index stale")
	}

	doc, err := store.Get("doc-1")
	if err != nil {
		t.Fatalf("get replaced document: %v", err)
	}
	if doc.Metadata["title"] != "new" {
		t.Fatalf("expected updated metadata, got %#v", doc.Metadata)
	}

	results, err := store.FindSimilar([]float32{0, 1, 0, 0}, SearchOptions{Limit: 1, MinScore: 0.9})
	if err != nil {
		t.Fatalf("search replaced document: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("unexpected results after replace: %#v", results)
	}
	if results[0].Metadata["title"] != "new" {
		t.Fatalf("expected search to return updated metadata, got %#v", results[0].Metadata)
	}

	if err := store.RefreshIndex(); err != nil {
		t.Fatalf("refresh index: %v", err)
	}
	if store.indexStale {
		t.Fatal("expected refresh to clear stale index state")
	}
}

func TestReopenRebuildsHNSWIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.vecdb")

	store, err := Open(path, 4)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	err = store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{0.91, 0.07, 0.01, 0.01}},
		{ID: "doc-2", Vector: []float32{0.85, 0.11, 0.03, 0.01}},
	})
	if err != nil {
		t.Fatalf("upsert many: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if _, err := os.Stat(path + ".hnsw"); err != nil {
		t.Fatalf("expected sidecar index file: %v", err)
	}

	reopened, err := Open(path, 4)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() {
		_ = reopened.Close()
	})

	results, err := reopened.FindSimilar([]float32{0.88, 0.10, 0.01, 0.01}, SearchOptions{Limit: 1})
	if err != nil {
		t.Fatalf("search reopened store: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-2" {
		t.Fatalf("unexpected results after reopen: %#v", results)
	}
	if reopened.indexDirty {
		t.Fatal("expected persisted sidecar index to be reused on reopen")
	}
	if reopened.indexStale {
		t.Fatal("expected reopened index to be fresh")
	}
}

func TestClosePersistsIndexAfterDestructiveWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.vecdb")

	store, err := Open(path, 4)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	err = store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0, 0}},
		{ID: "doc-2", Vector: []float32{0.9, 0.1, 0, 0}},
	})
	if err != nil {
		t.Fatalf("seed store: %v", err)
	}

	err = store.Delete("doc-1")
	if err != nil {
		t.Fatalf("delete document: %v", err)
	}
	if !store.indexStale {
		t.Fatal("expected destructive write to leave index stale before close")
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(path, 4)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() {
		_ = reopened.Close()
	})

	if reopened.indexDirty {
		t.Fatal("expected sidecar index to be saved on close")
	}
	if reopened.indexStale {
		t.Fatal("expected reopened index to be fresh")
	}
}

func TestOpenWithDifferentDimensionFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.vecdb")

	store, err := Open(path, 3)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	_ = store.Close()

	_, err = Open(path, 4)
	if err == nil {
		t.Fatal("expected dimension mismatch error")
	}
}

func TestOpenWithCorruptedDimensionMetadataFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.vecdb")

	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(metaBucket)
		if err != nil {
			return err
		}
		return meta.Put(dimensionKey, []byte{1, 2, 3})
	})
	if err != nil {
		t.Fatalf("seed corrupted meta: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	_, err = Open(path, 3)
	if err == nil {
		t.Fatal("expected open to fail with corrupted dimension metadata")
	}
	if !strings.Contains(err.Error(), "invalid dimension metadata") {
		t.Fatalf("expected invalid dimension metadata error, got %v", err)
	}
}

func TestOpenWithCorruptedGenerationMetadataFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.vecdb")

	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(documentsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(metadataIndexBucket); err != nil {
			return err
		}
		meta, err := tx.CreateBucketIfNotExists(metaBucket)
		if err != nil {
			return err
		}
		var dim [8]byte
		binary.LittleEndian.PutUint64(dim[:], 3)
		if err := meta.Put(dimensionKey, dim[:]); err != nil {
			return err
		}
		return meta.Put(generationKey, []byte{1, 2, 3})
	})
	if err != nil {
		t.Fatalf("seed corrupted generation meta: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	_, err = Open(path, 3)
	if err == nil {
		t.Fatal("expected open to fail with corrupted generation metadata")
	}
	if !strings.Contains(err.Error(), "invalid meta value") {
		t.Fatalf("expected invalid meta value error, got %v", err)
	}
}

func TestValidationErrors(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() {
		_ = store.Close()
	})

	err := store.Put("", []float32{1, 0, 0}, nil)
	if !errors.Is(err, ErrDocumentIDRequired) {
		t.Fatalf("expected ErrDocumentIDRequired, got %v", err)
	}

	err = store.Put("doc-1", []float32{1, 0}, nil)
	if err == nil {
		t.Fatal("expected dimension validation error")
	}

	_, err = store.FindSimilar([]float32{0, 0, 0}, SearchOptions{Limit: 1})
	if !errors.Is(err, ErrZeroVector) {
		t.Fatalf("expected ErrZeroVector, got %v", err)
	}

	_, err = store.FindSimilar([]float32{1, 0, 0}, SearchOptions{})
	if !errors.Is(err, ErrLimitRequired) {
		t.Fatalf("expected ErrLimitRequired, got %v", err)
	}
}

func TestClosedStoreReturnsErrClosed(t *testing.T) {
	store := openTestStore(t, 3)
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	if err := store.Put("doc-1", []float32{1, 0, 0}, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from Put, got %v", err)
	}
	if _, err := store.Get("doc-1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from Get, got %v", err)
	}
	if err := store.Delete("doc-1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from Delete, got %v", err)
	}
	if _, err := store.Count(); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from Count, got %v", err)
	}
	if _, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from FindSimilar, got %v", err)
	}
}

func TestConcurrentCloseAndSearch(t *testing.T) {
	store := openTestStore(t, 4)

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0, 0}},
		{ID: "doc-2", Vector: []float32{0.9, 0.1, 0, 0}},
	})
	if err != nil {
		t.Fatalf("seed store: %v", err)
	}

	errCh := make(chan error, 1)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}

			_, err := store.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{Limit: 1})
			if err != nil && !errors.Is(err, ErrClosed) {
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}()

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	close(stop)
	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("unexpected error during concurrent close/search: %v", err)
	default:
	}
}

func TestOpenInvalidDimension(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.vecdb")

	_, err := Open(path, 0)
	if !errors.Is(err, ErrInvalidDimension) {
		t.Fatalf("expected ErrInvalidDimension for zero, got %v", err)
	}

	_, err = Open(path, -1)
	if !errors.Is(err, ErrInvalidDimension) {
		t.Fatalf("expected ErrInvalidDimension for negative, got %v", err)
	}
}

func TestOpenEmptyPath(t *testing.T) {
	_, err := Open("", 3)
	if err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestOpenWithOptionsCustomConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.vecdb")
	indexPath := filepath.Join(t.TempDir(), "custom.hnsw")

	store, err := OpenWithOptions(Options{
		Path:      path,
		IndexPath: indexPath,
		Dimension: 4,
		FileMode:  0o644,
		HNSW: HNSWOptions{
			M:        16,
			EfSearch: 32,
			Ml:       0.5,
			Seed:     42,
		},
	})
	if err != nil {
		t.Fatalf("open with options: %v", err)
	}
	defer store.Close()

	if store.Dimension() != 4 {
		t.Fatalf("unexpected dimension: %d", store.Dimension())
	}

	err = store.Put("doc-1", []float32{1, 0, 0, 0}, Metadata{"title": "test"})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{Limit: 1})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("unexpected results: %#v", results)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := os.Stat(indexPath); err != nil {
		t.Fatalf("expected custom index path: %v", err)
	}
}

func TestNilStoreClose(t *testing.T) {
	var store *Store
	if err := store.Close(); err != nil {
		t.Fatalf("expected nil close to succeed, got %v", err)
	}
}

func TestNilStoreDimension(t *testing.T) {
	var store *Store
	if store.Dimension() != 0 {
		t.Fatalf("expected nil dimension to be 0, got %d", store.Dimension())
	}
}

func TestDoubleClose(t *testing.T) {
	store := openTestStore(t, 3)
	if err := store.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestUpsertManyEmptySlice(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	if err := store.UpsertMany(nil); err != nil {
		t.Fatalf("upsert nil: %v", err)
	}
	if err := store.UpsertMany([]Document{}); err != nil {
		t.Fatalf("upsert empty: %v", err)
	}

	count, err := store.Count()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 documents, got %d", count)
	}
}

func TestUpsertManyDuplicateIDsInBatch(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0}, Metadata: Metadata{"v": "first"}},
		{ID: "doc-1", Vector: []float32{0, 1, 0}, Metadata: Metadata{"v": "second"}},
	})
	if err != nil {
		t.Fatalf("upsert with duplicates: %v", err)
	}

	doc, err := store.Get("doc-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if doc.Metadata["v"] != "second" {
		t.Fatalf("expected last-write-wins metadata, got %#v", doc.Metadata)
	}

	count, err := store.Count()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 document, got %d", count)
	}
}

func TestUpsertManyWithVectorChange(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Put("doc-1", []float32{1, 0, 0}, Metadata{"title": "old"})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	err = store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{0, 1, 0}, Metadata: Metadata{"title": "new"}},
		{ID: "doc-2", Vector: []float32{0, 0, 1}, Metadata: Metadata{"title": "fresh"}},
	})
	if err != nil {
		t.Fatalf("upsert many with vector change: %v", err)
	}

	if !store.indexStale {
		t.Fatal("expected vector change to mark index stale")
	}

	results, err := store.FindSimilar([]float32{0, 1, 0}, SearchOptions{Limit: 1, MinScore: 0.9})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("unexpected results: %#v", results)
	}
}

func TestUpsertManyMetadataOnlyUpdate(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0}, Metadata: Metadata{"title": "old"}},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := store.RefreshIndex(); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	err = store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0}, Metadata: Metadata{"title": "new"}},
	})
	if err != nil {
		t.Fatalf("metadata-only upsert many: %v", err)
	}

	if store.indexStale {
		t.Fatal("expected metadata-only batch update to keep index fresh")
	}

	doc, err := store.Get("doc-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if doc.Metadata["title"] != "new" {
		t.Fatalf("expected updated metadata, got %#v", doc.Metadata)
	}
}

func TestUpsertManyValidationErrors(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.UpsertMany([]Document{
		{ID: "", Vector: []float32{1, 0, 0}},
	})
	if !errors.Is(err, ErrDocumentIDRequired) {
		t.Fatalf("expected ErrDocumentIDRequired, got %v", err)
	}

	err = store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0}},
	})
	if err == nil {
		t.Fatal("expected dimension validation error")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Delete("non-existent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestDeleteEmptyID(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Delete("")
	if !errors.Is(err, ErrDocumentIDRequired) {
		t.Fatalf("expected ErrDocumentIDRequired, got %v", err)
	}
}

func TestGetNonExistent(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	_, err := store.Get("non-existent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGetEmptyID(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	_, err := store.Get("")
	if !errors.Is(err, ErrDocumentIDRequired) {
		t.Fatalf("expected ErrDocumentIDRequired, got %v", err)
	}
}

func TestPutWithNilMetadata(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Put("doc-1", []float32{1, 0, 0}, nil)
	if err != nil {
		t.Fatalf("put with nil metadata: %v", err)
	}

	doc, err := store.Get("doc-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if doc.ID != "doc-1" {
		t.Fatalf("unexpected id: %s", doc.ID)
	}
	if len(doc.Metadata) != 0 {
		t.Fatalf("expected empty metadata, got %#v", doc.Metadata)
	}
}

func TestCountEmptyStore(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	count, err := store.Count()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0, got %d", count)
	}
}

func TestSearchConvenienceMethod(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Put("doc-1", []float32{1, 0, 0}, nil)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	results, err := store.Search([]float32{1, 0, 0}, 1)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("unexpected results: %#v", results)
	}
}

func TestFindSimilarEmptyStore(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 5})
	if err != nil {
		t.Fatalf("search empty: %v", err)
	}
	if results == nil {
		t.Fatal("expected empty slice, got nil")
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestFindSimilarEmptyStoreExact(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 5, Exact: true})
	if err != nil {
		t.Fatalf("exact search empty: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestFindSimilarMinScoreFiltering(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.UpsertMany([]Document{
		{ID: "close", Vector: []float32{0.99, 0.1, 0.01}},
		{ID: "far", Vector: []float32{0, 0, 1}},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 10, MinScore: 0.8})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "close" {
		t.Fatalf("expected only 'close' with high min score, got %#v", results)
	}
}

func TestFindSimilarMinScoreExact(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.UpsertMany([]Document{
		{ID: "close", Vector: []float32{0.99, 0.1, 0.01}},
		{ID: "far", Vector: []float32{0, 0, 1}},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 10, MinScore: 0.8, Exact: true})
	if err != nil {
		t.Fatalf("search exact: %v", err)
	}
	if len(results) != 1 || results[0].ID != "close" {
		t.Fatalf("expected only 'close' with high min score exact, got %#v", results)
	}
}

func TestFindSimilarWithCandidatesOption(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	docs := make([]Document, 20)
	for i := range docs {
		v := []float32{float32(i) + 1, float32(20-i) + 1, 1}
		docs[i] = Document{ID: fmt.Sprintf("doc-%02d", i), Vector: v}
	}
	if err := store.UpsertMany(docs); err != nil {
		t.Fatalf("seed: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{
		Limit:      3,
		Candidates: 5,
	})
	if err != nil {
		t.Fatalf("search with candidates: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results with custom candidate count")
	}
	if len(results) > 3 {
		t.Fatalf("expected at most 3 results, got %d", len(results))
	}
}

func TestFindSimilarStaleIndexFallback(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0}},
		{ID: "doc-2", Vector: []float32{0, 1, 0}},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	err = store.Put("doc-1", []float32{0, 0, 1}, nil)
	if err != nil {
		t.Fatalf("overwrite vector: %v", err)
	}
	if !store.indexStale {
		t.Fatal("expected index to be stale after vector overwrite")
	}

	results, err := store.FindSimilar([]float32{0, 0, 1}, SearchOptions{Limit: 1, MinScore: 0.9})
	if err != nil {
		t.Fatalf("search with stale index: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("expected fallback exact search to find updated doc, got %#v", results)
	}
}

func TestFindSimilarFilterNoMatch(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Put("doc-1", []float32{1, 0, 0}, Metadata{"topic": "go"})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{
		Limit:  1,
		Filter: Metadata{"topic": "rust"},
	})
	if err != nil {
		t.Fatalf("filtered search: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected no results for non-matching filter, got %#v", results)
	}
}

func TestFindSimilarFilterMultipleKeys(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0}, Metadata: Metadata{"lang": "go", "level": "intro"}},
		{ID: "doc-2", Vector: []float32{0.9, 0.1, 0}, Metadata: Metadata{"lang": "go", "level": "advanced"}},
		{ID: "doc-3", Vector: []float32{0, 1, 0}, Metadata: Metadata{"lang": "rust", "level": "intro"}},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{
		Limit:  5,
		Filter: Metadata{"lang": "go", "level": "intro"},
	})
	if err != nil {
		t.Fatalf("multi-key filter: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("expected only doc-1, got %#v", results)
	}
}

func TestRefreshIndexOnFreshStore(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Put("doc-1", []float32{1, 0, 0}, nil)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	if err := store.RefreshIndex(); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 1})
	if err != nil {
		t.Fatalf("search after refresh: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("unexpected results after refresh: %#v", results)
	}
}

func TestRefreshIndexClearsStaleState(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0}},
		{ID: "doc-2", Vector: []float32{0, 1, 0}},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := store.Delete("doc-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !store.indexStale {
		t.Fatal("expected stale after delete")
	}

	if err := store.RefreshIndex(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if store.indexStale {
		t.Fatal("expected fresh after refresh")
	}

	results, err := store.FindSimilar([]float32{0, 1, 0}, SearchOptions{Limit: 1})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-2" {
		t.Fatalf("unexpected results: %#v", results)
	}
}

func TestClosedStoreRefreshIndex(t *testing.T) {
	store := openTestStore(t, 3)
	_ = store.Close()

	if err := store.RefreshIndex(); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from RefreshIndex, got %v", err)
	}
}

func TestClosedStoreUpsertMany(t *testing.T) {
	store := openTestStore(t, 3)
	_ = store.Close()

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0}},
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from UpsertMany, got %v", err)
	}
}

func TestClosedStoreUpsert(t *testing.T) {
	store := openTestStore(t, 3)
	_ = store.Close()

	err := store.Upsert(Document{ID: "doc-1", Vector: []float32{1, 0, 0}})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from Upsert, got %v", err)
	}
}

func TestClosedStoreSearch(t *testing.T) {
	store := openTestStore(t, 3)
	_ = store.Close()

	_, err := store.Search([]float32{1, 0, 0}, 1)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from Search, got %v", err)
	}
}

func TestClosedStoreSearchExact(t *testing.T) {
	store := openTestStore(t, 3)
	_ = store.Close()

	_, err := store.SearchExact([]float32{1, 0, 0}, 1)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from SearchExact, got %v", err)
	}
}

func TestReopenWithMissingSidecar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.vecdb")

	store, err := Open(path, 3)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	err = store.Put("doc-1", []float32{1, 0, 0}, Metadata{"title": "test"})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_ = os.Remove(path + ".hnsw")

	reopened, err := Open(path, 3)
	if err != nil {
		t.Fatalf("reopen without sidecar: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	results, err := reopened.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 1})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("unexpected results after sidecar removal: %#v", results)
	}
}

func TestReopenWithCorruptedSidecar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.vecdb")

	store, err := Open(path, 3)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	err = store.Put("doc-1", []float32{1, 0, 0}, Metadata{"title": "test"})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := os.WriteFile(path+".hnsw", []byte("corrupted data"), 0o600); err != nil {
		t.Fatalf("write corrupted sidecar: %v", err)
	}

	reopened, err := Open(path, 3)
	if err != nil {
		t.Fatalf("reopen with corrupted sidecar: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	results, err := reopened.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 1})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("unexpected results after sidecar corruption: %#v", results)
	}
}

func TestReopenWithStaleSidecar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.vecdb")

	store, err := Open(path, 3)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	err = store.Put("doc-1", []float32{1, 0, 0}, nil)
	if err != nil {
		t.Fatalf("put first: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	store2, err := Open(path, 3)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	err = store2.Put("doc-2", []float32{0, 1, 0}, nil)
	if err != nil {
		t.Fatalf("put second: %v", err)
	}
	if err := store2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Desync the index generation after Close so the sidecar appears stale.
	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("open db to desync: %v", err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		return putUint64Meta(tx.Bucket(metaBucket), indexGenerationKey, 0)
	})
	if err != nil {
		t.Fatalf("desync index generation: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := Open(path, 3)
	if err != nil {
		t.Fatalf("reopen with stale sidecar: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	count, err := reopened.Count()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 documents, got %d", count)
	}

	results, err := reopened.FindSimilar([]float32{0, 1, 0}, SearchOptions{Limit: 1, MinScore: 0.9})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-2" {
		t.Fatalf("expected doc-2 from rebuilt index, got %#v", results)
	}

	if !reopened.indexDirty {
		t.Fatal("expected rebuilt index to be dirty")
	}
}

func TestMetadataIndexSyncOnOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.vecdb")

	store, err := Open(path, 3)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	err = store.Put("doc-1", []float32{1, 0, 0}, Metadata{"topic": "go"})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// Deliberately desync metadata index generation
	err = store.db.Update(func(tx *bbolt.Tx) error {
		return putUint64Meta(tx.Bucket(metaBucket), metadataIndexGenerationKey, 0)
	})
	if err != nil {
		t.Fatalf("desync generation: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path, 3)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	results, err := reopened.FindSimilar([]float32{1, 0, 0}, SearchOptions{
		Limit:  1,
		Filter: Metadata{"topic": "go"},
	})
	if err != nil {
		t.Fatalf("filtered search after metadata sync: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("expected metadata index to be rebuilt, got %#v", results)
	}
}

func TestConcurrentReads(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() { _ = store.Close() })

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0, 0}, Metadata: Metadata{"topic": "go"}},
		{ID: "doc-2", Vector: []float32{0, 1, 0, 0}, Metadata: Metadata{"topic": "rust"}},
		{ID: "doc-3", Vector: []float32{0, 0, 1, 0}, Metadata: Metadata{"topic": "go"}},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.Get("doc-1"); err != nil {
				errCh <- fmt.Errorf("get: %w", err)
				return
			}
			if _, err := store.Count(); err != nil {
				errCh <- fmt.Errorf("count: %w", err)
				return
			}
			if _, err := store.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{Limit: 2}); err != nil {
				errCh <- fmt.Errorf("search: %w", err)
				return
			}
			if _, err := store.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{Limit: 1, Filter: Metadata{"topic": "go"}}); err != nil {
				errCh <- fmt.Errorf("filtered search: %w", err)
				return
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent read error: %v", err)
	}
}

func TestConcurrentWritesAndReads(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := fmt.Sprintf("doc-%d", n)
			v := []float32{float32(n + 1), float32(5 - n), 1}
			if err := store.Put(id, v, Metadata{"n": fmt.Sprintf("%d", n)}); err != nil {
				errCh <- fmt.Errorf("put %s: %w", id, err)
			}
		}(i)
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = store.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 3})
			_, _ = store.Count()
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent write/read error: %v", err)
	}

	count, err := store.Count()
	if err != nil {
		t.Fatalf("final count: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5 documents, got %d", count)
	}
}

func TestSearchExactOnEmptyStore(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	results, err := store.SearchExact([]float32{1, 0, 0}, 5)
	if err != nil {
		t.Fatalf("exact search empty: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestFindSimilarLimitClamps(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0}},
		{ID: "doc-2", Vector: []float32{0, 1, 0}},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 100})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) > 2 {
		t.Fatalf("expected at most 2 results, got %d", len(results))
	}
}

func TestUpsertDocumentStructPreserved(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	doc := Document{
		ID:       "doc-1",
		Vector:   []float32{0.5, 0.3, 0.2},
		Metadata: Metadata{"key1": "val1", "key2": "val2"},
	}
	if err := store.Upsert(doc); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, err := store.Get("doc-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != doc.ID {
		t.Fatalf("id mismatch: %s vs %s", got.ID, doc.ID)
	}
	if len(got.Vector) != len(doc.Vector) {
		t.Fatalf("vector length mismatch")
	}
	for i := range doc.Vector {
		if got.Vector[i] != doc.Vector[i] {
			t.Fatalf("vector[%d] mismatch: %f vs %f", i, got.Vector[i], doc.Vector[i])
		}
	}
	if got.Metadata["key1"] != "val1" || got.Metadata["key2"] != "val2" {
		t.Fatalf("metadata mismatch: %#v", got.Metadata)
	}
}

func TestSearchResultScoresDescending(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.UpsertMany([]Document{
		{ID: "exact", Vector: []float32{1, 0, 0}},
		{ID: "close", Vector: []float32{0.9, 0.1, 0}},
		{ID: "far", Vector: []float32{0.1, 0.9, 0}},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 3, Exact: true})
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Fatalf("results not sorted by score descending at index %d: %f > %f",
				i, results[i].Score, results[i-1].Score)
		}
	}
}

func TestDeleteThenReinsert(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Put("doc-1", []float32{1, 0, 0}, Metadata{"v": "original"})
	if err != nil {
		t.Fatalf("put original: %v", err)
	}

	if err := store.Delete("doc-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	err = store.Put("doc-1", []float32{0, 1, 0}, Metadata{"v": "reinserted"})
	if err != nil {
		t.Fatalf("put reinserted: %v", err)
	}

	doc, err := store.Get("doc-1")
	if err != nil {
		t.Fatalf("get reinserted: %v", err)
	}
	if doc.Metadata["v"] != "reinserted" {
		t.Fatalf("expected reinserted metadata, got %#v", doc.Metadata)
	}
	if doc.Vector[0] != 0 || doc.Vector[1] != 1 {
		t.Fatalf("expected reinserted vector, got %#v", doc.Vector)
	}
}

func TestFilterAfterDeleteRemovesFromPostingIndex(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.UpsertMany([]Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0}, Metadata: Metadata{"topic": "go"}},
		{ID: "doc-2", Vector: []float32{0, 1, 0}, Metadata: Metadata{"topic": "go"}},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := store.Delete("doc-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{
		Limit:  5,
		Filter: Metadata{"topic": "go"},
	})
	if err != nil {
		t.Fatalf("filtered search after delete: %v", err)
	}
	for _, r := range results {
		if r.ID == "doc-1" {
			t.Fatal("deleted document appeared in filtered results")
		}
	}
	if len(results) != 1 || results[0].ID != "doc-2" {
		t.Fatalf("unexpected filtered results: %#v", results)
	}
}

func TestLargeMetadata(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	longValue := ""
	for i := 0; i < 1000; i++ {
		longValue += "x"
	}

	err := store.Put("doc-1", []float32{1, 0, 0}, Metadata{
		"long": longValue,
		"key":  "short",
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	doc, err := store.Get("doc-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if doc.Metadata["long"] != longValue {
		t.Fatalf("long metadata value truncated or corrupted")
	}
	if doc.Metadata["key"] != "short" {
		t.Fatalf("short metadata lost")
	}

	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{
		Limit:  1,
		Filter: Metadata{"long": longValue},
	})
	if err != nil {
		t.Fatalf("filter with long value: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("unexpected results with long metadata filter: %#v", results)
	}
}

func TestEmptyMetadataFilter(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Put("doc-1", []float32{1, 0, 0}, nil)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{
		Limit:  1,
		Filter: Metadata{},
	})
	if err != nil {
		t.Fatalf("search with empty filter: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("expected result with empty filter, got %#v", results)
	}
}

func TestPutNaNVector(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Put("doc-1", []float32{math.Float32frombits(0x7FC00000), 0, 0}, nil)
	if !errors.Is(err, ErrInvalidVector) {
		t.Fatalf("expected ErrInvalidVector for NaN, got %v", err)
	}
}

func TestPutInfVector(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Put("doc-1", []float32{float32(math.Inf(1)), 0, 0}, nil)
	if !errors.Is(err, ErrInvalidVector) {
		t.Fatalf("expected ErrInvalidVector for +Inf, got %v", err)
	}

	err = store.Put("doc-1", []float32{float32(math.Inf(-1)), 0, 0}, nil)
	if !errors.Is(err, ErrInvalidVector) {
		t.Fatalf("expected ErrInvalidVector for -Inf, got %v", err)
	}
}

func TestSearchNaNQueryVector(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Put("doc-1", []float32{1, 0, 0}, nil)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	_, err = store.FindSimilar([]float32{math.Float32frombits(0x7FC00000), 0, 0}, SearchOptions{Limit: 1})
	if !errors.Is(err, ErrInvalidVector) {
		t.Fatalf("expected ErrInvalidVector for NaN query, got %v", err)
	}
}

func TestUpsertManyNaNVector(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.UpsertMany([]Document{
		{ID: "ok", Vector: []float32{1, 0, 0}},
		{ID: "bad", Vector: []float32{float32(math.Inf(1)), 0, 0}},
	})
	if !errors.Is(err, ErrInvalidVector) {
		t.Fatalf("expected ErrInvalidVector, got %v", err)
	}

	// The good doc should NOT have been persisted since the batch failed before tx.
	count, _ := store.Count()
	if count != 0 {
		t.Fatalf("expected 0 documents after failed batch, got %d", count)
	}
}

func TestPutNegativeZeroVector(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	// Negative zero is a valid float32; should be accepted.
	negZero := math.Float32frombits(0x80000000)
	err := store.Put("doc-1", []float32{negZero, 1, 0}, nil)
	if err != nil {
		t.Fatalf("put with negative zero: %v", err)
	}
}

func TestPutDenormalizedVector(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	// Subnormal float32 should be accepted.
	subnormal := math.Float32frombits(1) // smallest positive subnormal
	err := store.Put("doc-1", []float32{subnormal, 1, 0}, nil)
	if err != nil {
		t.Fatalf("put with subnormal: %v", err)
	}
}

func TestReopenPersistsAndLoadsCorrectly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.vecdb")

	store, err := Open(path, 4)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	docs := []Document{
		{ID: "doc-1", Vector: []float32{1, 0, 0, 0}, Metadata: Metadata{"topic": "go", "level": "intro"}},
		{ID: "doc-2", Vector: []float32{0, 1, 0, 0}, Metadata: Metadata{"topic": "rust", "level": "advanced"}},
		{ID: "doc-3", Vector: []float32{0, 0, 1, 0}, Metadata: Metadata{"topic": "go", "level": "advanced"}},
	}
	if err := store.UpsertMany(docs); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path, 4)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	// Verify all docs survived.
	count, err := reopened.Count()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3, got %d", count)
	}

	// Verify approximate search works with loaded sidecar.
	results, err := reopened.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{Limit: 1})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("unexpected: %#v", results)
	}

	// Verify filtered search works after metadata index sync.
	filtered, err := reopened.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{
		Limit:  3,
		Filter: Metadata{"topic": "go"},
	})
	if err != nil {
		t.Fatalf("filtered: %v", err)
	}
	if len(filtered) != 2 {
		t.Fatalf("expected 2 go docs, got %d", len(filtered))
	}
}

func TestMultipleDeletesAndReinserts(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	for cycle := 0; cycle < 3; cycle++ {
		err := store.Put("doc-1", []float32{float32(cycle + 1), 0, 0}, Metadata{"cycle": fmt.Sprintf("%d", cycle)})
		if err != nil {
			t.Fatalf("put cycle %d: %v", cycle, err)
		}

		doc, err := store.Get("doc-1")
		if err != nil {
			t.Fatalf("get cycle %d: %v", cycle, err)
		}
		if doc.Metadata["cycle"] != fmt.Sprintf("%d", cycle) {
			t.Fatalf("cycle %d: expected metadata cycle=%d, got %s", cycle, cycle, doc.Metadata["cycle"])
		}

		if err := store.Delete("doc-1"); err != nil {
			t.Fatalf("delete cycle %d: %v", cycle, err)
		}
	}

	count, _ := store.Count()
	if count != 0 {
		t.Fatalf("expected 0 after cycles, got %d", count)
	}
}

func TestUpsertManyLargeBatch(t *testing.T) {
	store := openTestStore(t, 8)
	t.Cleanup(func() { _ = store.Close() })

	const n = 500
	docs := make([]Document, n)
	for i := range docs {
		v := make([]float32, 8)
		v[i%8] = float32(i + 1)
		v[(i+1)%8] = 0.5
		docs[i] = Document{
			ID:       fmt.Sprintf("doc-%04d", i),
			Vector:   v,
			Metadata: Metadata{"group": fmt.Sprintf("g-%d", i%10)},
		}
	}

	if err := store.UpsertMany(docs); err != nil {
		t.Fatalf("large batch upsert: %v", err)
	}

	count, err := store.Count()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != n {
		t.Fatalf("expected %d, got %d", n, count)
	}

	// Exact search to verify correctness.
	results, err := store.FindSimilar(docs[0].Vector, SearchOptions{Limit: 5, Exact: true})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results from large store")
	}
	if results[0].ID != "doc-0000" {
		t.Fatalf("expected doc-0000 first, got %s", results[0].ID)
	}

	// Also verify approximate search returns something reasonable.
	approx, err := store.FindSimilar(docs[0].Vector, SearchOptions{Limit: 5})
	if err != nil {
		t.Fatalf("approx search: %v", err)
	}
	if len(approx) == 0 {
		t.Fatal("expected approximate results from large store")
	}

	// Filtered search.
	filtered, err := store.FindSimilar(docs[0].Vector, SearchOptions{
		Limit:  5,
		Filter: Metadata{"group": "g-0"},
	})
	if err != nil {
		t.Fatalf("filtered: %v", err)
	}
	for _, r := range filtered {
		doc, _ := store.Get(r.ID)
		if doc.Metadata["group"] != "g-0" {
			t.Fatalf("filter leak: %s has group=%s", r.ID, doc.Metadata["group"])
		}
	}
}

func TestExactAndApproximateResultsConsistent(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() { _ = store.Close() })

	docs := []Document{
		{ID: "a", Vector: []float32{1, 0, 0, 0}},
		{ID: "b", Vector: []float32{0.9, 0.1, 0, 0}},
		{ID: "c", Vector: []float32{0.5, 0.5, 0, 0}},
		{ID: "d", Vector: []float32{0, 1, 0, 0}},
		{ID: "e", Vector: []float32{0, 0, 1, 0}},
	}
	if err := store.UpsertMany(docs); err != nil {
		t.Fatalf("seed: %v", err)
	}

	query := []float32{1, 0, 0, 0}
	exact, err := store.FindSimilar(query, SearchOptions{Limit: 3, Exact: true})
	if err != nil {
		t.Fatalf("exact: %v", err)
	}
	approx, err := store.FindSimilar(query, SearchOptions{Limit: 3})
	if err != nil {
		t.Fatalf("approx: %v", err)
	}

	if len(exact) != len(approx) {
		t.Fatalf("result count mismatch: exact=%d approx=%d", len(exact), len(approx))
	}

	// For a small dataset, approximate should return the same top results.
	for i := range exact {
		if exact[i].ID != approx[i].ID {
			t.Fatalf("result[%d] mismatch: exact=%s approx=%s", i, exact[i].ID, approx[i].ID)
		}
	}
}

func TestFilterWithMetadataUpdate(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Put("doc-1", []float32{1, 0, 0}, Metadata{"topic": "go"})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// Update metadata only (same vector).
	err = store.Put("doc-1", []float32{1, 0, 0}, Metadata{"topic": "rust"})
	if err != nil {
		t.Fatalf("update metadata: %v", err)
	}

	// Old filter should find nothing.
	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{
		Limit:  1,
		Filter: Metadata{"topic": "go"},
	})
	if err != nil {
		t.Fatalf("old filter: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results for old metadata, got %#v", results)
	}

	// New filter should find the doc.
	results, err = store.FindSimilar([]float32{1, 0, 0}, SearchOptions{
		Limit:  1,
		Filter: Metadata{"topic": "rust"},
	})
	if err != nil {
		t.Fatalf("new filter: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("expected doc-1, got %#v", results)
	}
}

func TestSearchScoreAccuracy(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Put("doc-1", []float32{1, 0, 0}, nil)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// Query with the identical vector: cosine similarity should be 1.0.
	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 1, Exact: true})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 {
		t.Fatal("expected 1 result")
	}
	if math.Abs(results[0].Score-1.0) > 1e-6 {
		t.Fatalf("expected score ~1.0, got %f", results[0].Score)
	}

	// Orthogonal vector: cosine similarity should be 0.0.
	err = store.Put("doc-2", []float32{0, 1, 0}, nil)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	results, err = store.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 2, Exact: true})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	for _, r := range results {
		if r.ID == "doc-2" && math.Abs(r.Score) > 1e-6 {
			t.Fatalf("expected orthogonal score ~0.0, got %f", r.Score)
		}
	}
}

func TestOpenWithOptionsBboltFileMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mode.vecdb")
	store, err := OpenWithOptions(Options{
		Path:      path,
		Dimension: 3,
		FileMode:  0o666,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = store.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// On unix, file should have at least rw-rw-rw- (umask may reduce).
	if info.Mode().Perm()&0o600 == 0 {
		t.Fatalf("unexpected permissions: %v", info.Mode().Perm())
	}
}

func TestCloseFlushesIndexAndReopensClean(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flush.vecdb")

	store, err := Open(path, 3)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Add many docs to exercise the HNSW index.
	docs := make([]Document, 50)
	for i := range docs {
		docs[i] = Document{
			ID:     fmt.Sprintf("d-%02d", i),
			Vector: []float32{float32(i + 1), float32(50 - i), 1},
		}
	}
	if err := store.UpsertMany(docs); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// Delete some to force stale + rebuild on close.
	for i := 0; i < 10; i++ {
		if err := store.Delete(fmt.Sprintf("d-%02d", i)); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: should load sidecar cleanly.
	reopened, err := Open(path, 3)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	if reopened.indexDirty {
		t.Fatal("expected clean index after close+reopen")
	}
	if reopened.indexStale {
		t.Fatal("expected fresh index after close+reopen")
	}

	count, _ := reopened.Count()
	if count != 40 {
		t.Fatalf("expected 40, got %d", count)
	}

	results, err := reopened.FindSimilar(docs[49].Vector, SearchOptions{Limit: 1})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 {
		t.Fatal("expected 1 result")
	}
}

func TestCloseReturnsPersistErrorAndDataRemainsRecoverable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist-error.vecdb")
	indexDir := t.TempDir()

	store, err := OpenWithOptions(Options{
		Path:      path,
		IndexPath: indexDir,
		Dimension: 3,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	if err := store.Put("doc-1", []float32{1, 0, 0}, Metadata{"topic": "go"}); err != nil {
		t.Fatalf("put document: %v", err)
	}

	err = store.Close()
	if err == nil {
		t.Fatal("expected close to fail when index path is a directory")
	}
	if !strings.Contains(err.Error(), "replace index file") {
		t.Fatalf("expected replace index file error, got %v", err)
	}

	reopened, err := Open(path, 3)
	if err != nil {
		t.Fatalf("reopen after persist error: %v", err)
	}
	t.Cleanup(func() {
		_ = reopened.Close()
	})

	doc, err := reopened.Get("doc-1")
	if err != nil {
		t.Fatalf("get reopened document: %v", err)
	}
	if doc.Metadata["topic"] != "go" {
		t.Fatalf("unexpected metadata after reopen: %#v", doc.Metadata)
	}

	results, err := reopened.FindSimilar([]float32{1, 0, 0}, SearchOptions{Limit: 1, Exact: true})
	if err != nil {
		t.Fatalf("search reopened store: %v", err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("unexpected results after reopen: %#v", results)
	}
}

func TestConcurrentMixedOperations(t *testing.T) {
	store := openTestStore(t, 4)
	t.Cleanup(func() { _ = store.Close() })

	// Seed
	for i := 0; i < 10; i++ {
		v := []float32{float32(i + 1), float32(10 - i), 1, 0}
		if err := store.Put(fmt.Sprintf("seed-%d", i), v, Metadata{"n": fmt.Sprintf("%d", i)}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	// Writers: Put new docs
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := fmt.Sprintf("w-%d", n)
			v := []float32{float32(n + 1), float32(5 - n), 1, 0.5}
			if err := store.Put(id, v, Metadata{"w": fmt.Sprintf("%d", n)}); err != nil {
				errCh <- fmt.Errorf("put %s: %w", id, err)
			}
		}(i)
	}

	// Readers: FindSimilar (both exact and approximate)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{Limit: 3})
			if err != nil {
				errCh <- fmt.Errorf("approx search: %w", err)
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.FindSimilar([]float32{1, 0, 0, 0}, SearchOptions{Limit: 3, Exact: true})
			if err != nil {
				errCh <- fmt.Errorf("exact search: %w", err)
			}
		}()
	}

	// Batch writers
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			batch := []Document{
				{ID: fmt.Sprintf("b-%d-a", n), Vector: []float32{1, 0, 0, float32(n)}, Metadata: Metadata{"b": "a"}},
				{ID: fmt.Sprintf("b-%d-b", n), Vector: []float32{0, 1, 0, float32(n)}, Metadata: Metadata{"b": "b"}},
			}
			if err := store.UpsertMany(batch); err != nil {
				errCh <- fmt.Errorf("batch %d: %w", n, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent error: %v", err)
	}
}

func TestEmptyStringMetadataKeyValue(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	err := store.Put("doc-1", []float32{1, 0, 0}, Metadata{"": "empty-key", "empty-val": ""})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	doc, err := store.Get("doc-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if doc.Metadata[""] != "empty-key" {
		t.Fatalf("expected empty key metadata, got %#v", doc.Metadata)
	}
	if doc.Metadata["empty-val"] != "" {
		t.Fatalf("expected empty value metadata, got %#v", doc.Metadata)
	}

	// Filter by empty key.
	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{
		Limit:  1,
		Filter: Metadata{"": "empty-key"},
	})
	if err != nil {
		t.Fatalf("filter empty key: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result for empty key filter, got %d", len(results))
	}

	// Filter by empty value.
	results, err = store.FindSimilar([]float32{1, 0, 0}, SearchOptions{
		Limit:  1,
		Filter: Metadata{"empty-val": ""},
	})
	if err != nil {
		t.Fatalf("filter empty val: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result for empty value filter, got %d", len(results))
	}
}

func TestSpecialCharactersInIDAndMetadata(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	specialID := "doc/with spaces&special=chars中文"
	specialMeta := Metadata{
		"key with spaces": "value/with/slashes",
		"中文键":             "中文值",
		"emoji🎉":          "value🎊",
	}

	err := store.Put(specialID, []float32{1, 0, 0}, specialMeta)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	doc, err := store.Get(specialID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if doc.ID != specialID {
		t.Fatalf("id mismatch: %q", doc.ID)
	}
	for k, v := range specialMeta {
		if doc.Metadata[k] != v {
			t.Fatalf("metadata[%q] mismatch: got %q, want %q", k, doc.Metadata[k], v)
		}
	}

	// Filtered search with special chars.
	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{
		Limit:  1,
		Filter: Metadata{"中文键": "中文值"},
	})
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(results) != 1 || results[0].ID != specialID {
		t.Fatalf("unexpected filtered results: %#v", results)
	}
}

func TestHighDimensionVector(t *testing.T) {
	dim := 1536 // typical embedding dimension
	store := openTestStore(t, dim)
	t.Cleanup(func() { _ = store.Close() })

	v1 := make([]float32, dim)
	v2 := make([]float32, dim)
	for i := range v1 {
		v1[i] = float32(i) / float32(dim)
		v2[i] = float32(dim-i) / float32(dim)
	}

	if err := store.Put("doc-1", v1, Metadata{"type": "ascending"}); err != nil {
		t.Fatalf("put 1: %v", err)
	}
	if err := store.Put("doc-2", v2, Metadata{"type": "descending"}); err != nil {
		t.Fatalf("put 2: %v", err)
	}

	results, err := store.FindSimilar(v1, SearchOptions{Limit: 2})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 2 || results[0].ID != "doc-1" {
		t.Fatalf("expected doc-1 first, got %#v", results)
	}
}

func TestReopenAfterUpsertManyPreservesGenerations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gen.vecdb")

	store, err := Open(path, 3)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Multiple UpsertMany calls to bump generation multiple times.
	for batch := 0; batch < 3; batch++ {
		docs := make([]Document, 5)
		for i := range docs {
			id := fmt.Sprintf("b%d-d%d", batch, i)
			docs[i] = Document{
				ID:       id,
				Vector:   []float32{float32(batch + 1), float32(i + 1), 1},
				Metadata: Metadata{"batch": fmt.Sprintf("%d", batch)},
			}
		}
		if err := store.UpsertMany(docs); err != nil {
			t.Fatalf("batch %d: %v", batch, err)
		}
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path, 3)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	count, _ := reopened.Count()
	if count != 15 {
		t.Fatalf("expected 15, got %d", count)
	}

	if reopened.indexDirty || reopened.indexStale {
		t.Fatal("expected clean sidecar after close+reopen")
	}

	// Filtered search across batches.
	results, err := reopened.FindSimilar([]float32{2, 3, 1}, SearchOptions{
		Limit:  10,
		Filter: Metadata{"batch": "1"},
	})
	if err != nil {
		t.Fatalf("filtered: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results from batch 1, got %d", len(results))
	}
}

func openTestStore(t *testing.T, dimension int) *Store {
	t.Helper()

	store, err := Open(filepath.Join(t.TempDir(), "test.vecdb"), dimension)
	if err != nil {
		t.Fatalf("open test store: %v", err)
	}
	return store
}

// --------------- Additional coverage tests ---------------

func TestReadBinaryUvarintEdgeCases(t *testing.T) {
	// Empty payload at offset
	_, _, err := readBinaryUvarint([]byte{}, 0)
	if err == nil || err.Error() != "unexpected end of payload" {
		t.Fatalf("expected 'unexpected end of payload', got %v", err)
	}

	// Offset beyond payload
	_, _, err = readBinaryUvarint([]byte{0x01}, 5)
	if err == nil || err.Error() != "unexpected end of payload" {
		t.Fatalf("expected 'unexpected end of payload', got %v", err)
	}

	// Varint overflow: 10 bytes of 0x80 triggers n<0
	overflow := make([]byte, 11)
	for i := range 10 {
		overflow[i] = 0x80
	}
	overflow[10] = 0x01
	_, _, err = readBinaryUvarint(overflow, 0)
	if err == nil || err.Error() != "varint overflow" {
		t.Fatalf("expected 'varint overflow', got %v", err)
	}

	// Truncated varint: single continuation byte with no terminator
	_, _, err = readBinaryUvarint([]byte{0x80}, 0)
	if err == nil || err.Error() != "truncated varint" {
		t.Fatalf("expected 'truncated varint', got %v", err)
	}
}

func TestReadBinaryStringTruncated(t *testing.T) {
	// Build a payload claiming a string of length 100, but only 2 bytes remain.
	var payload []byte
	payload = appendBinaryUvarint(payload, 100) // length = 100
	payload = append(payload, 0x41, 0x42)       // only 2 bytes
	_, _, err := readBinaryString(payload, 0)
	if err == nil || err.Error() != "truncated string" {
		t.Fatalf("expected 'truncated string', got %v", err)
	}
}

func TestSkipBinaryMetadataSectionTruncatedKey(t *testing.T) {
	// Construct payload with magic, 1 metadata entry, then truncated key
	payload := make([]byte, 0, 32)
	payload = append(payload, binaryDocumentMagic...)
	payload = appendBinaryUvarint(payload, 1)   // 1 metadata entry
	payload = appendBinaryUvarint(payload, 100) // key length = 100 (truncated)
	_, err := skipBinaryMetadataSection(payload)
	if err == nil {
		t.Fatal("expected truncated key error")
	}
}

func TestSkipBinaryMetadataSectionTruncatedValue(t *testing.T) {
	// Construct payload with magic, 1 metadata entry, valid key, then truncated value
	payload := make([]byte, 0, 32)
	payload = append(payload, binaryDocumentMagic...)
	payload = appendBinaryUvarint(payload, 1)   // 1 metadata entry
	payload = appendBinaryUvarint(payload, 3)   // key length = 3
	payload = append(payload, "abc"...)         // key
	payload = appendBinaryUvarint(payload, 200) // value length = 200 (truncated)
	_, err := skipBinaryMetadataSection(payload)
	if err == nil {
		t.Fatal("expected truncated value error")
	}
}

func TestSkipBinaryMetadataSectionOversizedCount(t *testing.T) {
	payload := make([]byte, 0, 32)
	payload = append(payload, binaryDocumentMagic...)
	payload = appendBinaryUvarint(payload, 999999) // metadata count way too big
	_, err := skipBinaryMetadataSection(payload)
	if err == nil || err.Error() != "invalid metadata count" {
		t.Fatalf("expected 'invalid metadata count', got %v", err)
	}
}

func TestStoredVectorBytesCorruptedBinaryPayload(t *testing.T) {
	// Dimension mismatch in binary format
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	// Create a valid payload with dimension 3, then try to read it as dimension 5
	doc := Document{ID: "test", Vector: []float32{1, 0, 0}, Metadata: Metadata{}}
	payload, err := store.marshalDocument(doc)
	if err != nil {
		t.Fatal(err)
	}
	_, err = storedVectorBytes(payload, 5) // wrong dimension
	if err == nil {
		t.Fatal("expected dimension mismatch error")
	}
}

func TestStoredVectorEqualPayloadCorrupted(t *testing.T) {
	// Test error path when left payload is corrupted
	_, err := storedVectorEqualPayload([]byte("garbage"), []byte("garbage"), 3)
	if err == nil {
		t.Fatal("expected error for corrupted payload")
	}
}

func TestUnmarshalStoredDocumentCorruptedBinaryVector(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	// Build payload with valid metadata but truncated vector bytes
	payload := make([]byte, 0, 64)
	payload = append(payload, binaryDocumentMagic...)
	payload = appendBinaryUvarint(payload, 0) // 0 metadata entries
	payload = appendBinaryUvarint(payload, 3) // vector dimension = 3
	payload = append(payload, 0, 0, 0, 0)     // only 1 float (need 3)

	_, err := store.unmarshalStoredDocument("corrupt", payload)
	if err == nil {
		t.Fatal("expected error for truncated vector bytes")
	}
}

func TestUnmarshalStoredDocumentInvalidJSON(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	_, err := store.unmarshalStoredDocument("bad", []byte(`{invalid json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestUnmarshalStoredMetadataInvalidJSON(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	_, err := store.unmarshalStoredMetadata("bad", []byte(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON metadata")
	}
}

func TestUnmarshalStoredMetadataCorruptedBinary(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	// Valid magic but truncated metadata
	payload := make([]byte, 0, 16)
	payload = append(payload, binaryDocumentMagic...)
	payload = appendBinaryUvarint(payload, 1)   // 1 metadata entry
	payload = appendBinaryUvarint(payload, 999) // key length too large
	_, err := store.unmarshalStoredMetadata("corrupt", payload)
	if err == nil {
		t.Fatal("expected error for corrupted binary metadata")
	}
}

func TestDecodeBinaryMetadataCorruptedKeyValue(t *testing.T) {
	// Corrupted key read
	payload := make([]byte, 0, 32)
	payload = append(payload, binaryDocumentMagic...)
	payload = appendBinaryUvarint(payload, 1)   // 1 entry
	payload = appendBinaryUvarint(payload, 100) // key length too large
	_, _, err := decodeBinaryMetadata(payload)
	if err == nil {
		t.Fatal("expected error for truncated metadata key")
	}

	// Corrupted value read
	payload = make([]byte, 0, 32)
	payload = append(payload, binaryDocumentMagic...)
	payload = appendBinaryUvarint(payload, 1)   // 1 entry
	payload = appendBinaryUvarint(payload, 1)   // key length = 1
	payload = append(payload, 'k')              // key
	payload = appendBinaryUvarint(payload, 200) // value length too large
	_, _, err = decodeBinaryMetadata(payload)
	if err == nil {
		t.Fatal("expected error for truncated metadata value")
	}
}

func TestSyncMetadataIndexAlreadySynced(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	// Put a document so metadata index is populated
	if err := store.Put("doc1", []float32{1, 0, 0}, Metadata{"key": "val"}); err != nil {
		t.Fatal(err)
	}

	// Calling syncMetadataIndex when already in sync should be a no-op
	store.mu.Lock()
	err := store.syncMetadataIndex()
	store.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func TestAddToIndexLockedWithNilIndex(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	store.mu.Lock()
	store.index = nil
	store.addToIndexLocked(Document{
		ID: "test", Vector: []float32{1, 0, 0},
	})
	hasIndex := store.index != nil
	store.mu.Unlock()

	if !hasIndex {
		t.Fatal("expected index to be created from nil")
	}
}

func TestStoredVectorBytesJSONFormatWrongDimension(t *testing.T) {
	// JSON format with wrong vector length
	jsonPayload := []byte(`{"vector":"AAAAAA==","metadata":{}}`) // 4 bytes = 1 float
	_, err := storedVectorBytes(jsonPayload, 3)                  // expecting 3 floats
	if err == nil {
		t.Fatal("expected dimension error for JSON format")
	}
}

func TestFindSimilarFilterEmptyPostingIntersection(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	if err := store.Put("d1", []float32{1, 0, 0}, Metadata{"a": "1"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("d2", []float32{0, 1, 0}, Metadata{"b": "2"}); err != nil {
		t.Fatal(err)
	}

	// Filter with both keys: no document has both, so intersection is empty
	results, err := store.FindSimilar([]float32{1, 0, 0}, SearchOptions{
		Limit:  5,
		Filter: Metadata{"a": "1", "b": "2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestUpsertSameVectorDifferentMetadata(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	if err := store.Put("doc", []float32{1, 0, 0}, Metadata{"v": "1"}); err != nil {
		t.Fatal(err)
	}
	// Same vector, different metadata — metadata-only update
	if err := store.Put("doc", []float32{1, 0, 0}, Metadata{"v": "2"}); err != nil {
		t.Fatal(err)
	}

	doc, err := store.Get("doc")
	if err != nil {
		t.Fatal(err)
	}
	if doc.Metadata["v"] != "2" {
		t.Fatalf("expected metadata 'v'='2', got %q", doc.Metadata["v"])
	}
}

func TestRefreshIndexOnEmptyStore(t *testing.T) {
	store := openTestStore(t, 3)
	t.Cleanup(func() { _ = store.Close() })

	if err := store.RefreshIndex(); err != nil {
		t.Fatal(err)
	}
}
