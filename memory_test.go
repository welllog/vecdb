package vecdb

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"
)

func TestMemoryStoreRememberAndGet(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	createdAt := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	err := mem.Remember(Memory{
		ID:        "memory-1",
		Vector:    []float32{1, 0, 0},
		Content:   "User prefers concise answers.",
		Level:     MemoryLevelSession,
		Metadata:  Metadata{"topic": "preferences"},
		CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatalf("remember: %v", err)
	}

	got, err := mem.Get("memory-1")
	if err != nil {
		t.Fatalf("get memory: %v", err)
	}
	if got.Content != "User prefers concise answers." {
		t.Fatalf("unexpected content: %q", got.Content)
	}
	if got.Level != MemoryLevelSession {
		t.Fatalf("unexpected level: %q", got.Level)
	}
	if got.Metadata["topic"] != "preferences" {
		t.Fatalf("unexpected metadata: %#v", got.Metadata)
	}
	if !got.CreatedAt.Equal(createdAt) {
		t.Fatalf("unexpected createdAt: %v", got.CreatedAt)
	}
	if got.ExpiresAt.IsZero() {
		t.Fatal("expected session memory to get default expiry")
	}
	if got.Vector[0] != 1 {
		t.Fatalf("unexpected vector: %#v", got.Vector)
	}

	doc, err := mem.store.Get("memory-1")
	if err != nil {
		t.Fatalf("get raw document: %v", err)
	}
	if _, ok := doc.Metadata[memoryContentKey]; ok {
		t.Fatalf("expected content to be stored outside metadata, got %#v", doc.Metadata)
	}

	var storedContent string
	mem.store.mu.RLock()
	err = mem.store.db.View(func(tx *bbolt.Tx) error {
		storedContent = loadMemoryContent(tx.Bucket(memoryContentsBucket), "memory-1")
		return nil
	})
	mem.store.mu.RUnlock()
	if err != nil {
		t.Fatalf("read content bucket: %v", err)
	}
	if storedContent != "User prefers concise answers." {
		t.Fatalf("unexpected stored content: %q", storedContent)
	}
}

func TestMemoryStoreDefaultLevelIsLongTerm(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	err := mem.Remember(Memory{
		ID:      "memory-1",
		Vector:  []float32{1, 0, 0},
		Content: "Default level should be long term.",
	})
	if err != nil {
		t.Fatalf("remember: %v", err)
	}

	got, err := mem.Get("memory-1")
	if err != nil {
		t.Fatalf("get memory: %v", err)
	}
	if got.Level != MemoryLevelLongTerm {
		t.Fatalf("expected long-term default, got %q", got.Level)
	}
	if !got.ExpiresAt.IsZero() {
		t.Fatalf("expected long-term memory to have no expiry, got %v", got.ExpiresAt)
	}
}

func TestMemoryStoreRememberValidation(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	err := mem.Remember(Memory{
		ID:      "memory-1",
		Vector:  []float32{1, 0, 0},
		Content: "",
	})
	if !errors.Is(err, ErrMemoryContentRequired) {
		t.Fatalf("expected ErrMemoryContentRequired, got %v", err)
	}

	err = mem.Remember(Memory{
		ID:      "memory-2",
		Vector:  []float32{1, 0, 0},
		Content: "bad level",
		Level:   MemoryLevel("weird"),
	})
	if !errors.Is(err, ErrInvalidMemoryLevel) {
		t.Fatalf("expected ErrInvalidMemoryLevel, got %v", err)
	}

	err = mem.Remember(Memory{
		ID:      "memory-3",
		Vector:  []float32{1, 0, 0},
		Content: "reserved key",
		Metadata: Metadata{
			memoryContentKey: "collision",
		},
	})
	if !errors.Is(err, ErrReservedMemoryMetadataKey) {
		t.Fatalf("expected ErrReservedMemoryMetadataKey, got %v", err)
	}
}

func TestOpenMemoryWithOptionsCustomTTL(t *testing.T) {
	mem, err := OpenMemoryWithOptions(MemoryStoreOptions{
		Store: Options{
			Path:      filepath.Join(t.TempDir(), "memory.vecdb"),
			Dimension: 3,
		},
		ShortTermTTL: 2 * time.Hour,
		SessionTTL:   48 * time.Hour,
	})
	if err != nil {
		t.Fatalf("open memory store: %v", err)
	}
	t.Cleanup(func() { _ = mem.Close() })

	createdAt := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	err = mem.Remember(Memory{
		ID:        "memory-1",
		Vector:    []float32{1, 0, 0},
		Content:   "custom ttl",
		Level:     MemoryLevelShortTerm,
		CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatalf("remember: %v", err)
	}

	got, err := mem.Get("memory-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.ExpiresAt.Equal(createdAt.Add(2 * time.Hour)) {
		t.Fatalf("unexpected expiry: %v", got.ExpiresAt)
	}
}

func TestMemoryStoreRecallByLevelAndFilter(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	err := mem.RememberMany([]Memory{
		{
			ID:       "short-1",
			Vector:   []float32{1, 0, 0},
			Content:  "Short-term note",
			Level:    MemoryLevelShortTerm,
			Metadata: Metadata{"topic": "prefs"},
		},
		{
			ID:       "long-1",
			Vector:   []float32{0.95, 0.05, 0},
			Content:  "Long-term profile fact",
			Level:    MemoryLevelLongTerm,
			Metadata: Metadata{"topic": "prefs"},
		},
		{
			ID:       "other",
			Vector:   []float32{0, 1, 0},
			Content:  "Unrelated item",
			Level:    MemoryLevelLongTerm,
			Metadata: Metadata{"topic": "other"},
		},
	})
	if err != nil {
		t.Fatalf("remember many: %v", err)
	}

	results, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{
		Limit:  2,
		Level:  MemoryLevelLongTerm,
		Filter: Metadata{"topic": "prefs"},
	})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(results) != 1 || results[0].ID != "long-1" {
		t.Fatalf("unexpected results: %#v", results)
	}
	if results[0].Content != "Long-term profile fact" {
		t.Fatalf("unexpected content: %#v", results[0])
	}
}

func TestMemoryStoreRecallSkipsExpiredAndCanIncludeExpired(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	now := time.Now().UTC()
	err := mem.RememberMany([]Memory{
		{
			ID:        "expired",
			Vector:    []float32{1, 0, 0},
			Content:   "Old memory",
			Level:     MemoryLevelShortTerm,
			ExpiresAt: now.Add(-time.Hour),
		},
		{
			ID:        "fresh",
			Vector:    []float32{0.99, 0.01, 0},
			Content:   "Fresh memory",
			Level:     MemoryLevelShortTerm,
			ExpiresAt: now.Add(time.Hour),
		},
	})
	if err != nil {
		t.Fatalf("remember many: %v", err)
	}

	results, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{Limit: 2})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(results) != 1 || results[0].ID != "fresh" {
		t.Fatalf("unexpected non-expired results: %#v", results)
	}

	allResults, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{
		Limit:          2,
		IncludeExpired: true,
	})
	if err != nil {
		t.Fatalf("recall include expired: %v", err)
	}
	if len(allResults) != 2 {
		t.Fatalf("expected 2 results including expired, got %#v", allResults)
	}
}

func TestMemoryStorePruneExpired(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	err := mem.RememberMany([]Memory{
		{
			ID:        "expired",
			Vector:    []float32{1, 0, 0},
			Content:   "Old memory",
			ExpiresAt: time.Now().UTC().Add(-time.Hour),
		},
		{
			ID:        "fresh",
			Vector:    []float32{0.9, 0.1, 0},
			Content:   "Fresh memory",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		},
	})
	if err != nil {
		t.Fatalf("remember many: %v", err)
	}

	removed, err := mem.PruneExpired()
	if err != nil {
		t.Fatalf("prune expired: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 pruned memory, got %d", removed)
	}

	_, err = mem.Get("expired")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected expired memory removed, got %v", err)
	}

	_, err = mem.Get("fresh")
	if err != nil {
		t.Fatalf("expected fresh memory preserved, got %v", err)
	}

	var expiredContent string
	mem.store.mu.RLock()
	err = mem.store.db.View(func(tx *bbolt.Tx) error {
		expiredContent = loadMemoryContent(tx.Bucket(memoryContentsBucket), "expired")
		return nil
	})
	mem.store.mu.RUnlock()
	if err != nil {
		t.Fatalf("inspect pruned content: %v", err)
	}
	if expiredContent != "" {
		t.Fatalf("expected pruned content to be removed, got %q", expiredContent)
	}
}

func TestMemoryStoreReopenPersistsMemoryRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.vecdb")

	mem, err := OpenMemory(path, 3)
	if err != nil {
		t.Fatalf("open memory store: %v", err)
	}

	err = mem.Remember(Memory{
		ID:       "memory-1",
		Vector:   []float32{1, 0, 0},
		Content:  "Persisted memory",
		Level:    MemoryLevelLongTerm,
		Metadata: Metadata{"topic": "identity"},
	})
	if err != nil {
		t.Fatalf("remember: %v", err)
	}
	if err := mem.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := OpenMemory(path, 3)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	got, err := reopened.Get("memory-1")
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if got.Content != "Persisted memory" || got.Metadata["topic"] != "identity" {
		t.Fatalf("unexpected reopened memory: %#v", got)
	}
}

func TestMemoryStoreRecallRejectsReservedFilterKey(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	_, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{
		Limit: 1,
		Filter: Metadata{
			memoryLevelKey: string(MemoryLevelLongTerm),
		},
	})
	if !errors.Is(err, ErrReservedMemoryMetadataKey) {
		t.Fatalf("expected ErrReservedMemoryMetadataKey, got %v", err)
	}
}

func TestMemoryStoreRecallValidationOnEmptyStore(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	_, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{})
	if !errors.Is(err, ErrLimitRequired) {
		t.Fatalf("expected ErrLimitRequired, got %v", err)
	}

	_, err = mem.Recall([]float32{0, 0, 0}, RecallOptions{Limit: 1})
	if !errors.Is(err, ErrZeroVector) {
		t.Fatalf("expected ErrZeroVector, got %v", err)
	}
}

func TestMemoryStoreRejectsInvalidMemoryRecord(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	if err := mem.store.Put("raw-doc", []float32{1, 0, 0}, Metadata{"topic": "raw"}); err != nil {
		t.Fatalf("put raw document: %v", err)
	}

	_, err := mem.Get("raw-doc")
	if !errors.Is(err, ErrInvalidMemoryRecord) {
		t.Fatalf("expected ErrInvalidMemoryRecord, got %v", err)
	}

	// Recall should gracefully skip invalid (non-memory) records instead of failing.
	matches, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{Limit: 1})
	if err != nil {
		t.Fatalf("recall should not error on invalid records: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("expected 0 matches (invalid record skipped), got %d", len(matches))
	}
}

func TestMemoryStoreSupportsLegacyInlineContent(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	err := mem.store.Put("legacy", []float32{1, 0, 0}, Metadata{
		memoryContentKey:   "legacy content",
		memoryLevelKey:     string(MemoryLevelLongTerm),
		memoryCreatedAtKey: time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("put legacy memory record: %v", err)
	}

	got, err := mem.Get("legacy")
	if err != nil {
		t.Fatalf("get legacy memory: %v", err)
	}
	if got.Content != "legacy content" {
		t.Fatalf("unexpected legacy content: %#v", got)
	}
}

func TestMemoryStoreForgetRemovesStoredContent(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	if err := mem.Remember(Memory{
		ID:      "memory-1",
		Vector:  []float32{1, 0, 0},
		Content: "forget me",
	}); err != nil {
		t.Fatalf("remember: %v", err)
	}

	if err := mem.Forget("memory-1"); err != nil {
		t.Fatalf("forget: %v", err)
	}

	var storedContent string
	mem.store.mu.RLock()
	err := mem.store.db.View(func(tx *bbolt.Tx) error {
		storedContent = loadMemoryContent(tx.Bucket(memoryContentsBucket), "memory-1")
		return nil
	})
	mem.store.mu.RUnlock()
	if err != nil {
		t.Fatalf("inspect content bucket: %v", err)
	}
	if storedContent != "" {
		t.Fatalf("expected forgotten content removed, got %q", storedContent)
	}
}

func TestNilMemoryStoreReturnsErrClosed(t *testing.T) {
	var mem *MemoryStore

	if err := mem.Remember(Memory{ID: "x", Vector: []float32{1}, Content: "x"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from Remember, got %v", err)
	}
	if _, err := mem.Get("x"); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from Get, got %v", err)
	}
	if _, err := mem.Recall([]float32{1}, RecallOptions{Limit: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from Recall, got %v", err)
	}
	if err := mem.Forget("x"); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from Forget, got %v", err)
	}
	if _, err := mem.Count(); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from Count, got %v", err)
	}
	if err := mem.RefreshIndex(); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed from RefreshIndex, got %v", err)
	}
	if mem.Dimension() != 0 {
		t.Fatalf("expected zero dimension, got %d", mem.Dimension())
	}
	if err := mem.Close(); err != nil {
		t.Fatalf("expected nil Close to succeed, got %v", err)
	}
}

// --------------- Additional coverage tests ---------------

func TestOpenMemoryWithOptionsNegativeTTL(t *testing.T) {
	dir := t.TempDir()

	_, err := OpenMemoryWithOptions(MemoryStoreOptions{
		Store:        Options{Path: filepath.Join(dir, "a.vecdb"), Dimension: 3},
		ShortTermTTL: -1 * time.Hour,
	})
	if err == nil || err.Error() != "vecdb: short-term ttl must be non-negative" {
		t.Fatalf("expected short-term ttl error, got %v", err)
	}

	_, err = OpenMemoryWithOptions(MemoryStoreOptions{
		Store:      Options{Path: filepath.Join(dir, "b.vecdb"), Dimension: 3},
		SessionTTL: -1 * time.Hour,
	})
	if err == nil || err.Error() != "vecdb: session ttl must be non-negative" {
		t.Fatalf("expected session ttl error, got %v", err)
	}
}

func TestOpenMemoryWithOptionsInvalidStore(t *testing.T) {
	_, err := OpenMemoryWithOptions(MemoryStoreOptions{
		Store: Options{Path: "", Dimension: 3},
	})
	if err == nil {
		t.Fatal("expected error for empty path")
	}

	_, err = OpenMemoryWithOptions(MemoryStoreOptions{
		Store: Options{Path: filepath.Join(t.TempDir(), "c.vecdb"), Dimension: 0},
	})
	if err == nil {
		t.Fatal("expected error for zero dimension")
	}
}

func TestOpenMemoryInvalidArgs(t *testing.T) {
	_, err := OpenMemory("", 3)
	if err == nil {
		t.Fatal("expected error for empty path")
	}

	_, err = OpenMemory(filepath.Join(t.TempDir(), "d.vecdb"), 0)
	if err == nil {
		t.Fatal("expected error for zero dimension")
	}
}

func TestRememberManyEmptySlice(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	if err := mem.RememberMany(nil); err != nil {
		t.Fatalf("RememberMany(nil) should not error: %v", err)
	}
	if err := mem.RememberMany([]Memory{}); err != nil {
		t.Fatalf("RememberMany([]) should not error: %v", err)
	}
	count, _ := mem.Count()
	if count != 0 {
		t.Fatalf("expected 0 records, got %d", count)
	}
}

func TestRememberManyValidationError(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	err := mem.RememberMany([]Memory{
		{ID: "good", Vector: []float32{1, 0, 0}, Content: "ok"},
		{ID: "bad", Vector: []float32{1, 0, 0}, Content: ""}, // missing content
	})
	if !errors.Is(err, ErrMemoryContentRequired) {
		t.Fatalf("expected ErrMemoryContentRequired, got %v", err)
	}
	// no records should be stored because validation fails before upsert
	count, _ := mem.Count()
	if count != 0 {
		t.Fatalf("expected 0 records after validation error, got %d", count)
	}
}

func TestRememberManyDuplicateIDsInBatch(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	err := mem.RememberMany([]Memory{
		{ID: "dup", Vector: []float32{1, 0, 0}, Content: "first"},
		{ID: "dup", Vector: []float32{0, 1, 0}, Content: "second"},
	})
	if err != nil {
		t.Fatalf("remember many with dups: %v", err)
	}

	got, err := mem.Get("dup")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Content != "second" {
		t.Fatalf("expected last-writer-wins content 'second', got %q", got.Content)
	}
}

func TestRememberUpdatesExistingRecord(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	// Insert initial record.
	if err := mem.Remember(Memory{
		ID: "up", Vector: []float32{1, 0, 0}, Content: "v1", Level: MemoryLevelLongTerm,
	}); err != nil {
		t.Fatalf("remember v1: %v", err)
	}

	// Metadata-only update (same vector).
	if err := mem.Remember(Memory{
		ID: "up", Vector: []float32{1, 0, 0}, Content: "v2", Level: MemoryLevelSession,
	}); err != nil {
		t.Fatalf("remember v2: %v", err)
	}
	got, _ := mem.Get("up")
	if got.Content != "v2" || got.Level != MemoryLevelSession {
		t.Fatalf("unexpected after metadata update: %+v", got)
	}

	// Vector change update.
	if err := mem.Remember(Memory{
		ID: "up", Vector: []float32{0, 1, 0}, Content: "v3",
	}); err != nil {
		t.Fatalf("remember v3: %v", err)
	}
	got, _ = mem.Get("up")
	if got.Content != "v3" || got.Vector[1] != 1 {
		t.Fatalf("unexpected after vector update: %+v", got)
	}
}

func TestRecallExpandsSearchWhenExpiredRecordsFiltered(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	now := time.Now().UTC()
	// Insert several expired and one fresh record.
	memories := make([]Memory, 0, 6)
	for i := 0; i < 5; i++ {
		memories = append(memories, Memory{
			ID:        fmt.Sprintf("expired-%d", i),
			Vector:    []float32{1, float32(i) * 0.001, 0},
			Content:   "expired",
			ExpiresAt: now.Add(-time.Hour),
		})
	}
	memories = append(memories, Memory{
		ID:        "fresh",
		Vector:    []float32{1, 0.01, 0},
		Content:   "fresh memory",
		ExpiresAt: now.Add(time.Hour),
	})

	if err := mem.RememberMany(memories); err != nil {
		t.Fatalf("remember many: %v", err)
	}

	// Request 1 result with Limit=1; initial fetch may only return expired records,
	// causing the loop to expand.
	results, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{Limit: 1})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(results) != 1 || results[0].ID != "fresh" {
		t.Fatalf("expected to find 'fresh' after expansion, got %v", results)
	}
}

func TestRecallEmptyStore(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	results, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{Limit: 5})
	if err != nil {
		t.Fatalf("recall on empty store: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestRecallWithLevelFilterOnly(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	if err := mem.RememberMany([]Memory{
		{ID: "s1", Vector: []float32{1, 0, 0}, Content: "short", Level: MemoryLevelShortTerm},
		{ID: "l1", Vector: []float32{0.99, 0.01, 0}, Content: "long", Level: MemoryLevelLongTerm},
	}); err != nil {
		t.Fatal(err)
	}

	results, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{
		Limit: 5,
		Level: MemoryLevelShortTerm,
	})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(results) != 1 || results[0].ID != "s1" {
		t.Fatalf("expected only short-term result, got %v", results)
	}
}

func TestRecallWithExactSearch(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	if err := mem.Remember(Memory{
		ID: "m1", Vector: []float32{1, 0, 0}, Content: "test",
	}); err != nil {
		t.Fatal(err)
	}

	results, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{Limit: 1, Exact: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "m1" {
		t.Fatalf("exact recall: %v", results)
	}
}

func TestRecallWithMinScore(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	if err := mem.RememberMany([]Memory{
		{ID: "close", Vector: []float32{1, 0, 0}, Content: "close"},
		{ID: "far", Vector: []float32{0, 0, 1}, Content: "far"},
	}); err != nil {
		t.Fatal(err)
	}

	results, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{
		Limit:    10,
		MinScore: 0.9,
		Exact:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "close" {
		t.Fatalf("expected only 'close' with high MinScore, got %v", results)
	}
}

func TestRecallInvalidVector(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	_, err := mem.Recall([]float32{1, 0}, RecallOptions{Limit: 1}) // wrong dimension
	if err == nil {
		t.Fatal("expected error for wrong dimension")
	}

	_, err = mem.Recall(nil, RecallOptions{Limit: 1})
	if err == nil {
		t.Fatal("expected error for nil vector")
	}
}

func TestRecallInvalidLevel(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	if err := mem.Remember(Memory{
		ID: "m1", Vector: []float32{1, 0, 0}, Content: "x",
	}); err != nil {
		t.Fatal(err)
	}

	_, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{
		Limit: 1,
		Level: MemoryLevel("bad"),
	})
	if !errors.Is(err, ErrInvalidMemoryLevel) {
		t.Fatalf("expected ErrInvalidMemoryLevel, got %v", err)
	}
}

func TestForgetEmptyID(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	if err := mem.Forget(""); !errors.Is(err, ErrDocumentIDRequired) {
		t.Fatalf("expected ErrDocumentIDRequired, got %v", err)
	}
}

func TestForgetNotFound(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	if err := mem.Forget("nonexistent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestPruneExpiredNoExpiredRecords(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	if err := mem.Remember(Memory{
		ID: "m1", Vector: []float32{1, 0, 0}, Content: "forever", Level: MemoryLevelLongTerm,
	}); err != nil {
		t.Fatal(err)
	}

	removed, err := mem.PruneExpired()
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if removed != 0 {
		t.Fatalf("expected 0 pruned, got %d", removed)
	}
}

func TestPruneExpiredOnEmptyStore(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	removed, err := mem.PruneExpired()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatalf("expected 0, got %d", removed)
	}
}

func TestMemoryStoreCountDimensionRefreshIndex(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	if dim := mem.Dimension(); dim != 3 {
		t.Fatalf("expected dimension 3, got %d", dim)
	}

	if err := mem.Remember(Memory{
		ID: "m1", Vector: []float32{1, 0, 0}, Content: "test",
	}); err != nil {
		t.Fatal(err)
	}

	count, err := mem.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}

	if err := mem.RefreshIndex(); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryStoreMethodsAfterClose(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	if err := mem.Close(); err != nil {
		t.Fatal(err)
	}

	if err := mem.Remember(Memory{ID: "x", Vector: []float32{1, 0, 0}, Content: "x"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Remember after close: %v", err)
	}
	if err := mem.RememberMany([]Memory{{ID: "x", Vector: []float32{1, 0, 0}, Content: "x"}}); !errors.Is(err, ErrClosed) {
		t.Fatalf("RememberMany after close: %v", err)
	}
	if _, err := mem.Get("x"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after close: %v", err)
	}
	if _, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{Limit: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Recall after close: %v", err)
	}
	if err := mem.Forget("x"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Forget after close: %v", err)
	}
	if _, err := mem.PruneExpired(); !errors.Is(err, ErrClosed) {
		t.Fatalf("PruneExpired after close: %v", err)
	}
	if _, err := mem.Count(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Count after close: %v", err)
	}
	if err := mem.RefreshIndex(); !errors.Is(err, ErrClosed) {
		t.Fatalf("RefreshIndex after close: %v", err)
	}
}

func TestMemoryStoreDoubleClose(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	if err := mem.Close(); err != nil {
		t.Fatal(err)
	}
	// second close should not panic
	if err := mem.Close(); err != nil {
		t.Fatalf("double close: %v", err)
	}
}

func TestRememberWithCustomExpiresAt(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	customExpiry := time.Date(2030, 6, 15, 0, 0, 0, 0, time.UTC)
	if err := mem.Remember(Memory{
		ID:        "custom",
		Vector:    []float32{1, 0, 0},
		Content:   "custom expiry",
		Level:     MemoryLevelShortTerm,
		ExpiresAt: customExpiry,
	}); err != nil {
		t.Fatal(err)
	}

	got, _ := mem.Get("custom")
	if !got.ExpiresAt.Equal(customExpiry) {
		t.Fatalf("expected custom expiry %v, got %v", customExpiry, got.ExpiresAt)
	}
}

func TestMemoryRecallAllExpiredReturnsEmpty(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	now := time.Now().UTC()
	if err := mem.RememberMany([]Memory{
		{ID: "a", Vector: []float32{1, 0, 0}, Content: "x", ExpiresAt: now.Add(-time.Hour)},
		{ID: "b", Vector: []float32{0, 1, 0}, Content: "y", ExpiresAt: now.Add(-time.Hour)},
	}); err != nil {
		t.Fatal(err)
	}

	results, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("expected empty when all expired, got %d", len(results))
	}
}

func TestRememberManyBatchUpsertAndGet(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	memories := make([]Memory, 20)
	for i := range memories {
		memories[i] = Memory{
			ID:      fmt.Sprintf("m-%d", i),
			Vector:  []float32{float32(i%3 + 1), float32(i%5 + 1), float32(i%7 + 1)},
			Content: fmt.Sprintf("memory content %d", i),
		}
	}

	if err := mem.RememberMany(memories); err != nil {
		t.Fatalf("remember many: %v", err)
	}

	count, _ := mem.Count()
	if count != 20 {
		t.Fatalf("expected 20, got %d", count)
	}

	for _, m := range memories {
		got, err := mem.Get(m.ID)
		if err != nil {
			t.Fatalf("get %s: %v", m.ID, err)
		}
		if got.Content != m.Content {
			t.Fatalf("content mismatch for %s: %q vs %q", m.ID, got.Content, m.Content)
		}
	}
}

func TestRecallLegacyInlineContentFoundViaSearch(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	// Insert via raw store.Put with content in metadata (legacy format).
	if err := mem.store.Put("legacy", []float32{1, 0, 0}, Metadata{
		memoryContentKey:   "inline content",
		memoryLevelKey:     string(MemoryLevelLongTerm),
		memoryCreatedAtKey: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	// Recall should find and decode legacy content.
	results, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Content != "inline content" {
		t.Fatalf("expected 'inline content', got %q", results[0].Content)
	}
}

func TestDecodeMemoryRecordInvalidFields(t *testing.T) {
	// Missing level
	_, err := decodeMemoryRecord("id", Metadata{
		memoryCreatedAtKey: time.Now().Format(time.RFC3339Nano),
	}, "content")
	if !errors.Is(err, ErrInvalidMemoryRecord) {
		t.Fatalf("expected ErrInvalidMemoryRecord for missing level, got %v", err)
	}

	// Invalid level
	_, err = decodeMemoryRecord("id", Metadata{
		memoryLevelKey:     "bogus",
		memoryCreatedAtKey: time.Now().Format(time.RFC3339Nano),
	}, "content")
	if !errors.Is(err, ErrInvalidMemoryLevel) {
		t.Fatalf("expected ErrInvalidMemoryLevel, got %v", err)
	}

	// Missing created_at
	_, err = decodeMemoryRecord("id", Metadata{
		memoryLevelKey: string(MemoryLevelLongTerm),
	}, "content")
	if !errors.Is(err, ErrInvalidMemoryRecord) {
		t.Fatalf("expected ErrInvalidMemoryRecord for missing created_at, got %v", err)
	}

	// Invalid created_at format
	_, err = decodeMemoryRecord("id", Metadata{
		memoryLevelKey:     string(MemoryLevelLongTerm),
		memoryCreatedAtKey: "not-a-date",
	}, "content")
	if !errors.Is(err, ErrInvalidMemoryRecord) {
		t.Fatalf("expected ErrInvalidMemoryRecord for bad created_at, got %v", err)
	}

	// Invalid expires_at format
	_, err = decodeMemoryRecord("id", Metadata{
		memoryLevelKey:     string(MemoryLevelLongTerm),
		memoryCreatedAtKey: time.Now().Format(time.RFC3339Nano),
		memoryExpiresAtKey: "not-a-date",
	}, "content")
	if !errors.Is(err, ErrInvalidMemoryRecord) {
		t.Fatalf("expected ErrInvalidMemoryRecord for bad expires_at, got %v", err)
	}

	// Missing content
	_, err = decodeMemoryRecord("id", Metadata{
		memoryLevelKey:     string(MemoryLevelLongTerm),
		memoryCreatedAtKey: time.Now().Format(time.RFC3339Nano),
	}, "")
	if !errors.Is(err, ErrInvalidMemoryRecord) {
		t.Fatalf("expected ErrInvalidMemoryRecord for missing content, got %v", err)
	}
}

func TestMemoryStoreShortTermDefaultExpiry(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	created := time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC)
	if err := mem.Remember(Memory{
		ID:        "short",
		Vector:    []float32{1, 0, 0},
		Content:   "short term",
		Level:     MemoryLevelShortTerm,
		CreatedAt: created,
	}); err != nil {
		t.Fatal(err)
	}

	got, _ := mem.Get("short")
	expected := created.Add(24 * time.Hour)
	if !got.ExpiresAt.Equal(expected) {
		t.Fatalf("expected expiry %v, got %v", expected, got.ExpiresAt)
	}
}

func TestRecallWithCandidatesOption(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	memories := make([]Memory, 10)
	for i := range memories {
		memories[i] = Memory{
			ID:      fmt.Sprintf("m-%d", i),
			Vector:  []float32{float32(i+1) * 0.1, 1, 0},
			Content: fmt.Sprintf("content %d", i),
		}
	}
	if err := mem.RememberMany(memories); err != nil {
		t.Fatal(err)
	}

	results, err := mem.Recall([]float32{0.5, 1, 0}, RecallOptions{
		Limit:      3,
		Candidates: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least 1 result with candidates option")
	}
}

func TestRememberManyUpdatesExistingRecords(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	// Insert initial.
	if err := mem.Remember(Memory{
		ID: "r1", Vector: []float32{1, 0, 0}, Content: "original",
	}); err != nil {
		t.Fatal(err)
	}

	// Update via RememberMany: same vector (metadata-only) and different vector.
	if err := mem.RememberMany([]Memory{
		{ID: "r1", Vector: []float32{1, 0, 0}, Content: "updated-same-vec"},
		{ID: "r2", Vector: []float32{0, 1, 0}, Content: "new-record"},
	}); err != nil {
		t.Fatal(err)
	}

	got1, _ := mem.Get("r1")
	if got1.Content != "updated-same-vec" {
		t.Fatalf("expected updated content, got %q", got1.Content)
	}
	got2, _ := mem.Get("r2")
	if got2.Content != "new-record" {
		t.Fatalf("expected new record, got %q", got2.Content)
	}
}

func TestPruneExpiredMultipleRecords(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	now := time.Now().UTC()
	if err := mem.RememberMany([]Memory{
		{ID: "e1", Vector: []float32{1, 0, 0}, Content: "x", ExpiresAt: now.Add(-2 * time.Hour)},
		{ID: "e2", Vector: []float32{0, 1, 0}, Content: "y", ExpiresAt: now.Add(-1 * time.Hour)},
		{ID: "ok", Vector: []float32{0, 0, 1}, Content: "z", Level: MemoryLevelLongTerm},
	}); err != nil {
		t.Fatal(err)
	}

	removed, err := mem.PruneExpired()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Fatalf("expected 2 pruned, got %d", removed)
	}

	count, _ := mem.Count()
	if count != 1 {
		t.Fatalf("expected 1 remaining, got %d", count)
	}
}

func TestRecallMatchFields(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	created := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	expires := time.Date(2099, 7, 1, 12, 0, 0, 0, time.UTC)
	if err := mem.Remember(Memory{
		ID:        "full",
		Vector:    []float32{1, 0, 0},
		Content:   "full record",
		Level:     MemoryLevelSession,
		Metadata:  Metadata{"key": "val"},
		CreatedAt: created,
		ExpiresAt: expires,
	}); err != nil {
		t.Fatal(err)
	}

	results, err := mem.Recall([]float32{1, 0, 0}, RecallOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	m := results[0]
	if m.ID != "full" {
		t.Fatalf("unexpected ID: %s", m.ID)
	}
	if m.Content != "full record" {
		t.Fatalf("unexpected content: %s", m.Content)
	}
	if m.Level != MemoryLevelSession {
		t.Fatalf("unexpected level: %s", m.Level)
	}
	if m.Metadata["key"] != "val" {
		t.Fatalf("unexpected metadata: %v", m.Metadata)
	}
	if !m.CreatedAt.Equal(created) {
		t.Fatalf("unexpected created_at: %v", m.CreatedAt)
	}
	if !m.ExpiresAt.Equal(expires) {
		t.Fatalf("unexpected expires_at: %v", m.ExpiresAt)
	}
	if m.Score <= 0 || m.Score > 1 {
		t.Fatalf("unexpected score: %f", m.Score)
	}
}

func TestGetEmptyIDReturnsError(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	_, err := mem.Get("")
	if !errors.Is(err, ErrDocumentIDRequired) {
		t.Fatalf("expected ErrDocumentIDRequired, got %v", err)
	}
}

func TestGetNotFoundReturnsError(t *testing.T) {
	mem := openTestMemoryStore(t, 3)
	t.Cleanup(func() { _ = mem.Close() })

	_, err := mem.Get("nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func openTestMemoryStore(t *testing.T, dimension int) *MemoryStore {
	t.Helper()

	mem, err := OpenMemory(filepath.Join(t.TempDir(), "memory.vecdb"), dimension)
	if err != nil {
		t.Fatalf("open test memory store: %v", err)
	}
	return mem
}
