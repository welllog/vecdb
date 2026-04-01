package vecdb

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

func BenchmarkFindSimilarApproximate(b *testing.B) {
	benchmarkFindSimilar(b, SearchOptions{Limit: 10})
}

func BenchmarkFindSimilarExact(b *testing.B) {
	benchmarkFindSimilar(b, SearchOptions{Limit: 10, Exact: true})
}

func BenchmarkFindSimilarFiltered(b *testing.B) {
	benchmarkFindSimilar(b, SearchOptions{Limit: 10, Filter: Metadata{"group": "g-3"}})
}

func BenchmarkPutMetadataOnlyOverwrite(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.vecdb")
	seedStore, err := Open(path, 64)
	if err != nil {
		b.Fatalf("open benchmark store: %v", err)
	}
	b.Cleanup(func() {
		_ = seedStore.Close()
	})

	vector := randomVector(rand.New(rand.NewSource(7)), 64)
	if err := seedStore.Put("doc-1", vector, Metadata{"group": "g-0"}); err != nil {
		b.Fatalf("seed metadata overwrite benchmark: %v", err)
	}
	if err := seedStore.Close(); err != nil {
		b.Fatalf("close seeded benchmark store: %v", err)
	}

	store, err := Open(path, 64)
	if err != nil {
		b.Fatalf("reopen benchmark store: %v", err)
	}
	b.Cleanup(func() {
		_ = store.Close()
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.Put("doc-1", vector, Metadata{"group": fmt.Sprintf("g-%d", i%10)}); err != nil {
			b.Fatalf("metadata-only overwrite: %v", err)
		}
	}
}

func BenchmarkOpenWithPersistedIndex(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.vecdb")
	store, _ := openBenchmarkStoreAtPath(b, path, 64, 1000, 32)
	if err := store.Close(); err != nil {
		b.Fatalf("close benchmark store: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reopened, err := Open(path, 64)
		if err != nil {
			b.Fatalf("reopen benchmark store: %v", err)
		}
		if err := reopened.Close(); err != nil {
			b.Fatalf("close reopened benchmark store: %v", err)
		}
	}
}

func benchmarkFindSimilar(b *testing.B, options SearchOptions) {
	b.Helper()

	store, queries := openBenchmarkStore(b, 64, 1000, 32)
	b.Cleanup(func() {
		_ = store.Close()
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.FindSimilar(queries[i%len(queries)], options); err != nil {
			b.Fatalf("find similar: %v", err)
		}
	}
}

func openBenchmarkStore(b *testing.B, dimension int, docs int, queryCount int) (*Store, [][]float32) {
	b.Helper()
	return openBenchmarkStoreAtPath(b, filepath.Join(b.TempDir(), "bench.vecdb"), dimension, docs, queryCount)
}

func openBenchmarkStoreAtPath(b *testing.B, path string, dimension int, docs int, queryCount int) (*Store, [][]float32) {
	b.Helper()

	store, err := Open(path, dimension)
	if err != nil {
		b.Fatalf("open benchmark store: %v", err)
	}

	rng := rand.New(rand.NewSource(42))
	seed := make([]Document, 0, docs)
	for i := 0; i < docs; i++ {
		seed = append(seed, Document{
			ID:       fmt.Sprintf("doc-%d", i),
			Vector:   randomVector(rng, dimension),
			Metadata: Metadata{"group": fmt.Sprintf("g-%d", i%10)},
		})
	}
	if err := store.UpsertMany(seed); err != nil {
		b.Fatalf("seed benchmark store: %v", err)
	}

	queries := make([][]float32, 0, queryCount)
	for i := 0; i < queryCount; i++ {
		queries = append(queries, randomVector(rng, dimension))
	}

	return store, queries
}

func randomVector(rng *rand.Rand, dimension int) []float32 {
	vector := make([]float32, dimension)
	for i := range vector {
		vector[i] = rng.Float32()
	}
	return vector
}
