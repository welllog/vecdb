// Package vecdb provides an embedded vector store for local Go applications.
package vecdb

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/coder/hnsw"
	bbolt "go.etcd.io/bbolt"
)

var documentsBucket = []byte("documents")
var metadataIndexBucket = []byte("metadata_index")
var metaBucket = []byte("meta")
var dimensionKey = []byte("dimension")
var generationKey = []byte("generation")
var indexGenerationKey = []byte("index_generation")
var metadataIndexGenerationKey = []byte("metadata_index_generation")
var binaryDocumentMagic = []byte("VDB1")

var (
	// ErrClosed indicates the store has already been closed.
	ErrClosed = errors.New("vecdb: store is closed")
	// ErrInvalidDimension indicates a non-positive vector dimension.
	ErrInvalidDimension = errors.New("vecdb: dimension must be positive")
	// ErrDocumentIDRequired indicates a document ID was empty.
	ErrDocumentIDRequired = errors.New("vecdb: document id is required")
	// ErrLimitRequired indicates a search limit was zero or negative.
	ErrLimitRequired = errors.New("vecdb: limit must be positive")
	// ErrZeroVector indicates cosine similarity cannot be computed for a zero vector.
	ErrZeroVector = errors.New("vecdb: vector norm must be non-zero")
	// ErrInvalidVector indicates a vector contains NaN or Inf values.
	ErrInvalidVector = errors.New("vecdb: vector contains NaN or Inf")
	// ErrNotFound indicates the requested document does not exist.
	ErrNotFound = errors.New("vecdb: document not found")
)

// Metadata stores exact-match string key/value pairs associated with a document.
type Metadata map[string]string

// HNSWOptions configures the in-memory HNSW index.
type HNSWOptions struct {
	M        int
	Ml       float64
	EfSearch int
	Seed     int64
}

// Options configures how a Store is opened.
type Options struct {
	Path      string
	IndexPath string
	Dimension int
	FileMode  os.FileMode
	HNSW      HNSWOptions
}

// SearchOptions configures similarity search behavior.
type SearchOptions struct {
	Limit      int
	MinScore   float64
	Exact      bool
	Candidates int
	Filter     Metadata
}

// Store is a persistent vector collection backed by bbolt and an optional HNSW sidecar index.
type Store struct {
	db        *bbolt.DB
	dimension int
	indexPath string

	mu         sync.RWMutex
	index      *hnsw.Graph[string]
	hnswConfig HNSWOptions
	generation uint64
	indexSaved uint64
	indexDirty bool
	indexStale bool
}

// Document is a stored vector and its associated metadata.
type Document struct {
	ID       string    `json:"id"`
	Vector   []float32 `json:"vector,omitempty"`
	Metadata Metadata  `json:"metadata,omitempty"`
}

// SearchResult is a similarity match returned by Search, SearchExact, or FindSimilar.
type SearchResult struct {
	ID       string   `json:"id"`
	Score    float64  `json:"score"`
	Metadata Metadata `json:"metadata,omitempty"`
}

type storedDocument struct {
	Metadata Metadata `json:"metadata,omitempty"`
	Vector   []byte   `json:"vector"`
}

// Open opens or creates a store at path using the provided vector dimension.
func Open(path string, dimension int) (*Store, error) {
	return OpenWithOptions(Options{Path: path, Dimension: dimension})
}

// OpenWithOptions opens or creates a store using the provided options.
func OpenWithOptions(options Options) (*Store, error) {
	if options.Dimension <= 0 {
		return nil, ErrInvalidDimension
	}
	if options.Path == "" {
		return nil, errors.New("vecdb: path is required")
	}
	if options.FileMode == 0 {
		options.FileMode = 0o600
	}

	db, err := bbolt.Open(options.Path, options.FileMode, nil)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	var generation uint64
	var indexSaved uint64
	var metadataIndexGeneration uint64

	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(documentsBucket); err != nil {
			return fmt.Errorf("create documents bucket: %w", err)
		}

		if _, err := tx.CreateBucketIfNotExists(metadataIndexBucket); err != nil {
			return fmt.Errorf("create metadata index bucket: %w", err)
		}

		meta, err := tx.CreateBucketIfNotExists(metaBucket)
		if err != nil {
			return fmt.Errorf("create meta bucket: %w", err)
		}

		rawDimension := meta.Get(dimensionKey)
		if rawDimension == nil {
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(options.Dimension))
			if err := meta.Put(dimensionKey, buf); err != nil {
				return err
			}
		} else {
			if len(rawDimension) != 8 {
				return errors.New("invalid dimension metadata")
			}
			storedDimension := int(binary.LittleEndian.Uint64(rawDimension))
			if storedDimension != options.Dimension {
				return fmt.Errorf("database dimension is %d, requested %d", storedDimension, options.Dimension)
			}
		}

		generation, err = ensureUint64Meta(meta, generationKey)
		if err != nil {
			return err
		}

		indexSaved, err = ensureUint64Meta(meta, indexGenerationKey)
		if err != nil {
			return err
		}

		metadataIndexGeneration, err = ensureUint64Meta(meta, metadataIndexGenerationKey)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	store := &Store{
		db:         db,
		dimension:  options.Dimension,
		indexPath:  resolveIndexPath(options),
		hnswConfig: options.HNSW,
		generation: generation,
		indexSaved: indexSaved,
	}

	if generation != metadataIndexGeneration {
		if err := store.syncMetadataIndex(); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	if generation == indexSaved {
		if err := store.loadIndexFromDisk(); err == nil {
			return store, nil
		}
	}

	if err := store.rebuildIndex(); err != nil {
		_ = db.Close()
		return nil, err
	}
	store.indexDirty = generation > 0

	return store, nil
}

// Close flushes pending index state, releases resources, and makes the store unusable.
// It is safe to call Close multiple times.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db == nil {
		return nil
	}

	persistErr := s.flushIndexLocked()
	closeErr := s.db.Close()
	s.index = nil
	s.db = nil
	s.indexDirty = false
	s.indexStale = false
	return errors.Join(persistErr, closeErr)
}

// Dimension returns the store vector dimension.
func (s *Store) Dimension() int {
	if s == nil {
		return 0
	}
	return s.dimension
}

// RefreshIndex rebuilds the in-memory HNSW index from the persisted documents.
func (s *Store) RefreshIndex() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureOpen(); err != nil {
		return err
	}
	if err := s.rebuildIndex(); err != nil {
		return err
	}
	s.indexDirty = true
	return nil
}

// Put inserts or replaces a document using separate ID, vector, and metadata arguments.
func (s *Store) Put(id string, vector []float32, metadata Metadata) error {
	return s.Upsert(Document{ID: id, Vector: vector, Metadata: metadata})
}

// Upsert inserts or replaces a single document.
func (s *Store) Upsert(doc Document) error {
	payload, err := s.marshalDocument(doc)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureOpen(); err != nil {
		return err
	}

	rebuildIndex := false
	addToIndex := false
	indexStillSaved := !s.indexDirty && !s.indexStale
	var generation uint64

	err = s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(documentsBucket)
		metadataIndex := tx.Bucket(metadataIndexBucket)
		meta := tx.Bucket(metaBucket)
		key := []byte(doc.ID)
		existingPayload := bucket.Get(key)
		if existingPayload != nil {
			storedMetadata, err := s.unmarshalStoredMetadata(doc.ID, existingPayload)
			if err != nil {
				return err
			}
			if err := deleteMetadataIndexEntries(metadataIndex, doc.ID, storedMetadata); err != nil {
				return err
			}

			sameVector, err := storedVectorEqualPayload(existingPayload, payload, s.dimension)
			if err != nil {
				return err
			}
			if !sameVector {
				rebuildIndex = true
				indexStillSaved = false
			}
		} else {
			addToIndex = true
			indexStillSaved = false
		}
		if err := bucket.Put(key, payload); err != nil {
			return err
		}
		if err := putMetadataIndexEntries(metadataIndex, doc.ID, doc.Metadata); err != nil {
			return err
		}
		nextGeneration, generationErr := incrementUint64Meta(meta, generationKey)
		if generationErr != nil {
			return generationErr
		}
		if err := putUint64Meta(meta, metadataIndexGenerationKey, nextGeneration); err != nil {
			return err
		}
		if indexStillSaved {
			if err := putUint64Meta(meta, indexGenerationKey, nextGeneration); err != nil {
				return err
			}
		}
		generation = nextGeneration
		return nil
	})
	if err == nil {
		s.generation = generation
		if rebuildIndex || s.indexStale {
			s.indexDirty = true
			s.index = nil
			s.indexStale = true
			return nil
		}
		if addToIndex {
			s.indexDirty = true
			s.addToIndexLocked(doc)
			return nil
		}
		if indexStillSaved {
			s.indexSaved = generation
			s.indexDirty = false
			return nil
		}
		s.indexDirty = true
	}
	return err
}

// UpsertMany inserts or replaces multiple documents in a single batch.
func (s *Store) UpsertMany(docs []Document) error {
	if len(docs) == 0 {
		return nil
	}

	type item struct {
		id      string
		payload []byte
	}

	items := make([]item, 0, len(docs))
	seenIDs := make(map[string]struct{}, len(docs))
	rebuildIndex := false
	docsToAdd := make([]Document, 0, len(docs))
	var generation uint64
	for _, doc := range docs {
		payload, err := s.marshalDocument(doc)
		if err != nil {
			return err
		}
		if _, exists := seenIDs[doc.ID]; exists {
			rebuildIndex = true
		}
		seenIDs[doc.ID] = struct{}{}
		items = append(items, item{id: doc.ID, payload: payload})
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureOpen(); err != nil {
		return err
	}

	indexStillSaved := !s.indexDirty && !s.indexStale
	if rebuildIndex {
		indexStillSaved = false
	}

	var err error
	err = s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(documentsBucket)
		metadataIndex := tx.Bucket(metadataIndexBucket)
		meta := tx.Bucket(metaBucket)
		for i, item := range items {
			key := []byte(item.id)
			existingPayload := bucket.Get(key)
			if existingPayload != nil {
				storedMetadata, err := s.unmarshalStoredMetadata(item.id, existingPayload)
				if err != nil {
					return err
				}
				if err := deleteMetadataIndexEntries(metadataIndex, item.id, storedMetadata); err != nil {
					return err
				}

				sameVector, err := storedVectorEqualPayload(existingPayload, item.payload, s.dimension)
				if err != nil {
					return err
				}
				if !sameVector {
					rebuildIndex = true
					indexStillSaved = false
				}
			} else {
				docsToAdd = append(docsToAdd, docs[i])
				indexStillSaved = false
			}
			if err := bucket.Put(key, item.payload); err != nil {
				return err
			}
			if err := putMetadataIndexEntries(metadataIndex, item.id, docs[i].Metadata); err != nil {
				return err
			}
		}
		nextGeneration, generationErr := incrementUint64Meta(meta, generationKey)
		if generationErr != nil {
			return generationErr
		}
		if err := putUint64Meta(meta, metadataIndexGenerationKey, nextGeneration); err != nil {
			return err
		}
		if indexStillSaved {
			if err := putUint64Meta(meta, indexGenerationKey, nextGeneration); err != nil {
				return err
			}
		}
		generation = nextGeneration
		return nil
	})
	if err == nil {
		s.generation = generation
		if rebuildIndex || s.indexStale {
			s.indexDirty = true
			s.index = nil
			s.indexStale = true
			return nil
		}
		if len(docsToAdd) > 0 {
			s.indexDirty = true
			s.addManyToIndexLocked(docsToAdd)
			return nil
		}
		if indexStillSaved {
			s.indexSaved = generation
			s.indexDirty = false
			return nil
		}
		s.indexDirty = true
	}
	return err
}

// Get returns a document by ID.
func (s *Store) Get(id string) (Document, error) {
	if id == "" {
		return Document{}, ErrDocumentIDRequired
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.ensureOpen(); err != nil {
		return Document{}, err
	}

	var doc Document
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(documentsBucket)
		payload := bucket.Get([]byte(id))
		if payload == nil {
			return ErrNotFound
		}

		stored, err := s.unmarshalStoredDocument(id, payload)
		if err != nil {
			return err
		}

		doc = stored
		return nil
	})
	if err != nil {
		return Document{}, err
	}

	return doc, nil
}

// Delete removes a document by ID.
func (s *Store) Delete(id string) error {
	if id == "" {
		return ErrDocumentIDRequired
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureOpen(); err != nil {
		return err
	}

	var generation uint64
	var err error
	err = s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(documentsBucket)
		metadataIndex := tx.Bucket(metadataIndexBucket)
		meta := tx.Bucket(metaBucket)
		key := []byte(id)
		payload := bucket.Get(key)
		if payload == nil {
			return ErrNotFound
		}
		storedMetadata, err := s.unmarshalStoredMetadata(id, payload)
		if err != nil {
			return err
		}
		if err := deleteMetadataIndexEntries(metadataIndex, id, storedMetadata); err != nil {
			return err
		}
		if err := bucket.Delete(key); err != nil {
			return err
		}
		nextGeneration, generationErr := incrementUint64Meta(meta, generationKey)
		if generationErr != nil {
			return generationErr
		}
		if err := putUint64Meta(meta, metadataIndexGenerationKey, nextGeneration); err != nil {
			return err
		}
		generation = nextGeneration
		return nil
	})
	if err == nil {
		s.generation = generation
		s.indexDirty = true
		s.indexStale = true
		s.index = nil
	}
	return err
}

// Count returns the number of stored documents.
func (s *Store) Count() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.ensureOpen(); err != nil {
		return 0, err
	}

	count := 0
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(documentsBucket)
		count = bucket.Stats().KeyN
		return nil
	})
	if err != nil {
		return 0, err
	}

	return count, nil
}

// Search performs approximate nearest-neighbor search using the default search behavior.
func (s *Store) Search(query []float32, limit int) ([]SearchResult, error) {
	return s.FindSimilar(query, SearchOptions{Limit: limit})
}

// SearchExact performs an exact full-scan cosine similarity search.
func (s *Store) SearchExact(query []float32, limit int) ([]SearchResult, error) {
	return s.FindSimilar(query, SearchOptions{Limit: limit, Exact: true})
}

// FindSimilar performs similarity search with explicit control over exact mode,
// candidate count, minimum score, and metadata filtering.
func (s *Store) FindSimilar(query []float32, options SearchOptions) ([]SearchResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	if err := s.validateVector(query); err != nil {
		return nil, fmt.Errorf("validate query vector: %w", err)
	}
	if options.Limit <= 0 {
		return nil, ErrLimitRequired
	}

	queryNorm := vectorNorm(query)
	if queryNorm == 0 {
		return nil, ErrZeroVector
	}
	if options.Exact || len(options.Filter) > 0 || s.indexStale {
		return s.findSimilarExactLocked(query, queryNorm, options)
	}

	index := s.index
	if index == nil || index.Len() == 0 {
		return []SearchResult{}, nil
	}

	neighbors := index.Search(query, candidateCount(options, index.Len()))

	results := make([]SearchResult, 0, len(neighbors))

	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(documentsBucket)
		for _, neighbor := range neighbors {
			payload := bucket.Get([]byte(neighbor.Key))
			if payload == nil {
				continue
			}

			metadata, err := s.unmarshalStoredMetadata(neighbor.Key, payload)
			if err != nil {
				return err
			}

			score := cosineSimilarity(query, queryNorm, neighbor.Value)
			if score < options.MinScore {
				continue
			}

			results = append(results, SearchResult{
				ID:       neighbor.Key,
				Score:    score,
				Metadata: metadata,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sortSearchResults(results)

	if len(results) > options.Limit {
		results = results[:options.Limit]
	}

	return results, nil
}

func (s *Store) findSimilarExactLocked(query []float32, queryNorm float64, options SearchOptions) ([]SearchResult, error) {
	topK := make(searchResultMinHeap, 0, options.Limit)

	err := s.db.View(func(tx *bbolt.Tx) error {
		documents := tx.Bucket(documentsBucket)
		if len(options.Filter) > 0 {
			candidateIDs, err := metadataFilterCandidates(tx.Bucket(metadataIndexBucket), options.Filter)
			if err != nil {
				return err
			}
			for _, id := range candidateIDs {
				payload := documents.Get([]byte(id))
				if payload == nil {
					continue
				}
				vectorBytes, err := storedVectorBytes(payload, s.dimension)
				if err != nil {
					return fmt.Errorf("extract vector %s: %w", id, err)
				}

				score := cosineSimilarityBytes(query, queryNorm, vectorBytes)
				if score < options.MinScore {
					continue
				}

				metadata, err := s.unmarshalStoredMetadata(id, payload)
				if err != nil {
					return err
				}

				considerSearchResult(&topK, options.Limit, SearchResult{
					ID:       id,
					Score:    score,
					Metadata: metadata,
				})
			}
			return nil
		}

		cursor := documents.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			vectorBytes, err := storedVectorBytes(value, s.dimension)
			if err != nil {
				return fmt.Errorf("decode %s: %w", string(key), err)
			}

			score := cosineSimilarityBytes(query, queryNorm, vectorBytes)
			if score < options.MinScore {
				continue
			}

			metadata, err := s.unmarshalStoredMetadata(string(key), value)
			if err != nil {
				return err
			}

			considerSearchResult(&topK, options.Limit, SearchResult{
				ID:       string(key),
				Score:    score,
				Metadata: metadata,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	results := make([]SearchResult, len(topK))
	copy(results, topK)
	sortSearchResults(results)

	return results, nil
}

func (s *Store) rebuildIndex() error {
	graph := newIndex(s.hnswConfig)

	err := s.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(documentsBucket).Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			doc, err := s.unmarshalStoredDocument(string(key), value)
			if err != nil {
				return err
			}
			graph.Add(hnsw.MakeNode(doc.ID, cloneVector(doc.Vector)))
		}
		return nil
	})
	if err != nil {
		return err
	}

	s.index = graph
	s.indexStale = false
	return nil
}

func (s *Store) syncMetadataIndex() error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		meta := tx.Bucket(metaBucket)
		generation, err := ensureUint64Meta(meta, generationKey)
		if err != nil {
			return err
		}
		metadataGeneration, err := ensureUint64Meta(meta, metadataIndexGenerationKey)
		if err != nil {
			return err
		}
		if generation == metadataGeneration {
			return nil
		}

		if err := tx.DeleteBucket(metadataIndexBucket); err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
			return fmt.Errorf("reset metadata index bucket: %w", err)
		}
		metadataIndex, err := tx.CreateBucket(metadataIndexBucket)
		if err != nil {
			return fmt.Errorf("create metadata index bucket: %w", err)
		}

		cursor := tx.Bucket(documentsBucket).Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			metadata, err := s.unmarshalStoredMetadata(string(key), value)
			if err != nil {
				return err
			}
			if err := putMetadataIndexEntries(metadataIndex, string(key), metadata); err != nil {
				return err
			}
		}

		return putUint64Meta(meta, metadataIndexGenerationKey, generation)
	})
}

func (s *Store) addToIndexLocked(doc Document) {
	if s.index == nil {
		s.index = newIndex(s.hnswConfig)
	}
	s.index.Add(hnsw.MakeNode(doc.ID, cloneVector(doc.Vector)))
}

func (s *Store) addManyToIndexLocked(docs []Document) {
	for _, doc := range docs {
		s.addToIndexLocked(doc)
	}
}

func (s *Store) marshalDocument(doc Document) ([]byte, error) {
	if doc.ID == "" {
		return nil, ErrDocumentIDRequired
	}
	if err := s.validateVector(doc.Vector); err != nil {
		return nil, err
	}

	payload := make([]byte, 0, len(binaryDocumentMagic)+len(doc.Vector)*4+len(doc.Metadata)*16)
	payload = append(payload, binaryDocumentMagic...)
	payload = appendBinaryUvarint(payload, uint64(len(doc.Metadata)))
	for key, value := range doc.Metadata {
		payload = appendBinaryUvarint(payload, uint64(len(key)))
		payload = append(payload, key...)
		payload = appendBinaryUvarint(payload, uint64(len(value)))
		payload = append(payload, value...)
	}
	payload = appendBinaryUvarint(payload, uint64(len(doc.Vector)))
	payload = appendEncodedVector(payload, doc.Vector)
	return payload, nil
}

func (s *Store) unmarshalStoredDocument(id string, payload []byte) (Document, error) {
	if bytes.HasPrefix(payload, binaryDocumentMagic) {
		metadata, offset, err := decodeBinaryMetadata(payload)
		if err != nil {
			return Document{}, fmt.Errorf("decode document %s metadata: %w", id, err)
		}

		vectorLength, nextOffset, err := readBinaryUvarint(payload, offset)
		if err != nil {
			return Document{}, fmt.Errorf("decode document %s vector length: %w", id, err)
		}
		if vectorLength != uint64(s.dimension) {
			return Document{}, fmt.Errorf("decode document %s: vector dimension mismatch: got %d want %d", id, vectorLength, s.dimension)
		}
		if len(payload[nextOffset:]) != s.dimension*4 {
			return Document{}, fmt.Errorf("decode document %s: invalid vector byte length %d", id, len(payload[nextOffset:]))
		}

		vector, err := decodeVector(payload[nextOffset:], s.dimension)
		if err != nil {
			return Document{}, fmt.Errorf("decode document %s: %w", id, err)
		}

		return Document{
			ID:       id,
			Vector:   vector,
			Metadata: metadata,
		}, nil
	}

	var record storedDocument
	if err := json.Unmarshal(payload, &record); err != nil {
		return Document{}, fmt.Errorf("unmarshal document %s: %w", id, err)
	}

	vector, err := decodeVector(record.Vector, s.dimension)
	if err != nil {
		return Document{}, fmt.Errorf("decode document %s: %w", id, err)
	}

	return Document{
		ID:       id,
		Vector:   vector,
		Metadata: record.Metadata,
	}, nil
}

func (s *Store) unmarshalStoredMetadata(id string, payload []byte) (Metadata, error) {
	if bytes.HasPrefix(payload, binaryDocumentMagic) {
		metadata, _, err := decodeBinaryMetadata(payload)
		if err != nil {
			return nil, fmt.Errorf("decode document %s metadata: %w", id, err)
		}
		return metadata, nil
	}

	var record struct {
		Metadata Metadata `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(payload, &record); err != nil {
		return nil, fmt.Errorf("unmarshal document %s metadata: %w", id, err)
	}
	return record.Metadata, nil
}

func decodeBinaryMetadata(payload []byte) (Metadata, int, error) {
	if !bytes.HasPrefix(payload, binaryDocumentMagic) {
		return nil, 0, errors.New("unknown binary document format")
	}

	offset := len(binaryDocumentMagic)
	metadataCount, nextOffset, err := readBinaryUvarint(payload, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("read metadata count: %w", err)
	}
	offset = nextOffset
	if remaining := len(payload) - offset; metadataCount > uint64(remaining/2) {
		return nil, 0, errors.New("invalid metadata count")
	}

	var metadata Metadata
	if metadataCount > 0 {
		metadata = make(Metadata, int(metadataCount))
	}

	for i := uint64(0); i < metadataCount; i++ {
		key, nextOffset, err := readBinaryString(payload, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("read metadata key: %w", err)
		}
		value, nextOffset2, err := readBinaryString(payload, nextOffset)
		if err != nil {
			return nil, 0, fmt.Errorf("read metadata value: %w", err)
		}
		metadata[key] = value
		offset = nextOffset2
	}

	return metadata, offset, nil
}

// skipBinaryMetadataSection advances past the metadata section without
// allocating any strings or maps, returning the offset where the vector
// length varint begins.
func skipBinaryMetadataSection(payload []byte) (int, error) {
	if !bytes.HasPrefix(payload, binaryDocumentMagic) {
		return 0, errors.New("unknown binary document format")
	}

	offset := len(binaryDocumentMagic)
	metadataCount, nextOffset, err := readBinaryUvarint(payload, offset)
	if err != nil {
		return 0, fmt.Errorf("read metadata count: %w", err)
	}
	offset = nextOffset
	if remaining := len(payload) - offset; metadataCount > uint64(remaining/2) {
		return 0, errors.New("invalid metadata count")
	}

	for i := uint64(0); i < metadataCount; i++ {
		keyLen, nextOff, err := readBinaryUvarint(payload, offset)
		if err != nil {
			return 0, fmt.Errorf("read metadata key length: %w", err)
		}
		if keyLen > uint64(len(payload)-nextOff) {
			return 0, errors.New("truncated metadata key")
		}
		offset = nextOff + int(keyLen)

		valLen, nextOff, err := readBinaryUvarint(payload, offset)
		if err != nil {
			return 0, fmt.Errorf("read metadata value length: %w", err)
		}
		if valLen > uint64(len(payload)-nextOff) {
			return 0, errors.New("truncated metadata value")
		}
		offset = nextOff + int(valLen)
	}

	return offset, nil
}

func readBinaryString(payload []byte, offset int) (string, int, error) {
	length, nextOffset, err := readBinaryUvarint(payload, offset)
	if err != nil {
		return "", 0, err
	}
	if length > uint64(len(payload)-nextOffset) {
		return "", 0, errors.New("truncated string")
	}
	end := nextOffset + int(length)
	return string(payload[nextOffset:end]), end, nil
}

func readBinaryUvarint(payload []byte, offset int) (uint64, int, error) {
	if offset >= len(payload) {
		return 0, 0, errors.New("unexpected end of payload")
	}
	value, n := binary.Uvarint(payload[offset:])
	if n == 0 {
		return 0, 0, errors.New("truncated varint")
	}
	if n < 0 {
		return 0, 0, errors.New("varint overflow")
	}
	return value, offset + n, nil
}

func (s *Store) ensureOpen() error {
	if s == nil || s.db == nil {
		return ErrClosed
	}
	return nil
}

func (s *Store) flushIndexLocked() error {
	if s.db == nil || (!s.indexDirty && !s.indexStale) {
		return nil
	}
	if s.indexStale || s.index == nil {
		if err := s.rebuildIndex(); err != nil {
			return err
		}
	}
	return s.saveIndexLocked()
}

func (s *Store) saveIndexLocked() error {
	if s.index == nil {
		s.index = newIndex(s.hnswConfig)
	}

	if err := os.MkdirAll(filepath.Dir(s.indexPath), 0o755); err != nil {
		return fmt.Errorf("create index directory: %w", err)
	}

	tempFile, err := os.CreateTemp(filepath.Dir(s.indexPath), filepath.Base(s.indexPath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temp index file: %w", err)
	}
	tempPath := tempFile.Name()

	writer := bufio.NewWriter(tempFile)
	exportErr := s.index.Export(writer)
	flushErr := writer.Flush()
	closeErr := tempFile.Close()
	if exportErr != nil || flushErr != nil || closeErr != nil {
		_ = os.Remove(tempPath)
		return errors.Join(exportErr, flushErr, closeErr)
	}

	if err := os.Rename(tempPath, s.indexPath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("replace index file: %w", err)
	}

	err = s.db.Update(func(tx *bbolt.Tx) error {
		meta := tx.Bucket(metaBucket)
		return putUint64Meta(meta, indexGenerationKey, s.generation)
	})
	if err != nil {
		return err
	}

	s.indexSaved = s.generation
	s.indexDirty = false
	return nil
}

func (s *Store) loadIndexFromDisk() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("import index: panic: %v", r)
		}
	}()

	file, err := os.Open(s.indexPath)
	if err != nil {
		return err
	}
	defer file.Close()

	graph := newIndex(s.hnswConfig)
	if err := graph.Import(bufio.NewReader(file)); err != nil {
		return fmt.Errorf("import index: %w", err)
	}

	s.index = graph
	s.indexStale = false
	s.indexDirty = false
	return nil
}

func (s *Store) validateVector(vector []float32) error {
	if len(vector) != s.dimension {
		return fmt.Errorf("vector dimension mismatch: got %d want %d", len(vector), s.dimension)
	}
	for _, v := range vector {
		// Reject NaN and Inf (IEEE 754: all exponent bits set)
		if math.Float32bits(v)&0x7F800000 == 0x7F800000 {
			return ErrInvalidVector
		}
	}
	return nil
}

func encodeVector(vector []float32) []byte {
	return appendEncodedVector(make([]byte, 0, len(vector)*4), vector)
}

func appendEncodedVector(dst []byte, vector []float32) []byte {
	for _, value := range vector {
		dst = binary.LittleEndian.AppendUint32(dst, math.Float32bits(value))
	}
	return dst
}

func appendBinaryUvarint(dst []byte, value uint64) []byte {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], value)
	return append(dst, scratch[:n]...)
}

func newIndex(options HNSWOptions) *hnsw.Graph[string] {
	graph := hnsw.NewGraph[string]()
	graph.Distance = hnsw.CosineDistance
	if options.M > 0 {
		graph.M = options.M
	}
	if options.Ml > 0 {
		graph.Ml = options.Ml
	}
	if options.EfSearch > 0 {
		graph.EfSearch = options.EfSearch
	}
	if options.Seed != 0 {
		graph.Rng.Seed(options.Seed)
	}
	return graph
}

func candidateCount(options SearchOptions, total int) int {
	if total <= 0 {
		return 0
	}
	candidates := options.Candidates
	if candidates <= 0 {
		candidates = options.Limit * 8
	}
	if candidates < options.Limit {
		candidates = options.Limit
	}
	if candidates > total {
		candidates = total
	}
	return candidates
}

type metadataTerm struct {
	key   string
	value string
}

type searchResultMinHeap []SearchResult

func (h searchResultMinHeap) Len() int {
	return len(h)
}

func (h searchResultMinHeap) Less(i, j int) bool {
	return compareSearchResult(h[i], h[j]) < 0
}

func (h searchResultMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *searchResultMinHeap) Push(x any) {
	*h = append(*h, x.(SearchResult))
}

func (h *searchResultMinHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	*h = old[:last]
	return item
}

func considerSearchResult(topK *searchResultMinHeap, limit int, result SearchResult) {
	if topK.Len() < limit {
		heap.Push(topK, result)
		return
	}
	if compareSearchResult(result, (*topK)[0]) > 0 {
		(*topK)[0] = result
		heap.Fix(topK, 0)
	}
}

func sortSearchResults(results []SearchResult) {
	sort.Slice(results, func(i, j int) bool {
		return compareSearchResult(results[i], results[j]) > 0
	})
}

func compareSearchResult(left SearchResult, right SearchResult) int {
	if left.Score > right.Score {
		return 1
	}
	if left.Score < right.Score {
		return -1
	}
	if left.ID < right.ID {
		return 1
	}
	if left.ID > right.ID {
		return -1
	}
	return 0
}

func decodeVector(data []byte, dimension int) ([]float32, error) {
	if len(data) != dimension*4 {
		return nil, fmt.Errorf("invalid vector byte length %d", len(data))
	}

	vector := make([]float32, dimension)
	for i := 0; i < dimension; i++ {
		vector[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[i*4:]))
	}
	return vector, nil
}

func vectorNorm(vector []float32) float64 {
	var sum float64
	for _, value := range vector {
		v := float64(value)
		sum += v * v
	}
	return math.Sqrt(sum)
}

func cosineSimilarity(query []float32, queryNorm float64, vector []float32) float64 {
	var dotProduct float64
	var vectorNormSum float64
	for i, value := range query {
		q := float64(value)
		v := float64(vector[i])
		dotProduct += q * v
		vectorNormSum += v * v
	}

	vectorNorm := math.Sqrt(vectorNormSum)
	if vectorNorm == 0 {
		return 0
	}

	return dotProduct / (queryNorm * vectorNorm)
}

// cosineSimilarityBytes computes cosine similarity directly from raw
// little-endian float32 bytes, avoiding a []float32 allocation.
func cosineSimilarityBytes(query []float32, queryNorm float64, vectorBytes []byte) float64 {
	var dotProduct float64
	var vectorNormSum float64
	for i, qVal := range query {
		bits := binary.LittleEndian.Uint32(vectorBytes[i*4:])
		v := float64(math.Float32frombits(bits))
		dotProduct += float64(qVal) * v
		vectorNormSum += v * v
	}

	vectorNorm := math.Sqrt(vectorNormSum)
	if vectorNorm == 0 {
		return 0
	}

	return dotProduct / (queryNorm * vectorNorm)
}

func cloneVector(vector []float32) []float32 {
	if len(vector) == 0 {
		return nil
	}
	cloned := make([]float32, len(vector))
	copy(cloned, vector)
	return cloned
}

func metadataFilterCandidates(indexBucket *bbolt.Bucket, filter Metadata) ([]string, error) {
	if len(filter) == 0 {
		return nil, nil
	}

	terms := sortedMetadataTerms(filter)
	postingLists := make([][]string, 0, len(terms))
	for _, term := range terms {
		ids, err := metadataPostingIDs(indexBucket, term)
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			return nil, nil
		}
		postingLists = append(postingLists, ids)
	}

	sort.Slice(postingLists, func(i, j int) bool {
		return len(postingLists[i]) < len(postingLists[j])
	})

	candidates := make(map[string]struct{}, len(postingLists[0]))
	for _, id := range postingLists[0] {
		candidates[id] = struct{}{}
	}

	for _, ids := range postingLists[1:] {
		next := make(map[string]struct{}, len(candidates))
		for _, id := range ids {
			if _, ok := candidates[id]; ok {
				next[id] = struct{}{}
			}
		}
		if len(next) == 0 {
			return nil, nil
		}
		candidates = next
	}

	result := make([]string, 0, len(candidates))
	for id := range candidates {
		result = append(result, id)
	}
	sort.Strings(result)
	return result, nil
}

func metadataPostingIDs(indexBucket *bbolt.Bucket, term metadataTerm) ([]string, error) {
	prefix := metadataPostingPrefix(term.key, term.value)
	cursor := indexBucket.Cursor()
	ids := make([]string, 0)
	for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
		ids = append(ids, string(key[len(prefix):]))
	}
	return ids, nil
}

func putMetadataIndexEntries(indexBucket *bbolt.Bucket, docID string, metadata Metadata) error {
	for key, value := range metadata {
		if err := indexBucket.Put(metadataPostingKey(key, value, docID), nil); err != nil {
			return err
		}
	}
	return nil
}

func deleteMetadataIndexEntries(indexBucket *bbolt.Bucket, docID string, metadata Metadata) error {
	for key, value := range metadata {
		if err := indexBucket.Delete(metadataPostingKey(key, value, docID)); err != nil {
			return err
		}
	}
	return nil
}

func metadataPostingKey(key string, value string, docID string) []byte {
	buf := make([]byte, 0, len(key)+len(value)+len(docID)+binary.MaxVarintLen64*2)
	buf = appendMetadataPostingPrefix(buf, key, value)
	return append(buf, docID...)
}

func metadataPostingPrefix(key string, value string) []byte {
	buf := make([]byte, 0, len(key)+len(value)+binary.MaxVarintLen64*2)
	return appendMetadataPostingPrefix(buf, key, value)
}

func appendMetadataPostingPrefix(dst []byte, key string, value string) []byte {
	dst = appendBinaryUvarint(dst, uint64(len(key)))
	dst = append(dst, key...)
	dst = appendBinaryUvarint(dst, uint64(len(value)))
	dst = append(dst, value...)
	return dst
}

func sortedMetadataTerms(filter Metadata) []metadataTerm {
	terms := make([]metadataTerm, 0, len(filter))
	for key, value := range filter {
		terms = append(terms, metadataTerm{key: key, value: value})
	}
	sort.Slice(terms, func(i, j int) bool {
		if terms[i].key == terms[j].key {
			return terms[i].value < terms[j].value
		}
		return terms[i].key < terms[j].key
	})
	return terms
}

func storedVectorEqualPayload(leftPayload []byte, rightPayload []byte, dimension int) (bool, error) {
	leftVector, err := storedVectorBytes(leftPayload, dimension)
	if err != nil {
		return false, err
	}
	rightVector, err := storedVectorBytes(rightPayload, dimension)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftVector, rightVector), nil
}

func storedVectorBytes(payload []byte, dimension int) ([]byte, error) {
	if bytes.HasPrefix(payload, binaryDocumentMagic) {
		offset, err := skipBinaryMetadataSection(payload)
		if err != nil {
			return nil, err
		}
		vectorLength, nextOffset, err := readBinaryUvarint(payload, offset)
		if err != nil {
			return nil, err
		}
		if vectorLength != uint64(dimension) {
			return nil, fmt.Errorf("vector dimension mismatch: got %d want %d", vectorLength, dimension)
		}
		vectorBytes := payload[nextOffset:]
		if len(vectorBytes) != dimension*4 {
			return nil, fmt.Errorf("invalid vector byte length %d", len(vectorBytes))
		}
		return vectorBytes, nil
	}

	var record struct {
		Vector []byte `json:"vector"`
	}
	if err := json.Unmarshal(payload, &record); err != nil {
		return nil, err
	}
	if len(record.Vector) != dimension*4 {
		return nil, fmt.Errorf("invalid vector byte length %d", len(record.Vector))
	}
	return record.Vector, nil
}

func resolveIndexPath(options Options) string {
	if options.IndexPath != "" {
		return options.IndexPath
	}
	return options.Path + ".hnsw"
}

func ensureUint64Meta(meta *bbolt.Bucket, key []byte) (uint64, error) {
	raw := meta.Get(key)
	if raw == nil {
		if err := putUint64Meta(meta, key, 0); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid meta value for %s", key)
	}
	return binary.LittleEndian.Uint64(raw), nil
}

func incrementUint64Meta(meta *bbolt.Bucket, key []byte) (uint64, error) {
	current, err := ensureUint64Meta(meta, key)
	if err != nil {
		return 0, err
	}
	next := current + 1
	if err := putUint64Meta(meta, key, next); err != nil {
		return 0, err
	}
	return next, nil
}

func putUint64Meta(meta *bbolt.Bucket, key []byte, value uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	return meta.Put(key, buf[:])
}
