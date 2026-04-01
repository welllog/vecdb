package vecdb

import (
	"errors"
	"fmt"
	"strings"
	"time"

	bbolt "go.etcd.io/bbolt"
)

const (
	defaultShortTermMemoryTTL = 24 * time.Hour
	defaultSessionMemoryTTL   = 7 * 24 * time.Hour

	memoryMetadataPrefix = "__vecdb_memory."
	memoryContentKey     = memoryMetadataPrefix + "content"
	memoryLevelKey       = memoryMetadataPrefix + "level"
	memoryCreatedAtKey   = memoryMetadataPrefix + "created_at"
	memoryExpiresAtKey   = memoryMetadataPrefix + "expires_at"
)

var memoryContentsBucket = []byte("__vecdb_memory.contents")

var (
	// ErrInvalidMemoryLevel indicates an unknown memory lifecycle level.
	ErrInvalidMemoryLevel = errors.New("vecdb: invalid memory level")
	// ErrMemoryContentRequired indicates a memory was stored without content.
	ErrMemoryContentRequired = errors.New("vecdb: memory content is required")
	// ErrReservedMemoryMetadataKey indicates user metadata attempted to use reserved system keys.
	ErrReservedMemoryMetadataKey = errors.New("vecdb: reserved memory metadata key")
	// ErrInvalidMemoryRecord indicates a stored document cannot be decoded as a memory record.
	ErrInvalidMemoryRecord = errors.New("vecdb: invalid memory record")
)

// MemoryLevel controls the default lifecycle policy assigned to a memory.
type MemoryLevel string

const (
	// MemoryLevelShortTerm expires after a short default TTL.
	MemoryLevelShortTerm MemoryLevel = "short_term"
	// MemoryLevelSession expires after a longer session-oriented TTL.
	MemoryLevelSession MemoryLevel = "session"
	// MemoryLevelLongTerm does not expire by default.
	MemoryLevelLongTerm MemoryLevel = "long_term"
)

// MemoryStoreOptions configures a memory store built on top of vecdb.
type MemoryStoreOptions struct {
	Store        Options
	ShortTermTTL time.Duration
	SessionTTL   time.Duration
}

// MemoryStore wraps Store with lifecycle-aware memory helpers.
type MemoryStore struct {
	store        *Store
	shortTermTTL time.Duration
	sessionTTL   time.Duration
}

// Memory is a lifecycle-aware record stored by MemoryStore.
type Memory struct {
	ID        string
	Vector    []float32
	Content   string
	Level     MemoryLevel
	Metadata  Metadata
	CreatedAt time.Time
	ExpiresAt time.Time
}

// RecallOptions configures memory recall behavior.
type RecallOptions struct {
	Limit          int
	MinScore       float64
	Exact          bool
	Candidates     int
	Level          MemoryLevel
	Filter         Metadata
	IncludeExpired bool
}

// MemoryMatch is a recalled memory and its similarity score.
type MemoryMatch struct {
	ID        string
	Content   string
	Level     MemoryLevel
	Metadata  Metadata
	CreatedAt time.Time
	ExpiresAt time.Time
	Score     float64
}

// OpenMemory opens or creates a memory store with default lifecycle policies.
func OpenMemory(path string, dimension int) (*MemoryStore, error) {
	store, err := Open(path, dimension)
	if err != nil {
		return nil, err
	}
	mem := &MemoryStore{
		store:        store,
		shortTermTTL: defaultShortTermMemoryTTL,
		sessionTTL:   defaultSessionMemoryTTL,
	}
	if err := mem.ensureBuckets(); err != nil {
		_ = store.Close()
		return nil, err
	}
	return mem, nil
}

// OpenMemoryWithOptions opens or creates a memory store with custom lifecycle policies.
func OpenMemoryWithOptions(options MemoryStoreOptions) (*MemoryStore, error) {
	if options.ShortTermTTL < 0 {
		return nil, errors.New("vecdb: short-term ttl must be non-negative")
	}
	if options.SessionTTL < 0 {
		return nil, errors.New("vecdb: session ttl must be non-negative")
	}

	store, err := OpenWithOptions(options.Store)
	if err != nil {
		return nil, err
	}

	shortTermTTL := options.ShortTermTTL
	if shortTermTTL == 0 {
		shortTermTTL = defaultShortTermMemoryTTL
	}
	sessionTTL := options.SessionTTL
	if sessionTTL == 0 {
		sessionTTL = defaultSessionMemoryTTL
	}

	mem := &MemoryStore{
		store:        store,
		shortTermTTL: shortTermTTL,
		sessionTTL:   sessionTTL,
	}
	if err := mem.ensureBuckets(); err != nil {
		_ = store.Close()
		return nil, err
	}
	return mem, nil
}

// Remember inserts or replaces a memory.
func (m *MemoryStore) Remember(memory Memory) error {
	if err := m.ensureOpen(); err != nil {
		return err
	}
	record, err := m.memoryRecord(memory)
	if err != nil {
		return err
	}
	return m.upsertRecord(record)
}

// RememberMany inserts or replaces multiple memories in one batch.
func (m *MemoryStore) RememberMany(memories []Memory) error {
	if err := m.ensureOpen(); err != nil {
		return err
	}
	if len(memories) == 0 {
		return nil
	}

	records := make([]memoryRecord, 0, len(memories))
	for _, memory := range memories {
		record, err := m.memoryRecord(memory)
		if err != nil {
			return err
		}
		records = append(records, record)
	}
	return m.upsertRecords(records)
}

// Get returns a memory by ID.
func (m *MemoryStore) Get(id string) (Memory, error) {
	if err := m.ensureOpen(); err != nil {
		return Memory{}, err
	}
	if id == "" {
		return Memory{}, ErrDocumentIDRequired
	}

	m.store.mu.RLock()
	defer m.store.mu.RUnlock()

	if err := m.store.ensureOpen(); err != nil {
		return Memory{}, err
	}

	var doc Document
	var content string
	err := m.store.db.View(func(tx *bbolt.Tx) error {
		payload := tx.Bucket(documentsBucket).Get([]byte(id))
		if payload == nil {
			return ErrNotFound
		}

		stored, err := m.store.unmarshalStoredDocument(id, payload)
		if err != nil {
			return err
		}
		doc = stored

		content = loadMemoryContent(tx.Bucket(memoryContentsBucket), id)
		return nil
	})
	if err != nil {
		return Memory{}, err
	}
	return decodeMemoryDocument(doc, content)
}

// Recall searches for relevant memories while respecting lifecycle settings.
func (m *MemoryStore) Recall(query []float32, options RecallOptions) ([]MemoryMatch, error) {
	if err := m.ensureOpen(); err != nil {
		return nil, err
	}
	if err := m.store.validateVector(query); err != nil {
		return nil, fmt.Errorf("validate query vector: %w", err)
	}
	if options.Limit <= 0 {
		return nil, ErrLimitRequired
	}
	if vectorNorm(query) == 0 {
		return nil, ErrZeroVector
	}
	filter, err := m.recallFilter(options.Level, options.Filter)
	if err != nil {
		return nil, err
	}

	total, err := m.store.Count()
	if err != nil {
		return nil, err
	}
	if total == 0 {
		return []MemoryMatch{}, nil
	}

	fetchLimit := options.Limit

	for {
		results, err := m.store.FindSimilar(query, SearchOptions{
			Limit:      fetchLimit,
			MinScore:   options.MinScore,
			Exact:      options.Exact,
			Candidates: options.Candidates,
			Filter:     filter,
		})
		if err != nil {
			return nil, err
		}

		contents, err := m.loadMemoryContents(results)
		if err != nil {
			return nil, err
		}

		matches := make([]MemoryMatch, 0, min(len(results), options.Limit))
		for _, result := range results {
			memory, err := decodeMemoryRecord(result.ID, result.Metadata, contents[result.ID])
			if err != nil {
				// Record may have been concurrently deleted between search
				// and content loading; skip it rather than failing the recall.
				continue
			}
			if !options.IncludeExpired && memoryExpired(memory.ExpiresAt, time.Now().UTC()) {
				continue
			}

			matches = append(matches, MemoryMatch{
				ID:        memory.ID,
				Content:   memory.Content,
				Level:     memory.Level,
				Metadata:  memory.Metadata,
				CreatedAt: memory.CreatedAt,
				ExpiresAt: memory.ExpiresAt,
				Score:     result.Score,
			})
			if len(matches) == options.Limit {
				return matches, nil
			}
		}

		if len(results) < fetchLimit || fetchLimit >= total {
			return matches, nil
		}

		next := fetchLimit * 2
		if next <= fetchLimit {
			next = total
		}
		if next > total {
			next = total
		}
		fetchLimit = next
	}
}

// Forget deletes a memory by ID.
func (m *MemoryStore) Forget(id string) error {
	if err := m.ensureOpen(); err != nil {
		return err
	}
	if id == "" {
		return ErrDocumentIDRequired
	}

	m.store.mu.Lock()
	defer m.store.mu.Unlock()

	if err := m.store.ensureOpen(); err != nil {
		return err
	}

	var generation uint64
	err := m.store.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(documentsBucket)
		metadataIndex := tx.Bucket(metadataIndexBucket)
		meta := tx.Bucket(metaBucket)
		contents := tx.Bucket(memoryContentsBucket)
		key := []byte(id)
		payload := bucket.Get(key)
		if payload == nil {
			return ErrNotFound
		}
		storedMetadata, err := m.store.unmarshalStoredMetadata(id, payload)
		if err != nil {
			return err
		}
		if err := deleteMetadataIndexEntries(metadataIndex, id, storedMetadata); err != nil {
			return err
		}
		if err := bucket.Delete(key); err != nil {
			return err
		}
		if contents != nil {
			if err := contents.Delete(key); err != nil {
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
		generation = nextGeneration
		return nil
	})
	if err == nil {
		m.store.generation = generation
		m.store.indexDirty = true
		m.store.indexStale = true
		m.store.index = nil
	}
	return err
}

// PruneExpired removes expired memories and returns the number deleted.
func (m *MemoryStore) PruneExpired() (int, error) {
	if m == nil || m.store == nil {
		return 0, ErrClosed
	}

	now := time.Now().UTC()
	ids := make([]string, 0)

	m.store.mu.RLock()
	if err := m.store.ensureOpen(); err != nil {
		m.store.mu.RUnlock()
		return 0, err
	}
	err := m.store.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(documentsBucket).Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			metadata, err := m.store.unmarshalStoredMetadata(string(key), value)
			if err != nil {
				return err
			}
			memory, err := decodeMemoryRecord(string(key), metadata, loadMemoryContent(tx.Bucket(memoryContentsBucket), string(key)))
			if err != nil {
				return err
			}
			if memoryExpired(memory.ExpiresAt, now) {
				ids = append(ids, string(key))
			}
		}
		return nil
	})
	m.store.mu.RUnlock()
	if err != nil {
		return 0, err
	}

	removed := 0
	for _, id := range ids {
		if err := m.Forget(id); err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return removed, err
		}
		removed++
	}
	return removed, nil
}

// Count returns the number of stored memories.
func (m *MemoryStore) Count() (int, error) {
	if err := m.ensureOpen(); err != nil {
		return 0, err
	}
	return m.store.Count()
}

// Dimension returns the memory vector dimension.
func (m *MemoryStore) Dimension() int {
	if m == nil || m.store == nil {
		return 0
	}
	return m.store.Dimension()
}

// RefreshIndex rebuilds the underlying in-memory index.
func (m *MemoryStore) RefreshIndex() error {
	if err := m.ensureOpen(); err != nil {
		return err
	}
	return m.store.RefreshIndex()
}

// Close closes the underlying store.
func (m *MemoryStore) Close() error {
	if m == nil {
		return nil
	}
	if m.store == nil {
		return nil
	}
	return m.store.Close()
}

func (m *MemoryStore) ensureOpen() error {
	if m == nil || m.store == nil {
		return ErrClosed
	}
	return nil
}

type memoryRecord struct {
	document Document
	content  []byte
}

func (m *MemoryStore) ensureBuckets() error {
	if err := m.ensureOpen(); err != nil {
		return err
	}

	m.store.mu.Lock()
	defer m.store.mu.Unlock()

	if err := m.store.ensureOpen(); err != nil {
		return err
	}

	return m.store.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(memoryContentsBucket)
		if err != nil {
			return fmt.Errorf("create memory contents bucket: %w", err)
		}
		return nil
	})
}

func (m *MemoryStore) memoryRecord(memory Memory) (memoryRecord, error) {
	level, err := normalizeMemoryLevel(memory.Level)
	if err != nil {
		return memoryRecord{}, err
	}
	if memory.Content == "" {
		return memoryRecord{}, ErrMemoryContentRequired
	}
	metadata, err := validateUserMemoryMetadata(memory.Metadata)
	if err != nil {
		return memoryRecord{}, err
	}

	createdAt := memory.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	expiresAt := memory.ExpiresAt.UTC()
	if expiresAt.IsZero() {
		expiresAt = m.defaultExpiry(level, createdAt)
	}

	metadata[memoryLevelKey] = string(level)
	metadata[memoryCreatedAtKey] = createdAt.Format(time.RFC3339Nano)
	if !expiresAt.IsZero() {
		metadata[memoryExpiresAtKey] = expiresAt.Format(time.RFC3339Nano)
	}

	return memoryRecord{
		document: Document{
			ID:       memory.ID,
			Vector:   memory.Vector,
			Metadata: metadata,
		},
		content: []byte(memory.Content),
	}, nil
}

func (m *MemoryStore) defaultExpiry(level MemoryLevel, createdAt time.Time) time.Time {
	switch level {
	case MemoryLevelShortTerm:
		return createdAt.Add(m.shortTermTTL)
	case MemoryLevelSession:
		return createdAt.Add(m.sessionTTL)
	default:
		return time.Time{}
	}
}

func (m *MemoryStore) recallFilter(level MemoryLevel, filter Metadata) (Metadata, error) {
	merged, err := validateUserMemoryMetadata(filter)
	if err != nil {
		return nil, err
	}
	normalized, err := normalizeMemoryLevel(level)
	if err != nil {
		return nil, err
	}
	if level != "" {
		merged[memoryLevelKey] = string(normalized)
	}
	return merged, nil
}

func normalizeMemoryLevel(level MemoryLevel) (MemoryLevel, error) {
	if level == "" {
		return MemoryLevelLongTerm, nil
	}
	switch level {
	case MemoryLevelShortTerm, MemoryLevelSession, MemoryLevelLongTerm:
		return level, nil
	default:
		return "", ErrInvalidMemoryLevel
	}
}

func validateUserMemoryMetadata(metadata Metadata) (Metadata, error) {
	if len(metadata) == 0 {
		return Metadata{}, nil
	}
	cloned := make(Metadata, len(metadata))
	for key, value := range metadata {
		if strings.HasPrefix(key, memoryMetadataPrefix) {
			return nil, fmt.Errorf("%w: %s", ErrReservedMemoryMetadataKey, key)
		}
		cloned[key] = value
	}
	return cloned, nil
}

func (m *MemoryStore) loadMemoryContents(results []SearchResult) (map[string]string, error) {
	if len(results) == 0 {
		return map[string]string{}, nil
	}

	contents := make(map[string]string, len(results))

	m.store.mu.RLock()
	defer m.store.mu.RUnlock()

	if err := m.store.ensureOpen(); err != nil {
		return nil, err
	}

	err := m.store.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(memoryContentsBucket)
		for _, result := range results {
			contents[result.ID] = loadMemoryContent(bucket, result.ID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return contents, nil
}

func (m *MemoryStore) upsertRecord(record memoryRecord) error {
	return m.upsertRecords([]memoryRecord{record})
}

func (m *MemoryStore) upsertRecords(records []memoryRecord) error {
	if len(records) == 0 {
		return nil
	}

	type item struct {
		id      string
		payload []byte
		content []byte
	}

	items := make([]item, 0, len(records))
	docsToAdd := make([]Document, 0, len(records))
	seenIDs := make(map[string]struct{}, len(records))
	rebuildIndex := false
	var generation uint64

	for _, record := range records {
		payload, err := m.store.marshalDocument(record.document)
		if err != nil {
			return err
		}
		if _, exists := seenIDs[record.document.ID]; exists {
			rebuildIndex = true
		}
		seenIDs[record.document.ID] = struct{}{}
		items = append(items, item{
			id:      record.document.ID,
			payload: payload,
			content: record.content,
		})
	}

	m.store.mu.Lock()
	defer m.store.mu.Unlock()

	if err := m.store.ensureOpen(); err != nil {
		return err
	}

	indexStillSaved := !m.store.indexDirty && !m.store.indexStale
	if rebuildIndex {
		indexStillSaved = false
	}

	err := m.store.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(documentsBucket)
		metadataIndex := tx.Bucket(metadataIndexBucket)
		meta := tx.Bucket(metaBucket)
		contents := tx.Bucket(memoryContentsBucket)
		for i, item := range items {
			key := []byte(item.id)
			existingPayload := bucket.Get(key)
			if existingPayload != nil {
				storedMetadata, err := m.store.unmarshalStoredMetadata(item.id, existingPayload)
				if err != nil {
					return err
				}
				if err := deleteMetadataIndexEntries(metadataIndex, item.id, storedMetadata); err != nil {
					return err
				}

				sameVector, err := storedVectorEqualPayload(existingPayload, item.payload, m.store.dimension)
				if err != nil {
					return err
				}
				if !sameVector {
					rebuildIndex = true
					indexStillSaved = false
				}
			} else {
				docsToAdd = append(docsToAdd, records[i].document)
				indexStillSaved = false
			}
			if err := bucket.Put(key, item.payload); err != nil {
				return err
			}
			if err := contents.Put(key, item.content); err != nil {
				return err
			}
			if err := putMetadataIndexEntries(metadataIndex, item.id, records[i].document.Metadata); err != nil {
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
		m.store.generation = generation
		if rebuildIndex || m.store.indexStale {
			m.store.indexDirty = true
			m.store.index = nil
			m.store.indexStale = true
			return nil
		}
		if len(docsToAdd) > 0 {
			m.store.indexDirty = true
			m.store.addManyToIndexLocked(docsToAdd)
			return nil
		}
		if indexStillSaved {
			m.store.indexSaved = generation
			m.store.indexDirty = false
			return nil
		}
		m.store.indexDirty = true
	}
	return err
}

func decodeMemoryDocument(doc Document, content string) (Memory, error) {
	memory, err := decodeMemoryRecord(doc.ID, doc.Metadata, content)
	if err != nil {
		return Memory{}, err
	}
	memory.Vector = doc.Vector
	return memory, nil
}

func decodeMemoryRecord(id string, metadata Metadata, content string) (Memory, error) {
	if content == "" {
		content = metadata[memoryContentKey]
	}
	if content == "" {
		return Memory{}, fmt.Errorf("%w: missing content", ErrInvalidMemoryRecord)
	}

	levelValue, ok := metadata[memoryLevelKey]
	if !ok {
		return Memory{}, fmt.Errorf("%w: missing level", ErrInvalidMemoryRecord)
	}
	level, err := normalizeMemoryLevel(MemoryLevel(levelValue))
	if err != nil {
		return Memory{}, err
	}

	createdRaw, ok := metadata[memoryCreatedAtKey]
	if !ok {
		return Memory{}, fmt.Errorf("%w: missing created_at", ErrInvalidMemoryRecord)
	}
	createdAt, err := time.Parse(time.RFC3339Nano, createdRaw)
	if err != nil {
		return Memory{}, fmt.Errorf("%w: invalid created_at", ErrInvalidMemoryRecord)
	}

	var expiresAt time.Time
	if expiresRaw := metadata[memoryExpiresAtKey]; expiresRaw != "" {
		expiresAt, err = time.Parse(time.RFC3339Nano, expiresRaw)
		if err != nil {
			return Memory{}, fmt.Errorf("%w: invalid expires_at", ErrInvalidMemoryRecord)
		}
	}

	userMetadata := make(Metadata, len(metadata))
	for key, value := range metadata {
		if strings.HasPrefix(key, memoryMetadataPrefix) {
			continue
		}
		userMetadata[key] = value
	}

	return Memory{
		ID:        id,
		Content:   content,
		Level:     level,
		Metadata:  userMetadata,
		CreatedAt: createdAt,
		ExpiresAt: expiresAt,
	}, nil
}

func loadMemoryContent(bucket *bbolt.Bucket, id string) string {
	if bucket == nil {
		return ""
	}
	raw := bucket.Get([]byte(id))
	if raw == nil {
		return ""
	}
	return string(raw)
}

func memoryExpired(expiresAt time.Time, now time.Time) bool {
	return !expiresAt.IsZero() && !expiresAt.After(now)
}
