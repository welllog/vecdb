# vecdb 接口文档

本文档面向 vecdb 的使用者，说明库的公开类型、接口行为、错误约定、并发语义与当前限制。

## 库简介

vecdb 是一个嵌入式 Go 向量存储库，适合在单进程内直接使用。

主要特性：

- 基于 bbolt 的本地持久化存储
- 默认使用 HNSW 做近似召回，再用精确余弦相似度重排
- 提供显式精确检索接口
- 支持基于持久化 postings 索引的元数据精确过滤
- 支持可复用的 HNSW sidecar 索引文件

当前实现中，bbolt 数据文件是唯一事实来源。内存中的 HNSW 索引优先从 sidecar 文件加载；如果 sidecar 不存在、已过期或损坏，则会根据持久化文档重建。元数据过滤依赖持久化在 bbolt 内部的 postings 索引，因此重启后仍可直接使用，不需要额外回填内存结构。

## 快速开始

```go
store, err := vecdb.Open("./docs.vecdb", 384)
if err != nil {
	log.Fatal(err)
}
defer store.Close()

err = store.Put("doc-1", embedding, vecdb.Metadata{
	"title": "向量检索入门",
	"topic": "go",
})
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

_ = results
```

## 公开类型

### Metadata

```go
type Metadata map[string]string
```

用于保存文档的简单键值元数据。

- 当前只支持字符串键和值
- 检索过滤时采用精确匹配

### Document

```go
type Document struct {
	ID       string
	Vector   []float32
	Metadata Metadata
}
```

表示一条完整的向量文档。

- `ID` 为必填字段，且在同一个库中必须唯一
- `Vector` 的维度必须和存储实例的维度一致
- `Metadata` 为可选字段

### SearchResult

```go
type SearchResult struct {
	ID       string
	Score    float64
	Metadata Metadata
}
```

表示一条检索结果。

- `Score` 为精确余弦相似度分数
- 分数越高，表示越相似
- 常见取值范围是 `[-1, 1]`

### HNSWOptions

```go
type HNSWOptions struct {
	M        int
	Ml       float64
	EfSearch int
	Seed     int64
}
```

用于调节 HNSW 索引参数。

- `M`：每个节点保留的最大邻居数
- `Ml`：层级生成参数
- `EfSearch`：查询时的候选扩展规模
- `Seed`：随机种子，便于复现实验结果

字段为零值时，底层 HNSW 使用默认参数。

### Options

```go
type Options struct {
	Path      string
	IndexPath string
	Dimension int
	FileMode  os.FileMode
	HNSW      HNSWOptions
}
```

用于打开存储实例。

- `Path`：数据库文件路径，必填
- `IndexPath`：HNSW sidecar 文件路径，选填；为空时默认使用 `Path + ".hnsw"`
- `Dimension`：向量维度，必填且必须大于 0
- `FileMode`：数据库文件权限，零值时默认 `0600`
- `HNSW`：HNSW 索引配置

### SearchOptions

```go
type SearchOptions struct {
	Limit      int
	MinScore   float64
	Exact      bool
	Candidates int
	Filter     Metadata
}
```

用于控制检索行为。

- `Limit`：返回结果数量上限，必填且必须大于 0
- `MinScore`：最小相似度阈值，低于该值的结果会被丢弃
- `Exact`：为 `true` 时强制走精确扫描
- `Candidates`：近似检索时 HNSW 召回的候选数，零值时默认使用 `Limit * 8`
- `Filter`：元数据精确匹配条件

补充说明：

- 当 `Exact=true` 时，一定走精确扫描
- 当 `Filter` 非空时，当前实现仍然走精确打分，以保证过滤结果正确；但会先通过持久化 metadata postings 索引收敛候选集合，而不是直接全表扫描
- `Candidates` 只在近似检索路径生效
- `Candidates` 控制的是“先从 HNSW 索引中取回多少个候选，再在这些候选里做后续打分和截断”，不是“从底层数据中只读取多少条再做全量匹配”
- 如果走精确路径，例如 `Exact=true`、`Filter` 非空或内存索引失效，则由精确扫描逻辑决定遍历范围，此时 `Candidates` 不参与

### MemoryLevel

```go
type MemoryLevel string
```

记忆系统使用的生命周期级别：

- `MemoryLevelShortTerm`：默认过期时间 24 小时
- `MemoryLevelSession`：默认过期时间 7 天
- `MemoryLevelLongTerm`：默认不过期

### Memory

```go
type Memory struct {
	ID        string
	Vector    []float32
	Content   string
	Level     MemoryLevel
	Metadata  Metadata
	CreatedAt time.Time
	ExpiresAt time.Time
}
```

表示一条带生命周期的记忆记录。

- `Content` 为必填
- `Level` 零值时默认视为 `MemoryLevelLongTerm`
- `CreatedAt` 为空时自动使用当前时间
- `ExpiresAt` 为空时按生命周期级别套用默认策略
- 当前实现中，生命周期字段保存在底层文档 metadata 中，而记忆正文会单独存储在同一个 `.vecdb` 文件内

### RecallOptions

```go
type RecallOptions struct {
	Limit          int
	MinScore       float64
	Exact          bool
	Candidates     int
	Level          MemoryLevel
	Filter         Metadata
	IncludeExpired bool
}
```

用于控制记忆召回行为。

- `Level`：按生命周期级别过滤
- `Filter`：按用户元数据做精确过滤
- `IncludeExpired`：为 `true` 时允许返回已过期记忆

### MemoryMatch

```go
type MemoryMatch struct {
	ID        string
	Content   string
	Level     MemoryLevel
	Metadata  Metadata
	CreatedAt time.Time
	ExpiresAt time.Time
	Score     float64
}
```

表示一条召回结果。

### MemoryStoreOptions

```go
type MemoryStoreOptions struct {
	Store        Options
	ShortTermTTL time.Duration
	SessionTTL   time.Duration
}
```

用于打开记忆系统时覆盖默认 TTL。

- `Store`：底层 vecdb 打开参数
- `ShortTermTTL`：短期记忆默认 TTL，零值时默认 24 小时
- `SessionTTL`：会话记忆默认 TTL，零值时默认 7 天

## 构造与生命周期接口

### Open

```go
func Open(path string, dimension int) (*Store, error)
```

使用最少参数打开一个存储实例。

行为说明：

- 数据库文件不存在时会自动创建
- 数据库已存在时会校验维度是否匹配
- 打开成功后会优先尝试加载 sidecar 索引
- 如果 sidecar 与数据库代际不一致，或 sidecar 无法读取，则退回到根据持久化文档重建索引

### OpenWithOptions

```go
func OpenWithOptions(options Options) (*Store, error)
```

提供完整可配置的打开方式。

适用场景：

- 需要自定义文件权限
- 需要调节 HNSW 参数
- 需要显式控制初始化参数

### Close

```go
func (s *Store) Close() error
```

关闭存储实例。

行为说明：

- 可重复调用
- 关闭后其余公开方法会返回 `ErrClosed`
- 关闭时会同时释放数据库句柄并清空内存索引引用
- 如果索引有未持久化变更，会在关闭前写入 sidecar 文件

### Dimension

```go
func (s *Store) Dimension() int
```

返回当前存储实例的向量维度。

### RefreshIndex

```go
func (s *Store) RefreshIndex() error
```

显式根据当前数据库内容重建内存中的 HNSW 索引。

适用场景：

- 覆盖写或删除之后，希望尽快恢复近似检索性能
- 批量更新结束后，希望主动刷新 ANN 索引

注意：

- 该方法会重建整个内存索引
- 该方法会将索引标记为“待持久化”，真正写入 sidecar 发生在 `Close` 时

## 写入接口

### Put

```go
func (s *Store) Put(id string, vector []float32, metadata Metadata) error
```

使用简化参数写入或覆盖一条文档。

适合单条写入场景。

### Upsert

```go
func (s *Store) Upsert(doc Document) error
```

写入或覆盖一条完整文档。

行为说明：

- 新 ID：写入 bbolt 后增量加入 HNSW
- 已存在 ID 且向量发生变化：写入 bbolt 后将内存索引标记为失效
- 已存在 ID 且仅元数据变化：保持当前 HNSW 索引可用，并同步更新 metadata postings 索引

当覆盖写导致向量变化时，不会立即同步重建 HNSW，而是让默认近似检索暂时回退到精确扫描。这样可以避免在写路径上做全量重建，同时仍然保证结果正确。若仅元数据变化且向量不变，则不会把 HNSW sidecar 打脏。

### UpsertMany

```go
func (s *Store) UpsertMany(docs []Document) error
```

批量写入文档。

行为说明：

- 纯新增批次：写入后增量加入 HNSW
- 批次中出现向量变化的覆盖写或重复 ID：写入后将内存索引标记为失效
- 仅元数据变化的覆盖写会保留当前 HNSW，并同步更新 metadata postings 索引

适合场景：

- 初始导入
- 离线批量灌库

## 读取接口

### Get

```go
func (s *Store) Get(id string) (Document, error)
```

按 ID 读取完整文档。

返回规则：

- 成功时返回完整 `Document`
- 文档不存在时返回 `ErrNotFound`

### Delete

```go
func (s *Store) Delete(id string) error
```

按 ID 删除文档。

行为说明：

- 删除成功后会将内存索引标记为失效
- 文档不存在时返回 `ErrNotFound`

### Count

```go
func (s *Store) Count() (int, error)
```

返回当前文档总数。

## 检索接口

### Search

```go
func (s *Store) Search(query []float32, limit int) ([]SearchResult, error)
```

最简单的检索入口。

等价于：

```go
store.FindSimilar(query, vecdb.SearchOptions{Limit: limit})
```

行为说明：

- 使用 HNSW 做近似召回
- 只对召回候选做精确余弦重排，而不是默认对所有文档做全量精确比对
- 不带元数据过滤
- 如果当前内存索引处于失效状态，则自动回退到精确扫描

### SearchExact

```go
func (s *Store) SearchExact(query []float32, limit int) ([]SearchResult, error)
```

显式精确检索入口。

等价于：

```go
store.FindSimilar(query, vecdb.SearchOptions{
	Limit: limit,
	Exact: true,
})
```

适合场景：

- 结果正确性优先
- 做效果对照或基准测试
- 数据量较小

### FindSimilar

```go
func (s *Store) FindSimilar(query []float32, options SearchOptions) ([]SearchResult, error)
```

最完整的检索入口。

支持能力：

- 近似检索
- 精确检索
- 最小分数阈值
- 元数据过滤
- 候选数调节

推荐用法：

近似检索：

```go
results, err := store.FindSimilar(queryEmbedding, vecdb.SearchOptions{
	Limit:      10,
	Candidates: 100,
	MinScore:   0.6,
})
```

精确检索：

```go
results, err := store.FindSimilar(queryEmbedding, vecdb.SearchOptions{
	Limit: 10,
	Exact: true,
})
```

带过滤检索：

```go
results, err := store.FindSimilar(queryEmbedding, vecdb.SearchOptions{
	Limit: 5,
	Filter: vecdb.Metadata{
		"topic": "go",
		"lang":  "zh",
	},
})
```

过滤行为说明：

- `Filter` 非空时，不走 HNSW 近似候选召回
- 先通过持久化 metadata postings 索引求交集，得到满足过滤条件的候选文档 ID
- 再只对这些候选文档做精确余弦计算和 top-k 排序
- 因此过滤结果保持精确，同时避免无条件全表扫描

## 记忆系统接口

记忆系统是构建在 `Store` 之上的一个轻量包装层，适合实现“短期 / 会话 / 长期”记忆。

建议：

- 为记忆系统使用独立的数据库文件
- 不要在 `MemoryStore` 管理的底层库里混放普通 `Document`
- 定期调用 `PruneExpired` 清理过期记忆

过期语义：

- 记忆到达 `ExpiresAt` 后，会被视为“逻辑过期”
- 逻辑过期不会自动触发物理删除；底层记录会继续保留，直到显式调用 `PruneExpired`
- `Recall` 默认不会返回已过期记忆
- 只有当 `RecallOptions.IncludeExpired = true` 时，`Recall` 才会返回已过期记忆
- `PruneExpired` 会同时删除过期记忆的向量记录和正文内容

### OpenMemory

```go
func OpenMemory(path string, dimension int) (*MemoryStore, error)
```

使用默认生命周期策略打开一个记忆库。

默认策略：

- `short_term`：24 小时
- `session`：7 天
- `long_term`：不过期

### OpenMemoryWithOptions

```go
func OpenMemoryWithOptions(options MemoryStoreOptions) (*MemoryStore, error)
```

使用自定义底层 `Store` 配置和 TTL 打开记忆库。

### Remember

```go
func (m *MemoryStore) Remember(memory Memory) error
```

写入或覆盖一条记忆。

### RememberMany

```go
func (m *MemoryStore) RememberMany(memories []Memory) error
```

批量写入记忆。

### Get

```go
func (m *MemoryStore) Get(id string) (Memory, error)
```

按 ID 读取完整记忆。

### Recall

```go
func (m *MemoryStore) Recall(query []float32, options RecallOptions) ([]MemoryMatch, error)
```

按向量召回记忆，并自动处理生命周期过滤。

行为说明：

- 默认不会返回已过期记忆
- 底层已过期记录仍会保留，直到显式调用 `PruneExpired`
- 当 `Level` 非空时，会先按级别过滤
- 当 `Filter` 非空时，会继续按用户元数据做精确过滤
- 当 `IncludeExpired=true` 时，允许返回已过期记忆
- 如果过滤或过期剔除导致结果不足，内部会自动扩大抓取范围，尽量补足 `Limit`

### Forget

```go
func (m *MemoryStore) Forget(id string) error
```

删除一条记忆。

### PruneExpired

```go
func (m *MemoryStore) PruneExpired() (int, error)
```

清理所有已过期记忆，返回删除数量。

补充说明：

- 该方法执行的是物理删除
- 会同时删除过期记忆对应的向量记录和正文内容

### 代理接口

记忆系统还提供以下透传方法：

```go
func (m *MemoryStore) Count() (int, error)
func (m *MemoryStore) Dimension() int
func (m *MemoryStore) RefreshIndex() error
func (m *MemoryStore) Close() error
```

## 错误约定

库公开了以下错误值：

```go
var (
	ErrClosed
	ErrInvalidDimension
	ErrDocumentIDRequired
	ErrLimitRequired
	ErrZeroVector
	ErrInvalidVector
	ErrNotFound
	ErrInvalidMemoryLevel
	ErrMemoryContentRequired
	ErrReservedMemoryMetadataKey
	ErrInvalidMemoryRecord
)
```

含义如下：

- `ErrClosed`：存储实例已关闭
- `ErrInvalidDimension`：打开存储实例时维度非法
- `ErrDocumentIDRequired`：文档 ID 为空
- `ErrLimitRequired`：检索结果上限非法
- `ErrZeroVector`：查询向量范数为 0，无法计算余弦相似度
- `ErrInvalidVector`：向量中包含 `NaN` 或 `Inf`
- `ErrNotFound`：文档不存在
- `ErrInvalidMemoryLevel`：记忆生命周期级别非法
- `ErrMemoryContentRequired`：记忆内容为空
- `ErrReservedMemoryMetadataKey`：用户元数据使用了记忆系统保留字段
- `ErrInvalidMemoryRecord`：底层记录无法按记忆格式解码

此外，接口还可能返回被包装的底层错误，例如：

- bbolt 的 I/O 错误
- JSON 编解码错误
- 向量解码错误

建议调用方使用 `errors.Is` 判断这些公开错误值。

## 并发与一致性语义

当前实现支持多 goroutine 并发使用。

保证如下：

- 读操作共享同一把读锁
- 写操作和 `Close` 使用同一把写锁
- 同一次检索中，候选向量与返回的元数据来自同一个一致视图
- `Close` 不会与其他公开方法发生生命周期竞态

当前实现更偏向“正确性优先”，因此：

- 覆盖写和删除不会立即重建索引，而是先标记索引失效
- 索引失效期间，默认近似检索会安全回退到精确扫描
- 调用 `RefreshIndex` 或 `Close` 可以恢复并持久化最新索引

## 性能建议

- 如果你的场景写多读少，并且经常覆盖同一 ID，当前实现不是最优形态
- 如果你的场景是读多写少，或先批量导入再集中检索，当前实现更合适
- 当前版本已经支持 HNSW sidecar 文件复用，适合降低重启成本
- 如果你需要更复杂的过滤表达式，可以在 `Filter` 基础上继续扩展

## 当前限制

- 元数据只支持字符串键值
- 元数据过滤只支持精确匹配
- 带过滤的检索当前走精确扫描
- 覆盖写和删除当前不会立即恢复 ANN 性能，需要显式刷新或等待关闭时持久化
- 还没有分页、游标和更多排序策略扩展

## 参考文件

- 示例程序见 `cmd/demo/main.go`
- 基础说明见 `README.md`
- 行为测试见 `store_test.go`
- 基准测试见 `store_benchmark_test.go`
