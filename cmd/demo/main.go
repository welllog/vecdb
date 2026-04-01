package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/welllog/vecdb"
)

func main() {
	path := "./demo.vecdb"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}

	store, err := vecdb.OpenWithOptions(vecdb.Options{
		Path:      path,
		Dimension: 4,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	err = store.UpsertMany([]vecdb.Document{
		{ID: "doc-1", Vector: []float32{0.91, 0.07, 0.01, 0.01}, Metadata: vecdb.Metadata{"title": "golang embedding intro", "topic": "go"}},
		{ID: "doc-2", Vector: []float32{0.85, 0.11, 0.03, 0.01}, Metadata: vecdb.Metadata{"title": "vector search basics", "topic": "search"}},
		{ID: "doc-3", Vector: []float32{0.10, 0.10, 0.70, 0.10}, Metadata: vecdb.Metadata{"title": "sqlite persistence", "topic": "db"}},
	})
	if err != nil {
		log.Fatal(err)
	}

	approximate, err := store.FindSimilar([]float32{0.88, 0.10, 0.01, 0.01}, vecdb.SearchOptions{
		Limit:    2,
		MinScore: 0.50,
	})
	if err != nil {
		log.Fatal(err)
	}

	filteredExact, err := store.FindSimilar([]float32{0.88, 0.10, 0.01, 0.01}, vecdb.SearchOptions{
		Limit:  2,
		Exact:  true,
		Filter: vecdb.Metadata{"topic": "go"},
	})
	if err != nil {
		log.Fatal(err)
	}

	output, err := json.MarshalIndent(map[string]any{
		"approximate":    approximate,
		"filtered_exact": filteredExact,
	}, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(output))
}
