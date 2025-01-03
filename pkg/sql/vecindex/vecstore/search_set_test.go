// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSearchResultHeap(t *testing.T) {
	// Note that smaller distances are actually "less than" higher distances,
	// since results need to be sorted in reverse order by the heap.
	results := searchResultHeap{
		SearchResult{
			QuerySquaredDistance: 1,
			ErrorBound:           0.5,
			CentroidDistance:     10,
			ParentPartitionKey:   100,
			ChildKey:             ChildKey{PrimaryKey: []byte{10}},
		},
		SearchResult{
			QuerySquaredDistance: 2,
			ErrorBound:           0,
			CentroidDistance:     20,
			ParentPartitionKey:   200,
			ChildKey:             ChildKey{PrimaryKey: []byte{30}},
		},
		SearchResult{
			QuerySquaredDistance: 2,
			ErrorBound:           1,
			CentroidDistance:     20,
			ParentPartitionKey:   200,
			ChildKey:             ChildKey{PrimaryKey: []byte{20}},
		},
		SearchResult{
			QuerySquaredDistance: 2,
			ErrorBound:           1,
			CentroidDistance:     30,
			ParentPartitionKey:   300,
			ChildKey:             ChildKey{PrimaryKey: []byte{40}},
		},
	}

	require.True(t, results.Less(1, 0))
	require.False(t, results.Less(0, 1))
	require.True(t, results.Less(1, 2))
	require.False(t, results.Less(2, 1))

	// Equal case.
	require.False(t, results.Less(2, 3))
	require.False(t, results.Less(3, 2))
}

func TestSearchStats(t *testing.T) {
	var stats SearchStats
	stats.SearchedPartition(LeafLevel, 10)
	stats.SearchedPartition(LeafLevel+1, 10)
	stats.Add(&SearchStats{
		PartitionCount:           3,
		QuantizedVectorCount:     30,
		QuantizedLeafVectorCount: 15,
		FullVectorCount:          5,
	})
	require.Equal(t, 5, stats.PartitionCount)
	require.Equal(t, 50, stats.QuantizedVectorCount)
	require.Equal(t, 25, stats.QuantizedLeafVectorCount)
	require.Equal(t, 5, stats.FullVectorCount)
}

func TestSearchSet(t *testing.T) {
	// Empty.
	searchSet := SearchSet{MaxResults: 3, MaxExtraResults: 7}
	require.Nil(t, searchSet.PopResults())
	require.Nil(t, searchSet.PopExtraResults())

	// Exceed max results, outside of error bounds.
	result1 := SearchResult{
		QuerySquaredDistance: 3, ErrorBound: 0.5, CentroidDistance: 10, ParentPartitionKey: 100, ChildKey: ChildKey{PrimaryKey: []byte{10}}}
	result2 := SearchResult{
		QuerySquaredDistance: 6, ErrorBound: 1, CentroidDistance: 20, ParentPartitionKey: 200, ChildKey: ChildKey{PrimaryKey: []byte{20}}}
	result3 := SearchResult{
		QuerySquaredDistance: 1, ErrorBound: 0.5, CentroidDistance: 30, ParentPartitionKey: 300, ChildKey: ChildKey{PrimaryKey: []byte{30}}}
	result4 := SearchResult{
		QuerySquaredDistance: 4, ErrorBound: 0.5, CentroidDistance: 40, ParentPartitionKey: 400, ChildKey: ChildKey{PrimaryKey: []byte{40}}}
	searchSet.Add(&result1)
	searchSet.Add(&result2)
	searchSet.Add(&result3)
	searchSet.Add(&result4)
	require.Equal(t, []SearchResult{result3, result1, result4}, searchSet.PopResults())
	require.Nil(t, searchSet.PopExtraResults())

	// Exceed max results, but within error bounds.
	result5 := SearchResult{
		QuerySquaredDistance: 6, ErrorBound: 1.5, CentroidDistance: 50, ParentPartitionKey: 500, ChildKey: ChildKey{PrimaryKey: []byte{50}}}
	result6 := SearchResult{
		QuerySquaredDistance: 5, ErrorBound: 1, CentroidDistance: 60, ParentPartitionKey: 600, ChildKey: ChildKey{PrimaryKey: []byte{60}}}
	searchSet.AddAll([]SearchResult{result1, result2, result3, result4, result5, result6})
	require.Equal(t, []SearchResult{result3, result1, result4}, searchSet.PopResults())
	require.Equal(t, []SearchResult{result6, result5}, searchSet.PopExtraResults())

	// Don't allow extra results.
	otherSet := SearchSet{MaxResults: 3}
	otherSet.AddAll([]SearchResult{result1, result2, result3, result4, result5, result6})
	require.Equal(t, []SearchResult{result3, result1, result4}, otherSet.PopResults())
	require.Nil(t, otherSet.PopExtraResults())

	// Add better results that invalidate farther candidates.
	result7 := SearchResult{
		QuerySquaredDistance: 4, ErrorBound: 1.5, CentroidDistance: 70, ParentPartitionKey: 700, ChildKey: ChildKey{PrimaryKey: []byte{70}}}
	searchSet.AddAll([]SearchResult{result1, result2, result3, result4, result5, result6, result7})
	require.Equal(t, []SearchResult{result3, result1, result7}, searchSet.PopResults())
	require.Equal(t, []SearchResult{result4, result6, result5}, searchSet.PopExtraResults())

	result8 := SearchResult{
		QuerySquaredDistance: 0.5, ErrorBound: 0.5, CentroidDistance: 80, ParentPartitionKey: 800, ChildKey: ChildKey{PrimaryKey: []byte{80}}}
	searchSet.AddAll([]SearchResult{result1, result2, result3, result4})
	searchSet.AddAll([]SearchResult{result5, result6, result7, result8})
	require.Equal(t, []SearchResult{result8, result3, result1}, searchSet.PopResults())
	require.Equal(t, []SearchResult{result7, result4}, searchSet.PopExtraResults())

	// Allow one extra result.
	otherSet.MaxExtraResults = 1
	otherSet.AddAll([]SearchResult{result1, result2, result3, result4, result5, result6, result7})
	require.Equal(t, []SearchResult{result3, result1, result7}, otherSet.PopResults())
	require.Equal(t, []SearchResult{result4}, otherSet.PopExtraResults())
}
