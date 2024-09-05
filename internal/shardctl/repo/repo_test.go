package repo_test

import (
	"testing"

	"raftkv/internal/shardctl/repo"
)

func Test_Join(t *testing.T) {

	repo := repo.NewConfigRepositery()
	repo.Join(map[int][]string{
		1: {"x", "y"},
		2: {"x", "y"},
	})
	repo.Join(map[int][]string{
		3: {"x", "y"},
	})
	repo.Leave([]int{
		1,
	})
	repo.Leave([]int{
		2, 3,
	})
	repo.Join(map[int][]string{
		3: {"x", "y"},
	})
	// repo.
}
