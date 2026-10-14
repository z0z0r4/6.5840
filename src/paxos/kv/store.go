// Package kv implements a key/value state machine without consensus or networking.
package kv

import (
	"encoding/json"
	"fmt"
	"maps"
)

// Storage is independent of networking and consensus. Kind identifies the
// snapshot format, and Restore must validate before replacing existing data.
type Storage interface {
	Kind() string
	Get(key string) string
	Set(key, value string)
	Snapshot() string
	Restore(snapshot string) error
}

// Store is the in-memory implementation of Storage. Its owner provides locking.
type Store struct{ values map[string]string }

func New() *Store                          { return &Store{values: make(map[string]string)} }
func (s *Store) Kind() string              { return "json/v1" }
func (s *Store) Set(key, value string)     { s.values[key] = value }
func (s *Store) Get(key string) string     { return s.values[key] }
func (s *Store) Values() map[string]string { return maps.Clone(s.values) }
func (s *Store) Snapshot() string          { data, _ := json.Marshal(s.values); return string(data) }
func (s *Store) Restore(snapshot string) error {
	var values map[string]string
	if err := json.Unmarshal([]byte(snapshot), &values); err != nil {
		return fmt.Errorf("invalid KV snapshot: %w", err)
	}
	if values == nil {
		return fmt.Errorf("KV snapshot must contain an object")
	}
	s.values = values
	return nil
}
