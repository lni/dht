// Copyright 2024 Lei Ni (nilei81@gmail.com)
//
// This library follows a dual licensing model -
//
// - it is licensed under the 2-clause BSD license if you have written evidence showing that you are a licensee of github.com/lni/pothos
// - otherwise, it is licensed under the GPL-2 license
//
// See the LICENSE file for details

package dht

import (
	"crypto/rand"
	"encoding/binary"
	"hash"
	"sync"
	"time"

	"github.com/minio/highwayhash"
)

var (
	codec = binary.BigEndian
)

func to4KBatches(values [][]byte) [][][]byte {
	results := make([][][]byte, 0)
	total := 0
	current := make([][]byte, 0)
	for _, value := range values {
		if total+len(value) >= 3*1024 {
			results = append(results, current)
			current = make([][]byte, 0)
			total = 0
		}
		current = append(current, value)
		total += len(value)
	}

	if len(current) > 0 {
		results = append(results, current)
	}

	return results
}

type checksum struct {
	v1 uint64
	v2 uint64
	v3 uint64
	v4 uint64
}

type stored struct {
	ttl      time.Time
	values   [][]byte
	included map[checksum]struct{}
}

// kvStore is a in-memory key-value store
type kvStore struct {
	mu   sync.Mutex
	hash hash.Hash
	data map[key]*stored
}

func newKVStore() *kvStore {
	highwayKey := make([]byte, 32)
	if _, err := rand.Read(highwayKey); err != nil {
		panic(err)
	}
	h, err := highwayhash.New(highwayKey)
	if err != nil {
		panic(err)
	}

	return &kvStore{
		hash: h,
		data: make(map[key]*stored),
	}
}

func (s *kvStore) Put(k key, v []byte, ttlInSeconds uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ttl := time.Now().Add(time.Duration(ttlInSeconds) * time.Second)
	rec, ok := s.data[k]
	if !ok {
		rec = &stored{
			values:   make([][]byte, 0),
			included: make(map[checksum]struct{}),
		}
		s.data[k] = rec
	}
	// to dedupl
	cs := s.getChecksum(v)
	if _, ok := rec.included[cs]; ok {
		return
	}
	rec.ttl = ttl
	rec.values = append(rec.values, v)
	rec.included[cs] = struct{}{}
}

func (s *kvStore) Get(k key) ([][]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.data[k]
	if !ok {
		return nil, false
	}
	return rec.values, true
}

func (s *kvStore) GC() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, stored := range s.data {
		if stored.ttl.Before(now) {
			delete(s.data, key)
		}
	}
}

func (s *kvStore) getChecksum(v []byte) checksum {
	s.hash.Reset()
	if _, err := s.hash.Write(v); err != nil {
		panic(err)
	}
	c := s.hash.Sum(nil)
	if len(c) != 32 {
		panic("unexpected checksum length")
	}

	return checksum{
		v1: codec.Uint64(c),
		v2: codec.Uint64(c[8:]),
		v3: codec.Uint64(c[16:]),
		v4: codec.Uint64(c[24:]),
	}
}
