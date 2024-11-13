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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTo4KBatches(t *testing.T) {
	values := make([][]byte, 0)
	for i := 0; i < 16; i++ {
		values = append(values, make([]byte, 1024))
	}
	result := to4KBatches(values)
	assert.Equal(t, 8, len(result))
	for _, batch := range result {
		assert.Equal(t, 2, len(batch))
		assert.Equal(t, 1024, len(batch[0]))
		assert.Equal(t, 1024, len(batch[1]))
	}
}

func TestKVStorePutAndGet(t *testing.T) {
	store := newKVStore()
	key := getRandomNodeID()
	store.Put(key, []byte("hello"), 100)
	store.Put(key, []byte("world"), 100)
	result, ok := store.Get(key)
	require.True(t, ok)
	assert.Equal(t, 2, len(result))

	for _, v := range result {
		assert.True(t, string(v) == "hello" || string(v) == "world")
	}
}

func TestKVStoreAndGetWithDuplicatedInput(t *testing.T) {
	store := newKVStore()
	key := getRandomNodeID()
	store.Put(key, []byte("hello"), 100)
	store.Put(key, []byte("hello"), 100)
	result, ok := store.Get(key)
	require.True(t, ok)
	assert.Equal(t, 1, len(result))

	for _, v := range result {
		assert.True(t, string(v) == "hello")
	}
}

func TestKVStoreGC(t *testing.T) {
	store := newKVStore()
	key := getRandomNodeID()
	now := time.Now()
	store.Put(key, []byte("hello"), 100)
	store.Put(key, []byte("world"), 200)

	// ttl determined by the second put
	rec, ok := store.data[key]
	require.True(t, ok)
	tt := now.Add(time.Duration(90) * time.Second)
	assert.True(t, rec.ttl.After(tt))

	key2 := getRandomNodeID()
	store.Put(key2, []byte("test"), 100)
	rec, ok = store.data[key2]
	require.True(t, ok)
	rec.ttl = time.Now()
	store.GC()
	// key2 will be gone
	_, ok = store.data[key2]
	require.False(t, ok)
}
