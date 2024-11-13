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
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func getTestDHTNetwork(t *testing.T) (*DHT, *DHT, func()) {
	cfg1 := Config{
		Proto: "udp4",
		Address: net.UDPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 39000,
		},
	}
	cfg2 := Config{
		Proto:   "udp4",
		Routers: []net.UDPAddr{cfg1.Address},
		Address: net.UDPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 39001,
		},
	}

	logger := zap.Must(zap.NewProduction())

	dht1, err := NewDHT(cfg1, logger)
	require.NoError(t, err)
	dht2, err := NewDHT(cfg2, logger)
	require.NoError(t, err)

	require.NoError(t, dht1.Start())
	require.NoError(t, dht2.Start())

	cleanup := func() {
		dht1.Close()
		dht2.Close()
	}

	return dht1, dht2, cleanup
}

func TestDHTPutAndGet(t *testing.T) {
	dht1, dht2, cleanup := getTestDHTNetwork(t)
	defer cleanup()

	time.Sleep(250 * time.Millisecond)
	key := getRandomNodeID()
	value := []byte("this is a test")
	dht1.Put(key, value, 100)
	time.Sleep(500 * time.Millisecond)
	dht2.Get(key)
	time.Sleep(500 * time.Millisecond)
	values := dht2.GetCached(key)
	require.Equal(t, 1, len(values))
	require.Equal(t, value, values[0])
}

func TestDHTPutTTL(t *testing.T) {
	dht1, dht2, cleanup := getTestDHTNetwork(t)
	defer cleanup()

	time.Sleep(250 * time.Millisecond)
	key := getRandomNodeID()
	value := []byte("this is a test")
	dht1.Put(key, value, 0)
	time.Sleep(500 * time.Millisecond)
	dht1.store.GC()
	dht2.store.GC()
	dht2.Get(key)
	time.Sleep(500 * time.Millisecond)
	values := dht2.GetCached(key)
	require.Equal(t, 0, len(values))
}

func TestDHTSecret(t *testing.T) {
	logger := zap.Must(zap.NewProduction())
	cfg := Config{
		Secret: 1,
		Proto:  "udp4",
		Address: net.UDPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 39000,
		},
	}
	router, err := NewDHT(cfg, logger)
	require.NoError(t, err)
	require.NoError(t, router.Start())
	defer router.Close()

	network := make([]*DHT, 0)
	for i := 0; i < 32; i++ {
		cfg := Config{
			Secret:  uint16(100 + i),
			Proto:   "udp4",
			Routers: []net.UDPAddr{router.cfg.Address},
			Address: net.UDPAddr{
				IP:   net.ParseIP("127.0.0.1"),
				Port: 39001 + i,
			},
		}
		dht, err := NewDHT(cfg, logger)
		require.NoError(t, err)
		require.NoError(t, dht.Start())

		network = append(network, dht)
	}
	defer func() {
		for _, dht := range network {
			assert.NoError(t, dht.Close())
		}
	}()

	time.Sleep(1200 * time.Millisecond)
	key := getRandomNodeID()
	value := []byte("this is a test")
	router.Put(key, value, 100)
	time.Sleep(100 * time.Millisecond)
	for _, dht := range network {
		dht.Get(key)
	}
	time.Sleep(200 * time.Millisecond)
	ok := 0
	for _, dht := range network {
		values := dht.GetCached(key)
		if len(values) == 1 && bytes.Equal(value, values[0]) {
			ok++
		}
	}
	t.Logf("ok: %d\n", ok)
	assert.Equal(t, 0, ok)
}

func TestSmallDHTNetwork(t *testing.T) {
	logger := zap.Must(zap.NewProduction())
	cfg := Config{
		Proto: "udp4",
		Address: net.UDPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 39000,
		},
	}
	router, err := NewDHT(cfg, logger)
	require.NoError(t, err)
	require.NoError(t, router.Start())
	defer router.Close()

	network := make([]*DHT, 0)
	for i := 0; i < 32; i++ {
		cfg := Config{
			Proto:   "udp4",
			Routers: []net.UDPAddr{router.cfg.Address},
			Address: net.UDPAddr{
				IP:   net.ParseIP("127.0.0.1"),
				Port: 39001 + i,
			},
		}
		dht, err := NewDHT(cfg, logger)
		require.NoError(t, err)
		require.NoError(t, dht.Start())

		network = append(network, dht)
	}
	defer func() {
		for _, dht := range network {
			assert.NoError(t, dht.Close())
		}
	}()

	time.Sleep(1200 * time.Millisecond)
	key := getRandomNodeID()
	value := []byte("this is a test")
	router.Put(key, value, 100)
	time.Sleep(100 * time.Millisecond)
	for _, dht := range network {
		dht.Get(key)
	}
	time.Sleep(200 * time.Millisecond)
	ok := 0
	for _, dht := range network {
		values := dht.GetCached(key)
		if len(values) == 1 && bytes.Equal(value, values[0]) {
			ok++
		}
	}
	t.Logf("ok: %d\n", ok)
	assert.True(t, ok >= 28)
}

func TestSmallDHTNetworkWithFailedNodes(t *testing.T) {
	logger := zap.Must(zap.NewProduction())

	cfg := Config{
		Proto: "udp4",
		Address: net.UDPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 39000,
		},
	}
	router, err := NewDHT(cfg, logger)
	require.NoError(t, err)
	require.NoError(t, router.Start())
	defer router.Close()

	network := make([]*DHT, 0)
	for i := 0; i < 32; i++ {
		cfg := Config{
			Proto:   "udp4",
			Routers: []net.UDPAddr{router.cfg.Address},
			Address: net.UDPAddr{
				IP:   net.ParseIP("127.0.0.1"),
				Port: 39001 + i,
			},
		}
		dht, err := NewDHT(cfg, logger)
		require.NoError(t, err)
		require.NoError(t, dht.Start())

		network = append(network, dht)
	}
	time.Sleep(300 * time.Millisecond)

	// stop 1/4 of all nodes before Put()
	for idx, dht := range network {
		if idx%4 == 0 {
			assert.NoError(t, dht.Close())
			network[idx] = nil
		}
	}

	defer func() {
		for _, dht := range network {
			if dht != nil {
				assert.NoError(t, dht.Close())
			}
		}
	}()

	time.Sleep(1200 * time.Millisecond)
	key := getRandomNodeID()
	value := []byte("this is a test")
	router.Put(key, value, 100)
	time.Sleep(100 * time.Millisecond)

	// stop another 1/4 of all nodes before Get()
	for idx, dht := range network {
		if idx%4 == 1 {
			require.NotNil(t, dht)
			assert.NoError(t, dht.Close())
			network[idx] = nil
		}
	}

	for _, dht := range network {
		if dht != nil {
			dht.Get(key)
		}
	}
	time.Sleep(200 * time.Millisecond)
	ok := 0
	for _, dht := range network {
		if dht != nil {
			values := dht.GetCached(key)
			if len(values) == 1 && bytes.Equal(value, values[0]) {
				ok++
			}
		}
	}
	t.Logf("ok: %d\n", ok)
	assert.True(t, ok >= 12)
}
