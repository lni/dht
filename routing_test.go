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
	"math/rand"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getTestAddress() net.UDPAddr {
	return net.UDPAddr{
		Port: 12345,
	}
}

func TestFlipNthBitFromLeft(t *testing.T) {
	tests := []struct {
		value  uint64
		n      int
		result uint64
	}{
		{0, 1, 0x8000000000000000},
		{0x8000000000000000, 1, 0},
		{3, 63, 1},
		{1, 64, 0},
		{0, 64, 1},
	}

	for idx, tt := range tests {
		result := flipNthBitFromLeft(tt.value, tt.n)
		assert.Equal(t, tt.result, result, idx)
	}
}

func TestKBucketRemoteCanBeSet(t *testing.T) {
	b := newBucket(2)
	assert.Equal(t, 0, b.Len())
	b.Observe(getRandomNodeID(), getTestAddress())
	assert.Equal(t, 1, b.Len())
	b.Observe(getRandomNodeID(), getTestAddress())
	assert.Equal(t, 2, b.Len())
	b.Observe(getRandomNodeID(), getTestAddress())
	assert.Equal(t, 2, b.Len())
	b.Observe(getRandomNodeID(), getTestAddress())
	assert.Equal(t, 2, b.Len())
}

func TestKBucketRemoteCanBeReplaced(t *testing.T) {
	b := newBucket(2)
	assert.Equal(t, 0, b.Len())

	id1 := getRandomNodeID()
	id2 := getRandomNodeID()
	b.Observe(id1, net.UDPAddr{Port: 100})
	b.Observe(id2, net.UDPAddr{Port: 200})
	b.Observe(id2, net.UDPAddr{Port: 300})
	assert.Equal(t, 2, b.Len())

	v, ok := b.buckets.Get(id2)
	assert.True(t, ok)
	assert.Equal(t, 300, v.Address.Port)
}

func TestStaleKBucketRemoteCanBeReplaced(t *testing.T) {
	b := newBucket(2)
	assert.Equal(t, 0, b.Len())

	id1 := getRandomNodeID()
	id2 := getRandomNodeID()
	id3 := getRandomNodeID()
	b.Observe(id1, net.UDPAddr{Port: 100})
	b.Observe(id2, net.UDPAddr{Port: 200})
	b.Observe(id3, net.UDPAddr{Port: 300})
	assert.Equal(t, 2, b.Len())

	_, ok := b.buckets.Get(id1)
	assert.False(t, ok)
}

func TestSortByDistance(t *testing.T) {
	target := getRandomNodeID()
	selected := make([]remote, 0)
	for i := 0; i < 1000; i++ {
		selected = append(selected, remote{NodeID: getRandomNodeID()})
	}
	selected = sortByDistance(selected, target)

	for i := range selected {
		remote := selected[i]
		for j := i + 1; j < len(selected); j++ {
			other := selected[j]
			var rt key
			var ot key

			rt.Distance(remote.NodeID, target)
			ot.Distance(other.NodeID, target)
			assert.True(t, rt.Less(ot))
		}
	}
}

// This test itself is not super exciting as it basically requires
// the KNearest() method to pick the right bucket.
func TestKNearestCanReturnAKnownNodeID(t *testing.T) {
	for i := 0; i < 100; i++ {
		myID := getRandomNodeID()
		table := newRoutingTable(DefaultK, DefaultBits, myID, net.UDPAddr{})

		port := 0
		for i := 0; i < DefaultK*DefaultBits; i++ {
			table.Observe(getRandomNodeID(), net.UDPAddr{Port: port})
			port++
		}
		// get all those randomly generated remotes, have to do it after they've all
		// been observed as some of them will be overwritten by the bucket.
		remotes := make([]remote, 0)
		for _, b := range table.buckets {
			remotes = b.CopyToList(remotes)
		}
		if len(remotes) < DefaultK {
			continue
		}
		// randomly pick a remote from the kbucket
		idx := rand.Uint64() % uint64(len(remotes))
		target := remotes[idx].NodeID

		located := false
		selected := table.KNearest(target)
		for _, r := range selected {
			if r.NodeID.Equal(target) {
				located = true
			}
		}
		assert.True(t, located)
	}
}

func TestInterestedNodes(t *testing.T) {
	for i := 0; i < 1000; i++ {
		myID := getRandomNodeID()
		table := newRoutingTable(DefaultK, DefaultBits, myID, net.UDPAddr{})
		emptyBucket := int(rand.Uint64() % uint64(DefaultBits))
		for i := 0; i < DefaultBits; i++ {
			if i == emptyBucket {
				continue
			}
			b := table.buckets[i]
			b.Observe(getRandomNodeID(), net.UDPAddr{})
		}

		interested := table.InterestedNodes()
		assert.Equal(t, 1, len(interested))

		idx := table.nodeID.CommonPrefixLength(interested[0])
		assert.Equal(t, emptyBucket, idx)
	}
}
