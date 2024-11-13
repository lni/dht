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
	"net"
	"sort"
	"time"

	"github.com/elliotchance/orderedmap/v2"
)

const (
	// a small K is used to reflect the fact that this is not a global service
	DefaultK int = 12
	// a small number of bits is used to reflect the fact that the network size
	// is much smaller
	DefaultBits int = 128
	// a stale remote need to be pinged
	staleThreshold = 180 * time.Second
	// when a remote is not seen in deadThreshold, it is dead and will be
	// removed during the next gc
	deadThreshold = 480 * time.Second
)

type remoteRecord struct {
	remote
	lastSeen time.Time
}

func (r remoteRecord) toRemote() remote {
	return r.remote
}

type kBucket struct {
	k int
	// up to 16 remotes in each bucket
	buckets *orderedmap.OrderedMap[nodeID, remoteRecord]
}

func newBucket(k int) *kBucket {
	return &kBucket{
		k:       k,
		buckets: orderedmap.NewOrderedMap[nodeID, remoteRecord](),
	}
}

func (b *kBucket) Len() int {
	return b.buckets.Len()
}

func (b *kBucket) Observe(nodeID nodeID, address net.UDPAddr) {
	rec := remoteRecord{
		remote: remote{
			NodeID:  nodeID,
			Address: address,
		},
		lastSeen: time.Now(),
	}
	sz := b.buckets.Len()
	if sz < b.k {
		b.buckets.Set(nodeID, rec)
		return
	} else if sz == b.k {
		if _, ok := b.buckets.Get(nodeID); ok {
			b.buckets.Set(nodeID, rec)
		} else {
			if el := b.buckets.Front(); el != nil {
				b.buckets.Delete(el.Key)
				b.buckets.Set(nodeID, rec)
			} else {
				panic("el == nil")
			}
		}
	} else {
		panic("more than k elements in the bucket")
	}
}

func (b *kBucket) CopyToList(l []remote) []remote {
	for el := b.buckets.Front(); el != nil; el = el.Next() {
		l = append(l, el.Value.toRemote())
	}

	return l
}

type routingTable struct {
	k       int
	bits    int
	nodeID  nodeID
	address net.UDPAddr
	empty   []nodeID
	stale   []remote
	// 128 buckets
	// 0 based indexes here, remotes with i common bits with r.nodeID are
	// placed in the i-th kbucket
	buckets []*kBucket
}

func newRoutingTable(k int,
	bits int, selfID nodeID, addr net.UDPAddr) *routingTable {
	rt := &routingTable{
		k:       k,
		bits:    bits,
		nodeID:  selfID,
		address: addr,
		empty:   make([]nodeID, 0, bits),
		stale:   make([]remote, 0, k*bits),
		buckets: make([]*kBucket, bits),
	}
	for i := 0; i < bits; i++ {
		rt.buckets[i] = newBucket(k)
	}

	return rt
}

func (r *routingTable) Observe(nodeID nodeID, address net.UDPAddr) {
	prefixLen := r.nodeID.CommonPrefixLength(nodeID)
	if prefixLen == r.bits {
		// same id, nothing to do
		return
	}
	b := r.buckets[prefixLen]
	b.Observe(nodeID, address)
}

// Empty buckets need to be filled by invoking find_node on nodeID values
// from those buckets.
func (r *routingTable) InterestedNodes() []nodeID {
	empty := r.empty[:]
	for i := 0; i < r.bits; i++ {
		if b := r.buckets[i]; b.Len() == 0 {
			v := r.getRandomeInterestedNodeID(i)
			empty = append(r.empty, v)
		}
	}

	return empty
}

// Stale remotes need to be pinged.
func (r *routingTable) GetStaleRemote() []remote {
	stale := r.stale[:]
	now := time.Now()
	for _, b := range r.buckets {
		for el := b.buckets.Front(); el != nil; el = el.Next() {
			rec := el.Value
			if now.Sub(rec.lastSeen) > staleThreshold {
				stale = append(stale, rec.toRemote())
			}
		}
	}

	return stale
}

func (r *routingTable) GC() {
	now := time.Now()
	dead := make([]nodeID, 0)
	for _, b := range r.buckets {
		toRemove := dead[:]
		for el := b.buckets.Front(); el != nil; el = el.Next() {
			rec := el.Value
			if now.Sub(rec.lastSeen) > deadThreshold {
				toRemove = append(toRemove, rec.NodeID)
			}
		}
		for _, nodeID := range toRemove {
			b.buckets.Delete(nodeID)
		}
	}
}

func (r *routingTable) KNearest(target nodeID) []remote {
	if target.IsEmpty() {
		panic("empty target")
	}

	var selected []remote
	prefixLen := r.nodeID.CommonPrefixLength(target)
	if prefixLen == r.bits {
		// target == r.nodeID, nothing to do
		return nil
	}
	// find all candidates
	b := r.buckets[prefixLen]
	selected = b.CopyToList(selected)
	// we can't return selected here even if there are k elements in that
	// bucket. there is just no guarantee that those k are the nearest
	// pull at least k elements from both sides
	i := prefixLen - 1
	added := 0
	for i >= 0 && added < r.k {
		cur := r.buckets[i]
		added += cur.Len()
		selected = cur.CopyToList(selected)
		i--
	}

	j := prefixLen + 1
	added = 0
	for j < len(r.buckets) && added < r.k {
		cur := r.buckets[j]
		added += cur.Len()
		selected = cur.CopyToList(selected)
		j++
	}
	// always include own remote before sorting
	selected = append(selected, r.self())

	selected = sortByDistance(selected, target)
	// less k elements in the routing table
	if len(selected) <= r.k {
		return selected
	}

	return selected[:r.k]
}

func (r *routingTable) self() remote {
	return remote{r.nodeID, r.address}
}

func sortByDistance(selected []remote, target nodeID) []remote {
	// sort them in asc order based their distances to target
	sort.Slice(selected, func(x, y int) bool {
		var dx key
		var dy key
		dx.Distance(selected[x].NodeID, target)
		dy.Distance(selected[y].NodeID, target)
		return dx.Less(dy)
	})

	return selected
}

func flipNthBitFromLeft(value uint64, n int) uint64 {
	if n < 1 || n > 64 {
		panic("invalid n")
	}
	pos := 64 - n
	mask := uint64(1) << pos
	return value ^ mask
}

// when compared to the r.nodeID, the returned value should have the same
// prefixLen common prefix bits, the prefixLen + 1 bit should be reversed while
// all other bits are random. in the current implementation, we just flip the
// (prefixLen+1)-th bit
func (r *routingTable) getRandomeInterestedNodeID(prefixLen int) nodeID {
	result := r.nodeID
	if prefixLen <= 63 {
		result.High = flipNthBitFromLeft(result.High, prefixLen+1)
	} else {
		result.Low = flipNthBitFromLeft(result.Low, prefixLen-64+1)
	}

	return result
}
