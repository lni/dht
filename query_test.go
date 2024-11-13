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

func TestQueryStatusAfterTimeout(t *testing.T) {
	q := newQuery(getRPCID(), getRandomNodeID(), nil)
	nid1 := getRandomNodeID()
	q.Request(nid1)
	assert.Equal(t, 1, q.Pending())

	rs, ok := q.Requested[nid1]
	require.True(t, ok)
	assert.False(t, rs.known())

	assert.True(t, q.OnTimeout(nid1))
	assert.True(t, rs.Timeout)
	assert.False(t, rs.Responded)

	assert.True(t, rs.known())
	assert.Equal(t, 0, q.Pending())
	assert.False(t, q.OnTimeout(nid1))
	assert.False(t, q.OnResponded(nid1))
	assert.True(t, rs.Timeout)
	assert.False(t, rs.Responded)
	assert.True(t, rs.known())
	assert.Equal(t, 0, q.Pending())
}

func TestQueryStatusAfterResponse(t *testing.T) {
	q := newQuery(getRPCID(), getRandomNodeID(), nil)
	nid1 := getRandomNodeID()
	q.Request(nid1)
	assert.Equal(t, 1, q.Pending())

	rs, ok := q.Requested[nid1]
	require.True(t, ok)
	assert.False(t, rs.known())

	assert.True(t, q.OnResponded(nid1))
	assert.False(t, rs.Timeout)
	assert.True(t, rs.Responded)

	assert.True(t, rs.known())
	assert.Equal(t, 0, q.Pending())
	assert.False(t, q.OnTimeout(nid1))
	assert.False(t, q.OnResponded(nid1))
	assert.False(t, rs.Timeout)
	assert.True(t, rs.Responded)
	assert.True(t, rs.known())
	assert.Equal(t, 0, q.Pending())
}

func TestQuryPendingRequestIsTracked(t *testing.T) {
	q := newQuery(getRPCID(), getRandomNodeID(), nil)
	require.Equal(t, 0, q.Pending())

	nid1 := getRandomNodeID()
	nid2 := getRandomNodeID()
	nid3 := getRandomNodeID()
	q.Request(nid1)
	q.Request(nid2)
	q.Request(nid3)
	assert.Equal(t, 3, q.Pending())

	q.OnTimeout(nid1)
	q.OnResponded(nid2)
	assert.Equal(t, 1, q.Pending())
	q.OnResponded(getRandomNodeID())
	assert.Equal(t, 1, q.Pending())
	q.OnResponded(nid3)
	assert.Equal(t, 0, q.Pending())
}

func TestQueryFilterRequestedNodes(t *testing.T) {
	q := newQuery(getRPCID(), getRandomNodeID(), nil)
	require.Equal(t, 0, q.Pending())

	nid1 := getRandomNodeID()
	nid2 := getRandomNodeID()
	q.Request(nid1)
	q.Request(nid2)

	r1 := remote{NodeID: nid1}
	r2 := remote{NodeID: nid2}
	r3 := remote{NodeID: getRandomNodeID()}

	result := q.Filter([]remote{r1, r2, r3})
	require.Equal(t, 1, len(result))
	assert.Equal(t, r3, result[0])
}

func TestQueryGC(t *testing.T) {
	q := newQueryManager()
	t1 := time.Now()
	t2 := time.Now().Add(-time.Hour)
	t3 := time.Now()

	q.findNode[getRPCID()] = &query{start: t1}
	q.findNode[getRPCID()] = &query{start: t2}
	q.findNode[getRPCID()] = &query{start: t3}

	q.join[getRPCID()] = &join{start: t1}
	q.join[getRPCID()] = &join{start: t2}
	q.join[getRPCID()] = &join{start: t3}

	q.ping[getRPCID()] = &ping{start: t1}
	q.ping[getRPCID()] = &ping{start: t2}
	q.ping[getRPCID()] = &ping{start: t3}

	q.get[getRPCID()] = &get{start: t1}
	q.get[getRPCID()] = &get{start: t2}
	q.get[getRPCID()] = &get{start: t3}

	q.GC()

	assert.Equal(t, 2, len(q.findNode))
	assert.Equal(t, 2, len(q.join))
	assert.Equal(t, 2, len(q.ping))
	assert.Equal(t, 2, len(q.get))
}
