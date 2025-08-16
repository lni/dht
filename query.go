// Copyright 2024 Lei Ni (nilei81@gmail.com)
//
// This library follows a dual licensing model -
//
// - it is licensed under the 2-clause BSD license if you have written evidence showing that you are a licensee of github.com/lni/pothos
// - otherwise, it is licensed under the GPL-2 license
//
// See the LICENSE file for details

package dht

import "time"

const (
	// parallelism factor
	// defaultAlpha int = 5
	expiredInterval = 10 * time.Second
)

type requestStatus struct {
	Timeout   bool
	Responded bool
}

func (r requestStatus) known() bool {
	return r.Timeout || r.Responded
}

// query is used to describe the ongoing queries. It acts like a session object.
type query struct {
	onCompletion func()
	pending      int
	start        time.Time
	RPCID        rpcID
	Target       nodeID
	Requested    map[nodeID]*requestStatus
}

func newQuery(rpcID rpcID, target nodeID, onCompletion func()) *query {
	return &query{
		RPCID:        rpcID,
		Target:       target,
		start:        time.Now(),
		onCompletion: onCompletion,
		Requested:    make(map[nodeID]*requestStatus),
	}
}

func (q *query) Pending() int {
	return q.pending
}

func (q *query) Request(nodeID nodeID) {
	q.Requested[nodeID] = &requestStatus{}
	q.pending++
}

func (q *query) Filter(candidates []remote) []remote {
	result := make([]remote, 0)
	for _, rec := range candidates {
		if _, ok := q.Requested[rec.NodeID]; !ok {
			result = append(result, rec)
		}
	}

	return result
}

func (q *query) OnTimeout(nodeID nodeID) bool {
	if status, ok := q.Requested[nodeID]; ok {
		if !status.known() {
			status.Timeout = true
			q.pending--
			return true
		}
	}

	return false
}

func (q *query) OnResponded(nodeID nodeID) bool {
	if status, ok := q.Requested[nodeID]; ok {
		if !status.known() {
			status.Responded = true
			q.pending--

			return true
		}
	}

	return false
}

type join struct {
	// we don't have the nodeID of the routers, so we just record the time
	// when join is requested and allow a time windows for responses to be
	// processed
	start time.Time
}

type ping struct {
	start     time.Time
	requested map[nodeID]struct{}
}

func (p *ping) Requested(nodeID nodeID) {
	p.requested[nodeID] = struct{}{}
}

type get struct {
	start time.Time
}

type queryManager struct {
	findNode map[rpcID]*query
	// join and ping are just used to keep track all join and ping requests
	// initiated by the local node, there is no timeout for join and ping
	join map[rpcID]*join
	ping map[rpcID]*ping
	get  map[rpcID]*get
}

func newQueryManager() *queryManager {
	return &queryManager{
		findNode: make(map[rpcID]*query),
		join:     make(map[rpcID]*join),
		ping:     make(map[rpcID]*ping),
		get:      make(map[rpcID]*get),
	}
}

func (q *queryManager) GetOnCompletionTask(rpcID rpcID) func() {
	if v, ok := q.findNode[rpcID]; ok {
		return v.onCompletion
	}
	return nil
}

func (q *queryManager) AddFindNode(rpcID rpcID,
	target nodeID, onCompletion func()) *query {
	v := newQuery(rpcID, target, onCompletion)
	q.findNode[rpcID] = v

	return v
}

func (q *queryManager) AddJoin(rpcID rpcID) {
	q.join[rpcID] = &join{time.Now()}
}

func (q *queryManager) AddPing(rpcID rpcID, target nodeID) {
	p := &ping{
		start:     time.Now(),
		requested: make(map[nodeID]struct{}),
	}
	p.requested[target] = struct{}{}
	q.ping[rpcID] = p
}

func (q *queryManager) AddGet(rpcID rpcID) {
	g := &get{
		start: time.Now(),
	}
	q.get[rpcID] = g
}

func (q *queryManager) GetQuery(rpcID rpcID) (*query, bool) {
	v, ok := q.findNode[rpcID]
	return v, ok
}

func (q *queryManager) RemoveQuery(rpcID rpcID) {
	delete(q.findNode, rpcID)
}

func (q *queryManager) IsExpectedResponse(msg message) bool {
	if msg.Query {
		panic("not a response")
	}

	switch msg.RPCType {
	case RPCPing:
		if p, ok := q.ping[msg.RPCID]; !ok {
			return false
		} else {
			if _, ok := p.requested[msg.From.NodeID]; !ok {
				return false
			}
		}
		return true
	case RPCJoin:
		_, ok := q.join[msg.RPCID]
		return ok
	case RPCFindNode:
		if f, ok := q.findNode[msg.RPCID]; !ok {
			return false
		} else {
			if _, ok := f.Requested[msg.From.NodeID]; !ok {
				return false
			}
		}
		return true
	case RPCGet:
		_, ok := q.get[msg.RPCID]
		return ok
	default:
		return false
	}
}

func (q *queryManager) GC() {
	last := time.Now().Add(-expiredInterval)

	for key, f := range q.findNode {
		if f.start.Before(last) {
			delete(q.findNode, key)
		}
	}
	for key, j := range q.join {
		if j.start.Before(last) {
			delete(q.join, key)
		}
	}
	for key, p := range q.ping {
		if p.start.Before(last) {
			delete(q.ping, key)
		}
	}
	for key, g := range q.get {
		if g.start.Before(last) {
			delete(q.get, key)
		}
	}
}
