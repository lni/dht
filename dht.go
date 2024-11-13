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
	"time"

	"github.com/lni/goutils/syncutil"
	"go.uber.org/zap"
)

const (
	cachedTTL                  uint16 = 60
	ongoingManagerGCInterval          = 5 * time.Second
	storeGCInterval                   = 60 * time.Second
	staledRemotePingInterval          = 120 * time.Second
	emptyKBucketRefillInterval        = 600 * time.Second
	routingTableGCInterval            = 300 * time.Second
	defaultFindNodeTimeout            = 100 * time.Millisecond
	minDelay                          = 50 * time.Millisecond
	minJoinInterval                   = 20 * time.Millisecond
	minRefillInterval                 = 90 * time.Millisecond
	// implies max of 2 ^ 24 nodes
	maxFindNodeIteration = 24
)

var (
	magicNumber = [2]byte{0xEF, 0x2B}
)

type Config = DHTConfig

type schedulable func()

type timeout struct {
	RPCID     rpcID
	RPCType   rpcType
	NodeID    nodeID
	Target    nodeID
	Iteration int
}

type sendReq struct {
	Msg  message
	Addr net.UDPAddr
}

type reqType int8

const (
	RequestJoin reqType = iota
	RequestPut
	RequestGet
	RequestGetFromCached
)

type request struct {
	RequestType  reqType
	Target       nodeID
	Value        []byte
	TTL          uint16
	FromCachedCh chan [][]byte
}

// DHT implements the kademlia protocol with a key-value store interface exposed
// for users. Asynchronous Put() and Get() methods follow the best effort
// approach.
//
// Note that this DHT is intended to be used in trusted environment, e.g.
// managed data centers, sybil attacks are assumed to be impossible in such
// settings.
type DHT struct {
	cfg     Config
	self    remote
	address net.UDPAddr
	conn    *conn
	rt      *routingTable
	ongoing *queryManager

	// store kv pairs to serve for RPC requests
	store *kvStore
	// store responses to serve for local requests
	cached *kvStore

	scheduledCh chan schedulable
	sendMsgCh   chan sendReq
	requestCh   chan request
	timeoutCh   chan timeout
	loopbackCh  chan message

	lastJoin   time.Time
	lastRefill time.Time

	stopper *syncutil.Stopper
	log     *zap.Logger
}

// NewDHT creates a DHT peer ready to be started.
func NewDHT(cfg Config, logger *zap.Logger) (*DHT, error) {
	nodeID := getRandomNodeID()
	conn, err := newConn(cfg)
	if err != nil {
		return nil, err
	}
	return &DHT{
		cfg:         cfg,
		self:        remote{NodeID: nodeID, Address: cfg.Address},
		address:     cfg.Address,
		conn:        conn,
		rt:          newRoutingTable(DefaultK, DefaultBits, nodeID, cfg.Address),
		ongoing:     newQueryManager(),
		store:       newKVStore(),
		cached:      newKVStore(),
		scheduledCh: make(chan schedulable, 16),
		sendMsgCh:   make(chan sendReq, 16),
		requestCh:   make(chan request, 16),
		timeoutCh:   make(chan timeout, 16),
		loopbackCh:  make(chan message, 16),
		stopper:     syncutil.NewStopper(),
		log:         logger,
	}, nil
}

// Start starts the local DHT peer.
func (d *DHT) Start() error {
	d.stopper.RunWorker(func() {
		if err := d.conn.ReceiveMessageLoop(d.stopper.ShouldStop()); err != nil {
			panic(err)
		}
	})
	d.stopper.RunWorker(func() {
		d.sendMessageWorker()
	})
	d.stopper.RunWorker(func() {
		d.loop()
	})
	d.requestToJoin()
	// schedule a join to make tests and simulation easier
	d.schedule(time.Second, func() {
		d.requestToJoin()
	})

	return nil
}

// Close closes the local DHT peer.
func (d *DHT) Close() error {
	d.log.Debug("going to stop the stopper")
	d.stopper.Stop()
	d.log.Debug("stopper stopped")
	return d.conn.Close()
}

// FIXME: check key and value length, return error if over the limit

// Put asynchronously puts the specified key value pair onto the DHT network
// with its TTL value set to ttl seconds.
func (d *DHT) Put(key Key, value []byte, ttl uint16) {
	req := request{
		RequestType: RequestPut,
		Target:      key,
		Value:       value,
		TTL:         ttl,
	}
	d.request(req)
}

// Get asynchronously queries the DHT network for values associated with the
// specified key. Matched values will be locally cached in current peer's local
// store. Caller should use a follow up GetCached() call to access the returned
// values.
func (d *DHT) Get(key Key) {
	req := request{
		RequestType: RequestGet,
		Target:      key,
	}
	d.request(req)
}

// ScheduleGet schedules a Get() to be performed after the specified delay.
func (d *DHT) ScheduleGet(delay time.Duration, key Key) {
	d.stopper.RunWorker(func() {
		timer := time.NewTicker(delay)
		defer timer.Stop()

		select {
		case <-d.stopper.ShouldStop():
		case <-timer.C:
			d.Get(key)
		}
	})
}

// GetCached returns the values associated with the specified key by only
// looking at current peer's local store. It is normally invoked after a
// successful return of Get().
func (d *DHT) GetCached(key Key) [][]byte {
	req := request{
		RequestType:  RequestGetFromCached,
		Target:       key,
		FromCachedCh: make(chan [][]byte, 1),
	}
	d.request(req)

	select {
	case <-d.stopper.ShouldStop():
		return nil
	case v := <-req.FromCachedCh:
		return v
	}
}

//
// long running loops
//

func (d *DHT) sendMessageWorker() {
	buf := make([]byte, 1024*4)
	for {
		select {
		case <-d.stopper.ShouldStop():
			return
		case req := <-d.sendMsgCh:
			msg := req.Msg
			msg.Secret = d.cfg.Secret
			b, err := getMessageBuf(buf, msg)
			if err != nil {
				panic(err)
			}
			if err := d.conn.SendMessage(b, req.Addr); err != nil {
				d.log.Debug("failed to send message")
			}
		}
	}
}

func (d *DHT) loop() {
	// random interval of up to 5 seconds to prevents all nodes in the network
	// to start the same job at the same time when they are all launched by
	// simulation at the same time
	ri := time.Duration(rand.Uint64()%5000) * time.Millisecond
	storeGCTicker := time.NewTicker(storeGCInterval + ri)
	defer storeGCTicker.Stop()
	stalePingTicker := time.NewTicker(staledRemotePingInterval + ri)
	defer stalePingTicker.Stop()
	emptyKBucketRefillTicker := time.NewTicker(emptyKBucketRefillInterval + ri)
	defer emptyKBucketRefillTicker.Stop()
	routingTableGCTicker := time.NewTicker(routingTableGCInterval + ri)
	defer routingTableGCTicker.Stop()

	for {
		select {
		case <-d.stopper.ShouldStop():
			d.log.Debug("main loop going to return")
			return
		case msg := <-d.loopbackCh:
			d.handleMessage(msg)
		case <-storeGCTicker.C:
			d.storeGC()
		case fn := <-d.scheduledCh:
			fn()
		case <-routingTableGCTicker.C:
			d.routingTableGC()
		case <-emptyKBucketRefillTicker.C:
			// minDelay is set to false to slowly refill those empty kbuckets
			d.refillEmptyKBucket(false)
		case <-stalePingTicker.C:
			d.pingStaleRemotes()
		case msg := <-d.conn.ReceivedCh:
			d.handleMessage(msg)
		case req := <-d.requestCh:
			d.handleRequest(req)
		case timeout := <-d.timeoutCh:
			d.handleTimeout(timeout)
		}
	}
}

//
// requests
//

func (d *DHT) request(r request) {
	select {
	case <-d.stopper.ShouldStop():
	case d.requestCh <- r:
	}
}

func (d *DHT) handleRequest(req request) {
	if req.RequestType == RequestJoin {
		d.join()
	} else if req.RequestType == RequestPut {
		d.put(req.Target, req.Value, req.TTL)
	} else if req.RequestType == RequestGet {
		d.get(req.Target)
	} else if req.RequestType == RequestGetFromCached {
		d.getFromCached(req.Target, req.FromCachedCh)
	} else {
		panic("unknown request type")
	}
}

func (d *DHT) requestToJoin() {
	d.request(request{RequestType: RequestJoin})
}

func (d *DHT) pingNode(nodeID nodeID, addr net.UDPAddr) {
	msg := message{
		RPCType: RPCPing,
		Query:   true,
		RPCID:   getRPCID(),
		From:    d.self,
		Target:  nodeID,
	}
	d.ongoing.AddPing(msg.RPCID, nodeID)
	d.sendMessage(msg, addr)
}

//
// rpc initiator
//

func (d *DHT) join() {
	if !d.allowToJoin() {
		return
	}
	// send a find_node request to each router to find the local node itself
	rpcID := getRPCID()
	d.ongoing.AddJoin(rpcID)
	msg := message{
		RPCType: RPCJoin,
		Query:   true,
		RPCID:   rpcID,
		Target:  d.self.NodeID,
		From:    d.self,
	}

	for _, router := range d.cfg.Routers {
		d.sendMessage(msg, router)
	}
}

func (d *DHT) findNode(target nodeID) {
	d.doFindNode(target, nil)
}

func (d *DHT) put(target nodeID, value []byte, ttl uint16) {
	onCompletion := func() {
		d.log.Debug("find node completed for put query",
			targetField(target),
			localNodeIDField(d))
		d.putKeyValue(target, value, ttl)
	}
	d.doFindNode(target, onCompletion)
}

func (d *DHT) get(target nodeID) {
	onCompletion := func() {
		d.log.Debug("find node completed for put query",
			targetField(target),
			localNodeIDField(d))
		d.getKeyValue(target)
	}
	d.doFindNode(target, onCompletion)
}

func (d *DHT) getFromCached(target nodeID, ch chan [][]byte) {
	v, _ := d.cached.Get(target)
	select {
	case <-d.stopper.ShouldStop():
	case ch <- v:
	}
}

func (d *DHT) putKeyValue(target nodeID, value []byte, ttl uint16) {
	msg := message{
		RPCType: RPCPut,
		Query:   true,
		RPCID:   getRPCID(),
		Target:  target,
		From:    d.self,
		TTL:     ttl,
		Values:  [][]byte{value},
	}
	kn := d.rt.KNearest(target)
	for _, rt := range kn {
		d.sendMessage(msg, rt.Address)
	}
}

func (d *DHT) getKeyValue(target nodeID) {
	rpcID := getRPCID()
	d.ongoing.AddGet(rpcID)
	msg := message{
		RPCType: RPCGet,
		RPCID:   rpcID,
		Query:   true,
		Target:  target,
		From:    d.self,
	}
	kn := d.rt.KNearest(target)
	for _, rt := range kn {
		d.sendMessage(msg, rt.Address)
	}
}

func (d *DHT) doFindNode(target nodeID, onCompletion schedulable) {
	kn := d.rt.KNearest(target)
	if len(kn) > 0 {
		rpcID := getRPCID()
		q := d.ongoing.AddFindNode(rpcID, target, onCompletion)
		for _, rt := range kn {
			d.sendFindNodeRequest(target, rpcID, rt, 0)
			q.Request(rt.NodeID)
		}
	}
	if len(kn) < DefaultK {
		d.schedule(getRandomDelay(time.Second), func() {
			d.join()
		})
	}
}

//
// send and receive messages
//

func (d *DHT) handleMessage(msg message) {
	if msg.Secret != d.cfg.Secret {
		return
	}
	d.rt.Observe(msg.From.NodeID, msg.From.Address)
	if msg.Query {
		d.handleQuery(msg)
		return
	}
	d.handleResponse(msg)
}

func (d *DHT) toLocalNode(addr net.UDPAddr) bool {
	return d.self.Address.IP.Equal(addr.IP) &&
		d.self.Address.Port == addr.Port &&
		d.self.Address.Zone == addr.Zone
}

func (d *DHT) sendMessage(m message, addr net.UDPAddr) {
	verifyMessage(m)

	if d.toLocalNode(addr) {
		select {
		case d.loopbackCh <- m:
		default:
		}
	}

	select {
	case d.sendMsgCh <- sendReq{m, addr}:
	default:
		// too many pending messages
	}
}

//
// queries
//

func (d *DHT) handleQuery(msg message) {
	if msg.RPCType == RPCPing {
		d.handlePingQuery(msg)
	} else if msg.RPCType == RPCJoin {
		d.handleJoinQuery(msg)
	} else if msg.RPCType == RPCFindNode {
		d.handleFindNodeQuery(msg)
	} else if msg.RPCType == RPCPut {
		d.handlePutQuery(msg)
	} else if msg.RPCType == RPCGet {
		d.handleGetQuery(msg)
	} else {
		panic("unknown type")
	}
}

func (d *DHT) handlePutQuery(msg message) {
	d.log.Debug("received put query",
		fromField(msg.From),
		localNodeIDField(d),
		targetField(msg.Target))
	if len(msg.Values) > 0 {
		d.store.Put(msg.Target, msg.Values[0], msg.TTL)
	}
}

func (d *DHT) handleGetQuery(msg message) {
	d.log.Debug("received get query",
		fromField(msg.From),
		localNodeIDField(d),
		targetField(msg.Target))
	values, ok := d.store.Get(msg.Target)
	if !ok {
		return
	}
	batches := to4KBatches(values)
	for _, v := range batches {
		reply := message{
			RPCType: RPCGet,
			Query:   false,
			Target:  msg.Target,
			RPCID:   msg.RPCID,
			From:    d.self,
			Values:  v,
		}
		d.sendMessage(reply, msg.From.Address)
	}
}

// very similar to find_node, we have a separate join rpc just because
// we don't know the target nodeID in the join rpc
func (d *DHT) handleJoinQuery(msg message) {
	d.log.Debug("received join query",
		fromField(msg.From),
		localNodeIDField(d))
	resp := message{
		RPCType: msg.RPCType,
		Query:   false,
		RPCID:   msg.RPCID,
		From:    d.self,
		Target:  msg.Target,
		Nodes:   d.rt.KNearest(msg.Target),
	}
	d.sendMessage(resp, msg.From.Address)
}

func (d *DHT) handlePingQuery(msg message) {
	resp := message{
		RPCType: msg.RPCType,
		Query:   false,
		RPCID:   msg.RPCID,
		From:    d.self,
	}

	d.sendMessage(resp, msg.From.Address)
}

func (d *DHT) handleFindNodeQuery(msg message) {
	d.log.Debug("received find node query",
		fromField(msg.From),
		localNodeIDField(d),
		targetField(msg.Target))

	if msg.Target.IsEmpty() {
		panic("empty target")
	}

	kn := d.rt.KNearest(msg.Target)
	resp := message{
		RPCType: msg.RPCType,
		Query:   false,
		RPCID:   msg.RPCID,
		From:    d.self,
		Nodes:   kn,
		Target:  msg.Target,
	}

	d.sendMessage(resp, msg.From.Address)
}

//
// responses
//

func (d *DHT) handleResponse(msg message) {
	if !d.ongoing.IsExpectedResponse(msg) {
		return
	}

	if msg.RPCType == RPCPing {
		// nothing to do
	} else if msg.RPCType == RPCGet {
		d.handleGetResponse(msg)
	} else if msg.RPCType == RPCFindNode {
		d.handleFindNodeResponse(msg)
	} else if msg.RPCType == RPCJoin {
		d.handleJoinResponse(msg)
	} else {
		panic("unknown type")
	}
}

func (d *DHT) handleGetResponse(msg message) {
	for _, v := range msg.Values {
		d.cached.Put(msg.Target, v, cachedTTL)
	}
}

func (d *DHT) handleFindNodeResponse(msg message) {
	// not an ongoing query
	q, ok := d.ongoing.GetQuery(msg.RPCID)
	if !ok {
		return
	}
	// not timeout
	if q.OnResponded(msg.From.NodeID) {
		for _, node := range msg.Nodes {
			d.rt.Observe(node.NodeID, node.Address)
		}
		iter := int(msg.Iteration) + 1
		d.recursiveFindNode(msg.Target, msg.RPCID, q, iter)
	}
}

func (d *DHT) handleJoinResponse(msg message) {
	for _, node := range msg.Nodes {
		d.rt.Observe(node.NodeID, node.Address)
	}
	// delay a little bit to allow other join responses to arrive
	d.schedule(100*time.Millisecond, func() {
		d.refillEmptyKBucket(true)
	})
}

func (d *DHT) sendFindNodeRequest(target nodeID,
	rpcID rpcID, rt remote, iter int) {
	// when iter <= maxFindNodeIteration, it is just normal path
	// when iter > maxFindNodeIteration, the message won't be sent, just like
	// the UDP packet is dropped, so no more nodes can be discovered. other
	// than that, everything else is the same. eventually this iteration of
	// findNode to the specified target will timeout, it gets a chance to
	// declare the whole op as completed
	if iter <= maxFindNodeIteration {
		msg := message{
			RPCType:   RPCFindNode,
			Query:     true,
			RPCID:     rpcID,
			From:      d.self,
			Target:    target,
			Iteration: uint8(iter),
		}
		d.sendMessage(msg, rt.Address)
	}
	// can't use schedule() here, see comments in doSchedule() for details
	d.runWorker(defaultFindNodeTimeout, func() {
		timeout := timeout{
			RPCID:     rpcID,
			RPCType:   RPCFindNode,
			NodeID:    rt.NodeID,
			Target:    target,
			Iteration: iter,
		}
		select {
		case d.timeoutCh <- timeout:
		case <-d.stopper.ShouldStop():
			return
		}
	})
}

func (d *DHT) recursiveFindNode(target nodeID,
	rpcID rpcID, q *query, iter int) bool {
	kn := d.rt.KNearest(target)
	kn = q.Filter(kn)
	if q.Pending() == 0 && len(kn) == 0 {
		// all ongoing find_node requests have been completed, no more closer target
		// to visit. find node completed.
		d.onFindNodeCompleted(rpcID)
		return true
	}
	for _, rt := range kn {
		d.sendFindNodeRequest(target, rpcID, rt, iter)
		q.Request(rt.NodeID)
	}

	return false
}

func (d *DHT) onFindNodeCompleted(rpcID rpcID) {
	// Put request will be initiated by the scheduled task
	onCompletion := d.ongoing.GetOnCompletionTask(rpcID)
	if onCompletion != nil {
		d.schedule(0, func() {
			onCompletion()
		})
	}
	d.ongoing.RemoveQuery(rpcID)
}

func (d *DHT) handleTimeout(timeout timeout) {
	if timeout.RPCType == RPCFindNode {
		iter := timeout.Iteration + 1
		if q, ok := d.ongoing.GetQuery(timeout.RPCID); ok {
			if q.OnTimeout(timeout.NodeID) {
				d.recursiveFindNode(timeout.Target, timeout.RPCID, q, iter)
			}
		}
	}
}

//
// maintenance tasks
//

func (d *DHT) pingStaleRemotes() {
	d.log.Debug("pinging staled remotes")
	staled := d.rt.GetStaleRemote()
	ms := staledRemotePingInterval.Milliseconds()

	for _, sr := range staled {
		remote := sr
		delay := time.Duration(rand.Uint64()%uint64(ms)) * time.Millisecond
		d.schedule(delay, func() {
			d.pingNode(remote.NodeID, remote.Address)
		})
	}
}

func (d *DHT) storeGC() {
	d.log.Debug("store gc called")
	d.store.GC()
	d.cached.GC()
}

func (d *DHT) routingTableGC() {
	d.log.Debug("routing table gc called")
	d.rt.GC()
}

func (d *DHT) refillEmptyKBucket(noDelay bool) {
	if !d.allowToRefill() {
		return
	}

	d.log.Debug("refilling empty kbucket")
	nodes := d.rt.InterestedNodes()
	ms := emptyKBucketRefillInterval.Milliseconds()

	for _, node := range nodes {
		n := node
		delay := time.Duration(rand.Uint64()%uint64(ms)) * time.Millisecond
		// still apply minimum delay
		if noDelay {
			delay = minDelay
		}
		d.schedule(delay, func() {
			d.findNode(n)
		})
	}
}

func (d *DHT) allowToJoin() bool {
	if time.Since(d.lastJoin) > minJoinInterval {
		d.lastJoin = time.Now()
		return true
	}

	return false
}

func (d *DHT) allowToRefill() bool {
	if time.Since(d.lastRefill) > minRefillInterval {
		d.lastRefill = time.Now()
		return true
	}

	return false
}

func (d *DHT) schedule(delay time.Duration, fn schedulable) {
	d.doSchedule(delay, true, fn)
}

func (d *DHT) runWorker(delay time.Duration, fn schedulable) {
	d.doSchedule(delay, false, fn)
}

func (d *DHT) doSchedule(delay time.Duration, mainThread bool, fn schedulable) {
	go func() {
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-d.stopper.ShouldStop():
				return
			}
		}

		if mainThread {
			select {
			case <-d.stopper.ShouldStop():
			case d.scheduledCh <- fn:
			}
		} else {
			// execute the fn in current thread. this is required if fn need to send
			// to those channels selected by the main loop to avoid deadlock.
			fn()
		}
	}()
}

func verifyMessage(msg message) {
	if msg.From.NodeID.IsEmpty() {
		panic("empty from node id")
	}
	if msg.RPCID == 0 {
		panic("empty RPCID")
	}
}

func getRPCID() rpcID {
	for {
		if v := rand.Uint64(); v != 0 {
			return rpcID(v)
		}
	}
}

func getRandomDelay(d time.Duration) time.Duration {
	ms := d.Milliseconds()
	return time.Duration(rand.Uint64()%uint64(ms)) * time.Millisecond
}
