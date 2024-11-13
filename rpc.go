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
	"errors"
	"net"
)

var (
	ErrBufferTooSmall = errors.New("size of the buffer is too small")
)

type rpcID uint64

type rpcType int8

const (
	RPCPing rpcType = iota
	RPCJoin
	RPCFindNode
	RPCPut
	RPCGet
)

type remote struct {
	NodeID  nodeID
	Address net.UDPAddr
}

func (r remote) MarshalSize() int {
	// NodeID
	sz := 16
	// size of Address
	sz += r.addressSize()

	return sz
}

func (r remote) addressSize() int {
	sz := 0
	// Address.IP
	sz += 4
	sz += len(r.Address.IP)
	// Address.Port
	sz += 4
	// Address.Zone
	sz += 4
	sz += len(r.Address.Zone)

	return sz
}

func (r *remote) Marshal(buf []byte) ([]byte, error) {
	if len(buf) < r.MarshalSize() {
		return nil, ErrBufferTooSmall
	}

	// NodeID
	codec.PutUint64(buf, r.NodeID.High)
	codec.PutUint64(buf[8:], r.NodeID.Low)
	// Address.IP
	ipsz := len(r.Address.IP)
	codec.PutUint32(buf[16:], uint32(ipsz))
	copy(buf[20:], r.Address.IP)
	// Address.Port
	codec.PutUint32(buf[20+ipsz:], uint32(r.Address.Port))
	// Address.Zone
	zonesz := len(r.Address.Zone)
	codec.PutUint32(buf[24+ipsz:], uint32(zonesz))
	copy(buf[28+ipsz:], []byte(r.Address.Zone))

	return buf[:28+ipsz+zonesz], nil
}

func (r *remote) Unmarshal(buf []byte) error {
	// NodeID
	if len(buf) < 16 {
		return ErrBufferTooSmall
	}
	r.NodeID.High = codec.Uint64(buf)
	r.NodeID.Low = codec.Uint64(buf[8:])

	// ip size
	if len(buf[16:]) < 4 {
		return ErrBufferTooSmall
	}
	ipSize := int(codec.Uint32(buf[16:]))
	if len(buf[20:]) < ipSize {
		return ErrBufferTooSmall
	}
	ip := make([]byte, ipSize)
	copy(ip, buf[20:20+ipSize])
	r.Address = net.UDPAddr{IP: ip}

	// port
	if len(buf[20+ipSize:]) < 4 {
		return ErrBufferTooSmall
	}
	r.Address.Port = int(codec.Uint32(buf[20+ipSize:]))

	// Zone
	if len(buf[24+ipSize:]) < 4 {
		return ErrBufferTooSmall
	}
	zoneSize := int(codec.Uint32(buf[24+ipSize:]))
	if len(buf[28+ipSize:]) < zoneSize {
		return ErrBufferTooSmall
	}
	zone := make([]byte, zoneSize)
	copy(zone, buf[28+ipSize:28+ipSize+zoneSize])
	r.Address.Zone = string(zone)

	return nil
}

type message struct {
	RPCType rpcType
	Query   bool
	// ttl in seconds
	TTL       uint16
	Target    nodeID
	RPCID     rpcID
	From      remote
	Nodes     []remote
	Values    [][]byte
	Iteration uint8
	Secret    uint16
}

func (m *message) MarshalSize() int {
	sz := 28
	// From
	sz += m.From.MarshalSize()
	// Nodes
	sz += 4
	for _, n := range m.Nodes {
		sz += n.MarshalSize()
	}
	// Values
	sz += 4
	for _, val := range m.Values {
		sz += 4
		sz += len(val)
	}
	// Iteration
	sz += 1
	// Secret
	sz += 2

	return sz
}

func (m *message) Marshal(buf []byte) ([]byte, error) {
	if len(buf) < m.MarshalSize() {
		return nil, ErrBufferTooSmall
	}

	// RPCType
	buf[0] = byte(m.RPCType)
	// Query
	if m.Query {
		buf[1] = 1
	} else {
		buf[1] = 0
	}
	// TTL
	codec.PutUint16(buf[2:], m.TTL)
	// Target
	codec.PutUint64(buf[4:], m.Target.High)
	codec.PutUint64(buf[12:], m.Target.Low)
	// RPCID
	codec.PutUint64(buf[20:], uint64(m.RPCID))
	offset := 28
	// From
	b, err := m.From.Marshal(buf[offset:])
	if err != nil {
		return nil, err
	}
	offset += len(b)
	// Nodes
	codec.PutUint32(buf[offset:], uint32(len(m.Nodes)))
	offset += 4
	for _, n := range m.Nodes {
		b, err := n.Marshal(buf[offset:])
		if err != nil {
			return nil, err
		}
		offset += len(b)
	}
	// Values
	codec.PutUint32(buf[offset:], uint32(len(m.Values)))
	offset += 4
	for _, v := range m.Values {
		codec.PutUint32(buf[offset:], uint32(len(v)))
		offset += 4
		copy(buf[offset:], v)
		offset += len(v)
	}
	buf[offset] = m.Iteration
	offset += 1
	codec.PutUint16(buf[offset:], m.Secret)

	return buf[:m.MarshalSize()], nil
}

func (m *message) Unmarshal(data []byte) error {
	// everything up to From
	if len(data) < 28 {
		return ErrBufferTooSmall
	}

	// RPCType
	m.RPCType = rpcType(data[0])
	// Query
	if data[1] == 0 {
		m.Query = false
	} else {
		m.Query = true
	}
	// TTL
	m.TTL = codec.Uint16(data[2:])
	// Target
	m.Target.High = codec.Uint64(data[4:])
	m.Target.Low = codec.Uint64(data[12:])
	// RPCID
	m.RPCID = rpcID(codec.Uint64(data[20:]))

	offset := 28
	// From
	rr := &remote{}
	if err := rr.Unmarshal(data[offset:]); err != nil {
		return err
	}
	m.From = *rr
	offset += rr.MarshalSize()
	// Nodes
	nodes := make([]remote, 0)
	nodesCount := 0
	if len(data[offset:]) < 4 {
		return ErrBufferTooSmall
	}
	nodesCount = int(codec.Uint32(data[offset:]))
	offset = offset + 4
	for i := 0; i < nodesCount; i++ {
		remote := &remote{}
		if err := remote.Unmarshal(data[offset:]); err != nil {
			return err
		}
		offset = offset + remote.MarshalSize()
		nodes = append(nodes, *remote)
	}
	m.Nodes = nodes

	// Values
	values := make([][]byte, 0)
	valuesCount := 0
	if len(data[offset:]) < 4 {
		return ErrBufferTooSmall
	}
	valuesCount = int(codec.Uint32(data[offset:]))
	offset += 4
	for i := 0; i < valuesCount; i++ {
		if len(data[offset:]) < 4 {
			return ErrBufferTooSmall
		}
		sz := int(codec.Uint32(data[offset:]))
		offset += 4

		if len(data[offset:]) < sz {
			return ErrBufferTooSmall
		}
		value := make([]byte, sz)
		copy(value, data[offset:offset+sz])
		offset += sz
		values = append(values, value)
	}
	m.Values = values
	m.Iteration = data[offset]
	offset += 1
	m.Secret = codec.Uint16(data[offset:])

	return nil
}
