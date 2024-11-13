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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageMarshal(t *testing.T) {
	rpcID := getRPCID()
	nodeID := getRandomNodeID()
	msg := &message{
		RPCType: RPCJoin,
		Query:   true,
		RPCID:   rpcID,
		Target:  nodeID,
		From: remote{nodeID, net.UDPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 39000,
		}},
		Nodes: []remote{
			remote{
				getRandomNodeID(),
				net.UDPAddr{
					IP:   net.ParseIP("127.0.0.1"),
					Port: 39000,
				},
			},
			remote{
				getRandomNodeID(),
				net.UDPAddr{
					IP:   net.ParseIP("127.0.0.2"),
					Port: 39002,
				},
			},
		},
		Values:    [][]byte{[]byte("test"), []byte("against"), []byte("samples")},
		Iteration: 120,
		Secret:    12345,
	}

	buf := make([]byte, msg.MarshalSize())

	data, err := msg.Marshal(buf)
	require.NoError(t, err)

	msg2 := &message{}
	require.NoError(t, msg2.Unmarshal(data))
	assert.Equal(t, msg, msg2)
}

func TestMarshalRemote(t *testing.T) {
	r := &remote{
		NodeID: nodeID{
			High: 1234567890,
			Low:  987654321,
		},
		Address: net.UDPAddr{
			IP:   []byte{1, 3, 5, 7},
			Port: 12345,
			Zone: "test",
		},
	}
	var err error
	buf := make([]byte, r.MarshalSize())
	buf, err = r.Marshal(buf)
	require.NoError(t, err)

	rr := &remote{}
	require.NoError(t, rr.Unmarshal(buf))
	assert.Equal(t, r, rr)
}
