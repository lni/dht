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

func TestMessageVerification(t *testing.T) {
	rpcID := getRPCID()
	nodeID := getRandomNodeID()
	msg := message{
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
	}

	buf, err := getMessageBuf(nil, msg)
	require.NoError(t, err)
	result, ok := verifyReceivedMessage(buf)
	require.True(t, ok)
	m := make([]byte, 4096)
	marshaled, err := msg.Marshal(m)
	require.NoError(t, err)
	assert.Equal(t, marshaled, result)
}
