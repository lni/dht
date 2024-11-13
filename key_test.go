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

	"github.com/stretchr/testify/assert"
)

func TestLeadingZeroBits(t *testing.T) {
	tests := []struct {
		v     uint64
		count int
	}{
		{0, 64},
		{1, 63},
		{3, 62},
		{0x80FF00FF00FF00FF, 0},
		{0x00FF00FF00FF00FF, 8},
		{0x7FFFFFFFFFFFFFFF, 1},
		{0x1FFFFFFFFFFFFFFF, 3},
		{0xFFFFFFFFFFFFFFFF, 0},
	}

	for idx, tt := range tests {
		result := leadingZeroBits(tt.v)
		assert.Equal(t, tt.count, result, idx)
	}
}

func TestKeyLeadingZeroBits(t *testing.T) {
	tests := []struct {
		high  uint64
		low   uint64
		count int
	}{
		{0, 0xFF, 120},
		{0, 0, 128},
		{0xFF, 0, 56},
		{0xFF00, 0, 48},
		{0x0031a6a576efd7b2, 0x7d68547268b0349a, 10},
	}

	for idx, tt := range tests {
		k := key{tt.high, tt.low}
		bits := k.leadingZeroBits()
		assert.Equal(t, tt.count, bits, idx)
	}
}

func TestKeyFromNodeID(t *testing.T) {
	n := nodeID{High: 123, Low: 456}
	var k key
	k.FromNodeID(n)

	assert.Equal(t, n, k)
}

func TestKeyDistance(t *testing.T) {
	allset := uint64(0xFFFFFFFFFFFFFFFF)
	tests := []struct {
		ah uint64
		al uint64
		bh uint64
		bl uint64
		kh uint64
		kl uint64
	}{
		{123, 456, 123, 456, 0, 0},
		{allset, allset, 0, 0, allset, allset},
	}

	for idx, tt := range tests {
		a := key{tt.ah, tt.al}
		b := key{tt.bh, tt.bl}
		var k1 key
		k1.Distance(a, b)
		var k2 key
		k2.Distance(b, a)

		assert.Equal(t, k1, k2, idx)
		assert.Equal(t, key{tt.kh, tt.kl}, k1, idx)
	}
}

func TestKeyCommonPrefixLength(t *testing.T) {
	allset := uint64(0xFFFFFFFFFFFFFFFF)
	tests := []struct {
		high      uint64
		low       uint64
		otherHigh uint64
		otherLow  uint64
		length    int
	}{
		{123, 456, 123, 456, 128},
		{123, allset, 123, 0x8000000000000000, 65},
		{123, allset, 123, 0, 64},
		{0x8000000000000000, allset, 0x8FFFFFFFFFFFFFFF, 0, 4},
		{allset, allset, 0, 0, 0},
	}

	for idx, tt := range tests {
		v := key{tt.high, tt.low}
		other := key{tt.otherHigh, tt.otherLow}
		result := v.CommonPrefixLength(other)
		assert.Equal(t, tt.length, result, idx)
	}
}

func TestKeyLess(t *testing.T) {
	tests := []struct {
		ah   uint64
		al   uint64
		bh   uint64
		bl   uint64
		less bool
	}{
		{100, 200, 101, 199, true},
		{100, 200, 101, 201, true},
		{100, 200, 101, 200, true},
		{100, 200, 100, 201, true},
		{100, 200, 100, 200, false},
		{100, 200, 100, 199, false},
		{100, 200, 99, 200, false},
		{100, 200, 99, 201, false},
		{100, 200, 99, 199, false},
	}

	for idx, tt := range tests {
		a := key{tt.ah, tt.al}
		b := key{tt.bh, tt.bl}
		assert.Equal(t, tt.less, a.Less(b), idx)
	}
}
