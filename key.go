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
	"crypto/sha1"
	"fmt"
	"io"
	"math/rand"
)

type Key = key

type nodeID = key

type key struct {
	High uint64
	Low  uint64
}

func (k key) IsEmpty() bool {
	return k.High == 0 && k.Low == 0
}

func (k key) Short() string {
	return fmt.Sprintf("%04x", k.Low%0x10000)
}

func (k key) String() string {
	return fmt.Sprintf("%016x %016x", k.High, k.Low)
}

func (k *key) Equal(other key) bool {
	return k.High == other.High && k.Low == other.Low
}

func (k *key) Less(other key) bool {
	if k.High < other.High {
		return true
	} else if k.High > other.High {
		return false
	}

	return k.Low < other.Low
}

func (k *key) leadingZeroBits() int {
	count := leadingZeroBits(k.High)
	if count < 64 {
		return count
	}

	return count + leadingZeroBits(k.Low)
}

func leadingZeroBits(v uint64) int {
	count := 0
	for i := 64 - 1; i >= 0; i-- {
		if v&(1<<i) == 0 {
			count++
		} else {
			break
		}
	}

	return count
}

func (k key) CommonPrefixLength(other key) int {
	var r key
	r.Distance(k, other)

	return r.leadingZeroBits()
}

// set k as the xor result of a and b
func (k *key) Distance(a, b key) {
	k.High = a.High ^ b.High
	k.Low = a.Low ^ b.Low
}

func (k *key) FromNodeID(other nodeID) {
	k.High = other.High
	k.Low = other.Low
}

func (k *key) FromUint64(v uint64) {
	k.High = v
	k.Low = v
}

func (k *key) FromString(v string) {
	h := sha1.New()
	if _, err := io.WriteString(h, v); err != nil {
		panic(err)
	}
	cs := h.Sum(nil)
	k.High = codec.Uint64(cs)
	k.Low = codec.Uint64(cs[8:])
}

func getRandomNodeID() nodeID {
	high := uint64(0)
	for high == 0 {
		high = rand.Uint64()
	}
	low := uint64(0)
	for low == 0 {
		low = rand.Uint64()
	}

	return nodeID{
		High: high,
		Low:  low,
	}
}
