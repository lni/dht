// Copyright 2024 Lei Ni (nilei81@gmail.com)
//
// This library follows a dual licensing model -
//
// - it is licensed under the 2-clause BSD license if you have written evidence showing that you are a licensee of github.com/lni/pothos
// - otherwise, it is licensed under the GPL-2 license
//
// See the LICENSE file for details

package dht

import "net"

// DHTConfig is the config for the DHT.
type DHTConfig struct {
	// Secret of the DHT deployment. Note that this is not a security setting,
	// it is only used to prevent dht nodes from different deployments talking to
	// each other as a result of misconfiguration.
	Secret  uint16
	Proto   string
	Routers []net.UDPAddr
	Address net.UDPAddr
}
