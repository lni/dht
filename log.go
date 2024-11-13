// Copyright 2024 Lei Ni (nilei81@gmail.com)
//
// This library follows a dual licensing model -
//
// - it is licensed under the 2-clause BSD license if you have written evidence showing that you are a licensee of github.com/lni/pothos
// - otherwise, it is licensed under the GPL-2 license
//
// See the LICENSE file for details

package dht

import "go.uber.org/zap"

func localNodeIDField(d *DHT) zap.Field {
	return zap.String("myid", d.self.NodeID.Short())
}

func targetField(target nodeID) zap.Field {
	return zap.String("target", target.Short())
}

func fromField(rt remote) zap.Field {
	return zap.String("from", rt.NodeID.Short())
}
