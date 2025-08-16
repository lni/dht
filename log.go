// Copyright 2024 Lei Ni (nilei81@gmail.com)
//
// This library follows a dual licensing model -
//
// - it is licensed under the 2-clause BSD license if you have written evidence showing that you are a licensee of github.com/lni/thyme
// - otherwise, it is licensed under the GPL-2 license
//
// See the LICENSE file for details

package dht

import "github.com/lni/log"

func localNodeIDField(d *DHT) log.Field {
	return log.String("myid", d.self.NodeID.Short())
}

func targetField(target nodeID) log.Field {
	return log.String("target", target.Short())
}

func fromField(rt remote) log.Field {
	return log.String("from", rt.NodeID.Short())
}
