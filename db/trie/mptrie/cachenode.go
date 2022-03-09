// Copyright (c) 2020 IoTeX
// This is an alpha (internal) release and is not suitable for production. This source code is provided 'as is' and no
// warranties are given as to title or non-infringement, merchantability or fitness for purpose and, to the extent
// permitted by law, all liability for your use of the code is disclaimed. This source code is governed by Apache
// License 2.0 that can be found in the LICENSE file.

package mptrie

import (
	"google.golang.org/protobuf/proto"
)

type cacheNode struct {
	dirty bool
	serializable
	hashVal []byte
	ser     []byte
}

func (cn *cacheNode) Hash(c client) ([]byte, error) {
	return cn.hash(c, false)
}

func (cn *cacheNode) hash(cli client, flush bool) ([]byte, error) {
	if cn.hashVal != nil {
		return cn.hashVal, nil
	}
	pb, err := cn.proto(cli, flush)
	if err != nil {
		return nil, err
	}
	ser, err := proto.Marshal(pb)
	if err != nil {
		return nil, err
	}

	cn.ser = ser
	cn.hashVal = cli.hash(ser)

	return cn.hashVal, nil
}

func (cn *cacheNode) delete(c client) error {
	if !cn.dirty {
		h, err := cn.hash(c, false)
		if err != nil {
			return err
		}
		if err := c.deleteNode(h); err != nil {
			return err
		}
	}
	cn.hashVal = nil
	cn.ser = nil

	return nil
}

func (cn *cacheNode) store(cli client) (node, error) {
	h, err := cn.hash(cli, true)
	if err != nil {
		return nil, err
	}
	if cn.dirty {
		if err := cli.putNode(h, cn.ser); err != nil {
			return nil, err
		}
		cn.dirty = false
	}
	return newHashNode(cli, h), nil
}
