// Copyright (c) 2019 IoTeX Foundation
// This is an alpha (internal) release and is not suitable for production. This source code is provided 'as is' and no
// warranties are given as to title or non-infringement, merchantability or fitness for purpose and, to the extent
// permitted by law, all liability for your use of the code is disclaimed. This source code is governed by Apache
// License 2.0 that can be found in the LICENSE file.

package mptrie

import (
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/iotexproject/iotex-core/db/trie"
	"github.com/iotexproject/iotex-core/db/trie/triepb"
)

type branchNode struct {
	cacheNode
	children map[byte]node
	indices  *SortedList
	isRoot   bool
}

func newBranchNode(
	cli client,
	children map[byte]node,
) (node, error) {
	if len(children) == 0 {
		return nil, errors.New("branch node children cannot be empty")
	}
	bnode := &branchNode{
		cacheNode: cacheNode{
			dirty: true,
		},
		children: children,
		indices:  NewSortedList(children),
	}
	bnode.cacheNode.serializable = bnode
	if len(bnode.children) != 0 {
		if !cli.asyncMode() {
			if _, err := bnode.store(cli); err != nil {
				return nil, err
			}
		}
	}
	return bnode, nil
}

func newRootBranchNode(cli client, children map[byte]node, dirty bool) (*branchNode, error) {
	bnode := &branchNode{
		cacheNode: cacheNode{
			dirty: dirty,
		},
		children: children,
		indices:  NewSortedList(children),
		isRoot:   true,
	}
	bnode.cacheNode.serializable = bnode
	if len(bnode.children) != 0 {
		if !cli.asyncMode() {
			_, err := bnode.store(cli)
			if err != nil {
				return nil, err
			}
		}
	}
	return bnode, nil
}

func newBranchNodeFromProtoPb(cli client, pb *triepb.BranchPb) *branchNode {
	bnode := &branchNode{
		cacheNode: cacheNode{},
		children:  make(map[byte]node, len(pb.Branches)),
	}
	for _, n := range pb.Branches {
		bnode.children[byte(n.Index)] = newHashNode(cli, n.Path)
	}
	bnode.indices = NewSortedList(bnode.children)
	bnode.cacheNode.serializable = bnode
	return bnode
}

func (b *branchNode) MarkAsRoot() {
	b.isRoot = true
}

func (b *branchNode) Children() []node {
	trieMtc.WithLabelValues("branchNode", "children").Inc()
	ret := make([]node, 0, len(b.children))
	for _, idx := range b.indices.List() {
		ret = append(ret, b.children[idx])
	}
	return ret
}

func (b *branchNode) Delete(cli client, key keyType, offset uint8) (node, error) {
	trieMtc.WithLabelValues("branchNode", "delete").Inc()
	offsetKey := key[offset]
	child, err := b.child(offsetKey)
	if err != nil {
		return nil, err
	}
	newChild, err := child.Delete(cli, key, offset+1)
	if err != nil {
		return nil, err
	}
	if newChild != nil || b.isRoot {
		return b.updateChild(cli, offsetKey, newChild, false)
	}
	switch len(b.children) {
	case 1:
		panic("branch shouldn't have 0 child after deleting")
	case 2:
		if err := b.delete(cli); err != nil {
			return nil, err
		}
		var orphan node
		var orphanKey byte
		for i, n := range b.children {
			if i != offsetKey {
				orphanKey = i
				orphan = n
				break
			}
		}
		if orphan == nil {
			panic("unexpected branch status")
		}
		if hn, ok := orphan.(*hashNode); ok {
			if orphan, err = hn.LoadNode(cli); err != nil {
				return nil, err
			}
		}
		switch node := orphan.(type) {
		case *extensionNode:
			return node.updatePath(
				cli,
				append([]byte{orphanKey}, node.path...),
				false,
			)
		case *leafNode:
			return node, nil
		default:
			return newExtensionNode(cli, []byte{orphanKey}, node)
		}
	default:
		return b.updateChild(cli, offsetKey, newChild, false)
	}
}

func (b *branchNode) Upsert(cli client, key keyType, offset uint8, value []byte) (node, error) {
	trieMtc.WithLabelValues("branchNode", "upsert").Inc()
	var newChild node
	offsetKey := key[offset]
	child, err := b.child(offsetKey)
	switch errors.Cause(err) {
	case nil:
		newChild, err = child.Upsert(cli, key, offset+1, value) // look for next key offset
	case trie.ErrNotExist:
		newChild, err = newLeafNode(cli, key, value)
	}
	if err != nil {
		return nil, err
	}

	return b.updateChild(cli, offsetKey, newChild, true)
}

func (b *branchNode) Search(cli client, key keyType, offset uint8) (node, error) {
	trieMtc.WithLabelValues("branchNode", "search").Inc()
	child, err := b.child(key[offset])
	if err != nil {
		return nil, err
	}
	return child.Search(cli, key, offset+1)
}

func (b *branchNode) proto(cli client, flush bool) (proto.Message, error) {
	trieMtc.WithLabelValues("branchNode", "serialize").Inc()
	nodes := []*triepb.BranchNodePb{}
	for _, idx := range b.indices.List() {
		c := b.children[idx]
		if flush {
			if sn, ok := c.(serializable); ok {
				var err error
				c, err = sn.store(cli)
				if err != nil {
					return nil, err
				}
			}
		}
		h, err := c.Hash(cli)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, &triepb.BranchNodePb{Index: uint32(idx), Path: h})
	}
	return &triepb.NodePb{
		Node: &triepb.NodePb_Branch{
			Branch: &triepb.BranchPb{Branches: nodes},
		},
	}, nil
}

func (b *branchNode) child(key byte) (node, error) {
	c, ok := b.children[key]
	if !ok {
		return nil, trie.ErrNotExist
	}
	return c, nil
}

func (b *branchNode) Flush(cli client) error {
	if !b.dirty {
		return nil
	}
	for _, idx := range b.indices.List() {
		if err := b.children[idx].Flush(cli); err != nil {
			return err
		}
	}
	_, err := b.store(cli)
	return err
}

func (b *branchNode) updateChild(cli client, key byte, child node, hashnode bool) (node, error) {
	if err := b.delete(cli); err != nil {
		return nil, err
	}
	var children map[byte]node
	// update branchnode with new child
	if child == nil {
		children = make(map[byte]node, len(b.children)-1)
		for k, v := range b.children {
			if k != key {
				children[k] = v
			}
		}
	} else {
		children = make(map[byte]node, len(b.children))
		for k, v := range b.children {
			children[k] = v
		}
		children[key] = child
	}

	if b.isRoot {
		bn, err := newRootBranchNode(cli, children, true)
		if err != nil {
			return nil, err
		}
		return bn, nil
	}
	return newBranchNode(cli, children)
}

func (bn *branchNode) Clone() (branch, error) {
	if bn.dirty {
		return nil, errors.New("dirty branch node cannot be cloned")
	}
	children := make(map[byte]node, len(bn.children))
	for key, child := range bn.children {
		children[key] = child
	}
	hashVal := make([]byte, len(bn.hashVal))
	copy(hashVal, bn.hashVal)
	ser := make([]byte, len(bn.ser))
	copy(ser, bn.ser)
	return &branchNode{
		cacheNode: cacheNode{
			dirty:   false,
			hashVal: hashVal,
			ser:     ser,
		},
		children: children,
		indices:  bn.indices,
		isRoot:   bn.isRoot,
	}, nil
}
