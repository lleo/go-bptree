/*
Package bptree implements a B+Tree in go.
*/
package bptree

import (
	"fmt"
	"log"
	"os"
)

//BpTree implements all the User facing API of the B+Tree implementation.
//
type BpTree interface {
	Order() int
	Get(BptKey) (interface{}, bool)
	Put(BptKey, interface{}) bool
	Del(BptKey) (interface{}, bool)
	String() string
	NumberOfEntries() int
}

//BptKey is the interface the user must implement to create their own BptKey
//type. The only provided one is StringKey with my own interpretation of what
//less-than should mean for strings.
type BptKey interface {
	Equals(BptKey) bool
	LessThan(BptKey) bool
	String() string
}

type nodeI interface {
	String() string
	equals(nodeI) bool
	isToBig() bool
	findPeerLeft(*interiorNodeS) (nodeI, BptKey)
	findPeerRight(*interiorNodeS) (nodeI, BptKey)
	isLeaf() bool
	findLeftMostKey() BptKey
	order() int
	size() int
	halfFullSize() int
	//Modifying Ops
	insert(key BptKey, val interface{}) bool
	split() (nodeI, BptKey)
	stealLeft(nodeI)
	stealRight(nodeI)
	mergeRight(nodeI)
}

var lgr = log.New(os.Stderr, "[bptree] ", log.Lshortfile)

type tree struct {
	root    nodeI
	order   int
	numEnts int
}

func mkTree(order int) *tree {
	var t = new(tree)
	t.root = mkLeaf(order)
	t.order = order
	t.numEnts = 0
	return t
}

func (t *tree) newNode(k BptKey, l, r nodeI) *interiorNodeS {
	node := mkNode(t.order)
	node.keys = append(node.keys, k)
	node.vals = append(node.vals, l, r)
	return node
}

//NewBpTree instantiates a new B+Tree for a given order. The order controls
//the number of index keys and node children for inner tree nodes, and
//key/value pairs at the leaf node level. The general rule is that nodes
//fluxuate between about half full of entries, ceil(order/2), at the low end
//and order-1 entries when they are considered full.
//
//For this implementation we support order=3 and up. An order=32 B+Tree can
//cover 2 Billion (2GB) entries in at most 7 levels. That means scanning nodes
//between 16 and 31 entries 7 times to find any entry in the index. And
//practically that is more towards 16 than 31 and 6 times is more common than 7.
//
//The B+Tree order is a constant for the life of a B+Tree.
func NewBpTree(order int) BpTree {
	if order < 3 {
		lgr.Panic("Cannot make a BpTree with lessthan order=3")
	}
	return mkTree(order)
}

//Order returns the order of the *tree
//
func (t *tree) Order() int {
	return t.order
}

//String creates a string representation of the *tree structure.
//
func (t *tree) String() string {
	s := ""
	if t.root.isLeaf() {
		rootLeaf := t.root.(*leafNodeS)
		s += fmt.Sprintf("TREE: root=%p; order=%d;\n", rootLeaf, t.order)
		s += "\n"
		s += fmt.Sprint(rootLeaf)
	} else { // t.root is an interiorNodeS
		rootNode := t.root.(*interiorNodeS)

		s += fmt.Sprintf("TREE: root=%p; order=%d\n", rootNode, t.order)
		s += "\n"
		s += fmt.Sprint(rootNode)

		nodes := make([]nodeI, 0, 2)

		//seed the nodes slice
		for i := 0; i < len(rootNode.vals); i++ {
			nodes = append(nodes, rootNode.vals[i])
		}

		//printing nodes and conditionally adding new nodes
		for i := 0; i < len(nodes); i++ {
			s += fmt.Sprint(nodes[i])
			if !nodes[i].isLeaf() {
				tNode := nodes[i].(*interiorNodeS)
				nodes = append(nodes, tNode.vals...)
			}
		}
	}
	return s
}

//NumberOfEntries() returns the number of entries in the B+Tree.
//
func (t *tree) NumberOfEntries() int {
	return t.numEnts
}

//Get(key) returns the value stored for key, and a boolean that indicates
//if it was found or not.
//
func (t *tree) Get(key BptKey) (interface{}, bool) {
	path := newPathT()

	//Find a Leaf matching BptKey from the root of *tree
	leaf := t.findLeaf(key, &path)

	for i, k := range leaf.keys {
		if key.Equals(k) {
			return leaf.vals[i], true
		}
	}

	return nil, false
}

// tree.Put(k, v) returns true iff a new a new (key,value) pair was inserted
// tree.Put(k, v) returns false iff a value for key was replaced
func (t *tree) Put(key BptKey, val interface{}) bool {
	path := newPathT()

	//Find a Leaf matching BptKey from the root of *tree
	leaf := t.findLeaf(key, &path)

	added := leaf.insert(key, val)
	if added {
		t.numEnts++
	}

	if leaf.isToBig() {
		//Found a full Leaf=n
		// split Leaf
		rightLeaf, rightKey := leaf.split()

		//leaf is shrunk to half its size the rest is rightLeaf
		// this preserves the leafs spot in the parent keys & vals

		if path.isEmpty() {
			t.root = t.newNode(rightKey, leaf, rightLeaf)
		} else {
			parent := path.pop()

			parent.insert(rightKey, rightLeaf)

			for parent.isToBig() {
				rightNode, rightKey := parent.split()

				//if len(path) == 0 {
				if path.isEmpty() {
					t.root = t.newNode(rightKey, parent, rightNode)
					break
				}

				parent = path.pop()

				parent.insert(rightKey, rightNode)
			}
		}
	}

	return added
}

// tree.Del(key) returns the (value, true) if the key was found.
// tree.Del(key) returns (nil, false) if the key was not found.
func (t *tree) Del(key BptKey) (interface{}, bool) {
	path := newPathT()

	//Find a Leaf matching BptKey from the root of *tree
	//leaf := t.findLeaf(key, &path)
	leaf := t.findLeaf(key, &path)

	var val interface{}
	var found bool
	for i, k := range leaf.keys {
		if key.Equals(k) {
			val = leaf.vals[i]
			found = true

			leaf.keys = append(leaf.keys[:i], leaf.keys[i+1:]...)
			leaf.vals = append(leaf.vals[:i], leaf.vals[i+1:]...)

			t.numEnts--

			break
		}
	}

	if t.isRoot(leaf) {
		return val, found
	}

	parent := path.pop()

	if leaf.size() >= leaf.halfFullSize() {
		//fine nothing more to do
		return val, found
	}
	// ELSE leaf.size() < leaf.halfFullSize()

	leftLeaf, leftKey := leaf.findPeerLeft(parent)
	if leftLeaf != nil {

		if leftLeaf.size() > leftLeaf.halfFullSize() {
			leaf.stealLeft(leftLeaf)

			parent.swapKeys(leftKey, leaf.findLeftMostKey())
			return val, found
		}

	}

	rightLeaf, rightKey := leaf.findPeerRight(parent)

	if rightLeaf != nil {

		if rightLeaf.size() > rightLeaf.halfFullSize() {
			leaf.stealRight(rightLeaf)

			parent.swapKeys(rightKey, rightLeaf.findLeftMostKey())

			return val, found
		}

	}

	// If neither peer is has a stealable entry (both are exactly
	// half full or dont exist), then merge with one of the peers.
	// For this implementation try left peer then right.
	// Merge op needs to delete the right node's key in the parent.

	var mergedLeaf nodeI
	var deadLeaf nodeI
	if leftLeaf == nil && rightLeaf == nil {
		lgr.Panic("leftLeaf == nil && rightLeaf == nil; should not be able to happend outside order==2 which we don't support.")
	}
	//else either or both leftLeaf&rightLeaf != nil
	if leftLeaf != nil {
		leftLeaf.mergeRight(leaf)
		mergedLeaf = leftLeaf
		deadLeaf = leaf
	} else if rightLeaf != nil {
		leaf.mergeRight(rightLeaf)
		mergedLeaf = leaf
		deadLeaf = rightLeaf
	}

	t.delUp(parent, mergedLeaf, deadLeaf, path)

	return val, found
}

func (t *tree) Graph() string {
	return ""
}

func (t *tree) isRoot(node nodeI) bool {
	return t.root.equals(node)
}

func (t *tree) findLeaf(key BptKey, path *pathT) *leafNodeS {
	nextNode := t.root
	for !nextNode.isLeaf() {
		// curNode is the current node being examined.
		curNode := nextNode.(*interiorNodeS)

		path.push(curNode)
		var i int
		for i = 0; i < len(curNode.keys); i++ {
			if key.LessThan(curNode.keys[i]) {
				// set new node to explore
				nextNode = curNode.vals[i]
				break // guaranteed i != len(curNode.keys)
			}
		}
		if i == len(curNode.keys) {
			//lastValIdx := len(curNode.vals) - 1
			// set new node to explore
			//nextNode = curNode.vals[lastValIdx]
			nextNode = curNode.vals[i]
		}
	}
	leafNode := nextNode.(*leafNodeS)
	return leafNode
}

func (t *tree) delUp(parent *interiorNodeS, mergedNode, deadNode nodeI, path pathT) {
	//ALL merges are rNode.mergeRight(lNode)

	//mergedNode is an unchanged except it appended the keys&vals of deadNode
	//so its position in parent remains unchanged

	var i int
	for i = 0; i < len(parent.vals); i++ {
		curNode := parent.vals[i]
		if deadNode.equals(curNode) {
			//Given that mergedNode is always the leftNode and deadNode is
			//the node immediately after mergedNode there is always a
			//parent.keys[i-1]

			parent.keys = append(parent.keys[:i-1], parent.keys[i:]...)
			parent.vals = append(parent.vals[:i], parent.vals[i+1:]...)

			break
		}
	}

	//Did I just shrink the Root?
	if t.isRoot(parent) {

		//And is it small enough to kill in the bath tub?
		if len(parent.keys) == 0 {
			t.root = parent.vals[0]
		}

		return
	}

	grandParent := path.pop()

	if parent.size() >= parent.halfFullSize() {
		//nothing to do

		return
	}
	//ELSE parent.size() < parent.halfFullSize()

	//Try to steal from the left sibling; she didn't like me anyways.
	//leftNode, leftKey := t.findPeerLeft(grandParent, parent)
	leftNode, leftKey := parent.findPeerLeft(grandParent)

	if leftNode != nil {

		if leftNode.size() > leftNode.halfFullSize() {
			//parent.nodeStealLeft(leftNode, leftKey, grandParent)
			parent.stealLeft(leftNode)

			grandParent.swapKeys(leftKey, parent.findLeftMostKey())

			return
		}
	}

	//Try to steal from the right sibling; he owes me big for the weed I scored for him.
	//rightNode, rightKey := t.findPeerRight(grandParent, parent)
	rightNode, rightKey := parent.findPeerRight(grandParent)

	if rightNode != nil {

		if rightNode.size() > rightNode.halfFullSize() {
			parent.stealRight(rightNode)

			grandParent.swapKeys(rightKey, rightNode.findLeftMostKey())

			return
		}
	}

	//Can't Steal Must Merge! there is something biblical about this.

	var mNode nodeI
	var dNode nodeI
	if leftNode == nil && rightNode == nil {
		lgr.Panic("leftNode == nil && rightNode == nil; should not be able to happend outside order==2 which we don't support.")
	}
	if leftNode != nil {
		leftNode.mergeRight(parent)
		mNode = leftNode
		dNode = parent
	} else if rightNode != nil {
		parent.mergeRight(rightNode)
		mNode = parent
		dNode = rightNode
	}

	//recursing into grandParent
	t.delUp(grandParent, mNode, dNode, path)

	return
} //end: func (t *tree) delUp(...)
