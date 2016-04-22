package bptree

import (
	"github.com/lleo/util"
	"math"
	"math/rand"
	"os"
	"testing"
)

type entry struct {
	key BptKey
	val int
}

var genRandomizedEntries func(ents []entry) []entry

var largeNumEnts []entry
var veryLargeNumEnts []entry

func TestMain(m *testing.M) {
	//SETUP
	genRandomizedEntries = genRandomizedEntriesInPlace

	largeNumEnts = make([]entry, 0, 32) //binary growth
	s := util.Str("")
	nEnts := 900
	for i := 0; i < nEnts; i++ {
		s = s.Inc(1) //get off "" first
		largeNumEnts = append(largeNumEnts, entry{StringKey(string(s)), i + 1})
	}

	veryLargeNumEnts = make([]entry, 0, 32) //binary growth
	s = util.Str("")
	//nEnts = 1000000
	nEnts = 1000
	for i := 0; i < nEnts; i++ {
		s = s.Inc(1) //get off "" first
		veryLargeNumEnts = append(largeNumEnts, entry{StringKey(string(s)), i + 1})
	}

	util.RandSeed() //seeds rand.Seed() with time.Now().UnixNano()

	xit := m.Run()

	//TEARDOWN

	os.Exit(xit)
}

func genRandomizedEntriesTmpSlice(ents []entry) []entry {
	tmpEnts := make([]entry, len(ents))
	copy(tmpEnts, ents)

	randomEnts := make([]entry, 0, len(ents))
	for len(tmpEnts) > 0 {
		i := rand.Intn(len(tmpEnts))
		randomEnts = append(randomEnts, tmpEnts[i])
		//cut out i'th element from tmpEnts
		tmpEnts = append(tmpEnts[:i], tmpEnts[i+1:]...)
		//tmpEnts = tmpEnts[:i+copy(tmpEnts[i:], tmpEnts[i+1:])]
	}
	return randomEnts
}

//First genRandomizedEntries() copies []entry passed in. Then it randomizes that
//copy in-place. Finnally, it returns the randomized copy.
func genRandomizedEntriesInPlace(ents []entry) []entry {
	randEnts := make([]entry, len(ents))
	copy(randEnts, ents)

	//From: https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle#The_modern_algorithm
	for i := len(randEnts) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		randEnts[i], randEnts[j] = randEnts[j], randEnts[i]
	}

	return randEnts
}

func TestValidInOrderTree(t *testing.T) {
	bpt := NewBpTree(3)
	for _, ent := range largeNumEnts {
		bpt.Put(ent.key, ent.val)
	}
	bptv := bpt.(*tree)
	if !_validTree(t, bptv) {
		t.Logf("TREE =\n%v", bptv)
		t.Fail()
	}
}

func TestValidRandomInsertOrderTree(t *testing.T) {
	randomEnts := genRandomizedEntries(largeNumEnts)

	bpt := NewBpTree(3)
	for _, ent := range randomEnts {
		bpt.Put(ent.key, ent.val)
	}
	bptv := bpt.(*tree)
	if !_validTree(t, bptv) {
		t.Logf("TREE=\n%v", bptv)
		t.Fail()
	}
}

func TestRandomInsertOrderWithRandomGetOfAllEntries(t *testing.T) {
	randomEntsPut := genRandomizedEntries(largeNumEnts)
	randomEntsGet := genRandomizedEntries(largeNumEnts)

	bpt := NewBpTree(3)
	//for _, ent := range largeNumEnts {
	for _, ent := range randomEntsPut {
		//bpt.Put(ent.key, ent.val)
		added := bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("¿Put failed? ent.key=%q; ent.val=%d;\n", ent.key, ent.val)
			t.FailNow()
		}
	}

	//for _, ent := range largeNumEnts {
	for _, ent := range randomEntsGet {
		val, found := bpt.Get(ent.key)
		if !found {
			t.Logf("did NOT find entry for ent.key=%q", ent.key)
			t.Fail()
		}
		v := val.(int)
		if v != ent.val {
			t.Logf("found val does not equal ent.val=%v", ent.val)
			t.Fail()
		}
	}
}

func TestBuildUpTreeThenDelAllEntriesOrder3(t *testing.T) {
	order := 3
	bpt := NewBpTree(order)

	putEnts := genRandomizedEntries(largeNumEnts)
	delEnts := genRandomizedEntries(largeNumEnts)

	for _, ent := range putEnts {
		added := bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("Put didn't insert, it overwrote!?!")
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	for _, ent := range delEnts {
		val, found := bpt.Del(ent.key)
		if !found {
			t.Logf("Del failed to find ent.key=%q", ent.key)
			t.Fail()
		}
		ival := val.(int)
		if ival != ent.val {
			t.Logf("The value Del() returned %d didn't not match what was stored %d", ival, ent.val)
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	if bpt.NumberOfEntries() != 0 {
		t.Logf("bpt.NumberOfEntries() != 0")
		t.Fail()
	}

	//bpt tree should be empty now
	bptv := bpt.(*tree)
	if !bptv.root.isLeaf() {
		t.Logf("bptv.root is not leaf.")
		t.Fail()
	} else {
		rootNode := bptv.root.(*leafNodeS)
		if len(rootNode.keys) > 0 {
			t.Logf("rootNode.keys is not empty")
			t.Fail()
		}
		if len(rootNode.vals) > 0 {
			t.Logf("rootNode.vals is not empty")
			t.Fail()
		}
	}
}

func TestBuildUpTreeThenDelAllEntriesOrder4(t *testing.T) {
	order := 4
	bpt := NewBpTree(order)

	putEnts := genRandomizedEntries(largeNumEnts)
	delEnts := genRandomizedEntries(largeNumEnts)

	for _, ent := range putEnts {
		added := bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("¿Put failed?")
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	for _, ent := range delEnts {
		val, found := bpt.Del(ent.key)
		if !found {
			t.Logf("Del failed to find ent.key=%q", ent.key)
			t.Fail()
		}
		ival := val.(int)
		if ival != ent.val {
			t.Logf("The value Del() returned %d didn't not match what was stored %d", ival, ent.val)
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	//bpt tree should be empty now
	if bpt.NumberOfEntries() != 0 {
		t.Logf("bpt.NumberOfEntries() != 0")
		t.Fail()
	}

	bptv := bpt.(*tree)
	if !bptv.root.isLeaf() {
		t.Logf("bptv.root is not leaf.")
		t.Fail()
	} else {
		rootNode := bptv.root.(*leafNodeS)
		if len(rootNode.keys) > 0 {
			t.Logf("rootNode.keys is not empty")
			t.Fail()
		}
		if len(rootNode.vals) > 0 {
			t.Logf("rootNode.vals is not empty")
			t.Fail()
		}
	}
}

func TestBuildUpTreeThenDelAllEntriesOrder5(t *testing.T) {
	order := 5
	bpt := NewBpTree(order)

	putEnts := genRandomizedEntries(largeNumEnts)
	delEnts := genRandomizedEntries(largeNumEnts)

	for _, ent := range putEnts {
		added := bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("¿Put failed?")
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	for _, ent := range delEnts {
		val, found := bpt.Del(ent.key)
		if !found {
			t.Logf("Del failed to find ent.key=%q", ent.key)
			t.Fail()
		}
		ival := val.(int)
		if ival != ent.val {
			t.Logf("The value Del() returned %d didn't not match what was stored %d", ival, ent.val)
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	//bpt tree should be empty now
	if bpt.NumberOfEntries() != 0 {
		t.Logf("bpt.NumberOfEntries() != 0")
		t.Fail()
	}

	bptv := bpt.(*tree)
	if !bptv.root.isLeaf() {
		t.Logf("bptv.root is not leaf.")
		t.Fail()
	} else {
		rootNode := bptv.root.(*leafNodeS)
		if len(rootNode.keys) > 0 {
			t.Logf("rootNode.keys is not empty")
			t.Fail()
		}
		if len(rootNode.vals) > 0 {
			t.Logf("rootNode.vals is not empty")
			t.Fail()
		}
	}
}

func TestBuildUpTreeThenDelAllEntriesOrder15(t *testing.T) {
	order := 15
	bpt := NewBpTree(order)

	putEnts := genRandomizedEntries(veryLargeNumEnts)
	delEnts := genRandomizedEntries(veryLargeNumEnts)

	for _, ent := range putEnts {
		added := bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("¿Put failed?")
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	for _, ent := range delEnts {
		val, found := bpt.Del(ent.key)
		if !found {
			t.Logf("Del failed to find ent.key=%q", ent.key)
			t.Fail()
		}
		ival := val.(int)
		if ival != ent.val {
			t.Logf("The value Del() returned %d didn't not match what was stored %d", ival, ent.val)
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	//bpt tree should be empty now
	if bpt.NumberOfEntries() != 0 {
		t.Logf("bpt.NumberOfEntries() != 0")
		t.Fail()
	}

	bptv := bpt.(*tree)
	if !bptv.root.isLeaf() {
		t.Logf("bptv.root is not leaf.")
		t.Fail()
	} else {
		rootNode := bptv.root.(*leafNodeS)
		if len(rootNode.keys) > 0 {
			t.Logf("rootNode.keys is not empty")
			t.Fail()
		}
		if len(rootNode.vals) > 0 {
			t.Logf("rootNode.vals is not empty")
			t.Fail()
		}
	}
}

func TestBuildUpTreeThenDelAllEntriesOrder16(t *testing.T) {
	order := 16
	bpt := NewBpTree(order)

	putEnts := genRandomizedEntries(veryLargeNumEnts)
	delEnts := genRandomizedEntries(veryLargeNumEnts)

	for _, ent := range putEnts {
		added := bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("¿Put failed?")
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	for _, ent := range delEnts {
		val, found := bpt.Del(ent.key)
		if !found {
			t.Logf("Del failed to find ent.key=%q", ent.key)
			t.Fail()
		}
		ival := val.(int)
		if ival != ent.val {
			t.Logf("The value Del() returned %d didn't not match what was stored %d", ival, ent.val)
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	//bpt tree should be empty now
	if bpt.NumberOfEntries() != 0 {
		t.Logf("bpt.NumberOfEntries() != 0")
		t.Fail()
	}

	bptv := bpt.(*tree)
	if !bptv.root.isLeaf() {
		t.Logf("bptv.root is not leaf.")
		t.Fail()
	} else {
		rootNode := bptv.root.(*leafNodeS)
		if len(rootNode.keys) > 0 {
			t.Logf("rootNode.keys is not empty")
			t.Fail()
		}
		if len(rootNode.vals) > 0 {
			t.Logf("rootNode.vals is not empty")
			t.Fail()
		}
	}
}

func TestBuildUpTreeThenDelAllEntriesOrder31(t *testing.T) {
	order := 31
	bpt := NewBpTree(order)

	putEnts := genRandomizedEntries(veryLargeNumEnts)
	delEnts := genRandomizedEntries(veryLargeNumEnts)

	for _, ent := range putEnts {
		added := bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("¿Put failed?")
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	for _, ent := range delEnts {
		val, found := bpt.Del(ent.key)
		if !found {
			t.Logf("Del failed to find ent.key=%q", ent.key)
			t.Fail()
		}
		ival := val.(int)
		if ival != ent.val {
			t.Logf("The value Del() returned %d didn't not match what was stored %d", ival, ent.val)
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	//bpt tree should be empty now
	if bpt.NumberOfEntries() != 0 {
		t.Logf("bpt.NumberOfEntries() != 0")
		t.Fail()
	}

	bptv := bpt.(*tree)
	if !bptv.root.isLeaf() {
		t.Logf("bptv.root is not leaf.")
		t.Fail()
	} else {
		rootNode := bptv.root.(*leafNodeS)
		if len(rootNode.keys) > 0 {
			t.Logf("rootNode.keys is not empty")
			t.Fail()
		}
		if len(rootNode.vals) > 0 {
			t.Logf("rootNode.vals is not empty")
			t.Fail()
		}
	}
}

func TestBuildUpTreeThenDelAllEntriesOrder32(t *testing.T) {
	order := 32
	bpt := NewBpTree(order)

	putEnts := genRandomizedEntries(veryLargeNumEnts)
	delEnts := genRandomizedEntries(veryLargeNumEnts)

	for _, ent := range putEnts {
		added := bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("¿Put failed?")
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	for _, ent := range delEnts {
		val, found := bpt.Del(ent.key)
		if !found {
			t.Logf("Del failed to find ent.key=%q", ent.key)
			t.Fail()
		}
		ival := val.(int)
		if ival != ent.val {
			t.Logf("The value Del() returned %d didn't not match what was stored %d", ival, ent.val)
			t.Fail()
		}
		if !_validTree(t, bpt) {
			t.Logf("!_validTree(t, bpt)")
			t.Fail()
		}
	}

	//bpt tree should be empty now
	if bpt.NumberOfEntries() != 0 {
		t.Logf("bpt.NumberOfEntries() != 0")
		t.Fail()
	}

	bptv := bpt.(*tree)
	if !bptv.root.isLeaf() {
		t.Logf("bptv.root is not leaf.")
		t.Fail()
	} else {
		rootNode := bptv.root.(*leafNodeS)
		if len(rootNode.keys) > 0 {
			t.Logf("rootNode.keys is not empty")
			t.Fail()
		}
		if len(rootNode.vals) > 0 {
			t.Logf("rootNode.vals is not empty")
			t.Fail()
		}
	}
}

func _validTree(test *testing.T, t_ BpTree) bool {
	t, ok := t_.(*tree)
	if !ok {
		test.Log("failed to cast BpTree to *tree")
		test.Fail()
	}

	if !_validRootNode(test, t.root, t.order) {
		return false
	}
	if t.root.isLeaf() {
		return true //else _validRootNode(t.root) would have caught it
	}

	rootNode := t.root.(*interiorNodeS)

	nodes := make([]nodeI, 0, 2)

	//seed the nodes slice
	for i := 0; i < len(rootNode.vals); i++ {
		nodes = append(nodes, rootNode.vals[i])
	}

	for i := 0; i < len(nodes); i++ {
		if nodes[i].isLeaf() {
			n := nodes[i].(*leafNodeS)
			if !_validLeafNode(test, n, t.order) {
				test.Logf("!_validLeafNode(test, n, t.order) n=\n%v", n)
				return false
			}
		} else {
			n := nodes[i].(*interiorNodeS)
			if !_validInteriorNode(test, n, t.order) {
				test.Logf("!_validInteriorNode(test, n, t.order) n=\n%v", n)
				return false
			}
			nodes = append(nodes, n.vals...)
		}
	}
	return true
}

func _validRootNode(t *testing.T, n nodeI, order int) bool {
	if n.isLeaf() {
		n := n.(*leafNodeS)
		if !(len(n.keys) >= 0 && len(n.keys) <= order-1) {
			t.Logf("!(len(n.keys) >= 1 && len(n.keys) <= order-1) n=\n%v", n)
			return false
		}
		if len(n.keys) != len(n.vals) {
			t.Logf("len(n.keys) != len(n.vals) n=\n%v", n)
			return false
		}
	} else {
		n := n.(*interiorNodeS)
		if !(len(n.keys) >= 1 && len(n.keys) <= order-1) {
			t.Logf("!(len(n.keys),%d >= 1 && len(n.keys),%d <= order-1,%d)", len(n.keys), len(n.keys), order-1)
			return false
		}
		if len(n.keys) != len(n.vals)-1 {
			t.Logf("len(n.keys),%d != len(n.vals)-1,%d", len(n.keys), len(n.vals)-1)
			return false
		}
	}
	return true
}

func _validInteriorNode(t *testing.T, node_ nodeI, order int) bool {
	node, ok := node_.(*interiorNodeS)
	if !ok {
		lgr.Printf("The nodeI passed in is not castable to *interiorNodeS")
		return false
	}

	if !_validNodeKeys(t, node.keys, order) {
		t.Logf("!_validNodeKeys(t, node.keys, order) node=\n%v", node)
		return false
	}
	if !_validNodeVals(t, node.vals, order) {
		t.Logf("!_validNodeVals(t, node.vals, order) node=\n%v", node)
		return false
	}

	if len(node.keys) != len(node.vals)-1 {
		t.Logf("len(node.keys),%d != len(node.vals)-1,%d node=\n%v", len(node.keys), len(node.vals)-1, node)
		return false
	}
	return true
}

func _validLeafNode(t *testing.T, node_ nodeI, order int) bool {
	node, ok := node_.(*leafNodeS)
	if !ok {
		lgr.Printf("The nodeI passed in is not castable to *leafNodeS")
		return false
	}

	if !_validLeafKeys(t, node.keys, order) {
		t.Logf("!_validLeafKeys(t, node.keys, order) node=\n%v", node)
		return false
	}
	if !_validLeafVals(t, node.vals, order) {
		t.Logf("!_validLeafVals(t, node.vals, order) node=\n%v", node)
		return false
	}
	if len(node.keys) != len(node.vals) {
		t.Logf("len(node.keys),%d != len(node.vals),%d node=\n%v", len(node.keys), len(node.vals), node)
		return false
	}
	return true
}

func _validLeafKeys(t *testing.T, keys []BptKey, order int) bool {
	if !(len(keys) >= _intCeil(order-1, 2) && len(keys) <= order-1) {
		t.Logf("!(len(keys),%d >= _intCeil(order-1, 2),%d && len(keys),%d <= order-1),%d", len(keys), _intCeil(order-1, 2), len(keys), order)
		return false
	}
	return true
}

func _validLeafVals(t *testing.T, vals []interface{}, order int) bool {
	if !(len(vals) >= _intCeil(order-1, 2) && len(vals) <= order-1) {
		t.Logf("!(len(vals),%d >= _intCeil(order-1, 2),%d && len(vals),%d <= order-1),%d", len(vals), _intCeil(order-1, 2), len(vals), order)
		return false
	}
	return true
}

func _validNodeKeys(t *testing.T, keys []BptKey, order int) bool {
	if !(len(keys) >= _intCeil(order, 2)-1 && len(keys) <= order-1) {
		t.Logf("!(len(keys),%d >= _intCeil(order, 2)-1,%d && len(keys),%d <= order-1),%d", len(keys), _intCeil(order, 2)-1, len(keys), order)
		return false
	}
	return true
}

func _validNodeVals(t *testing.T, vals []nodeI, order int) bool {
	if !(len(vals) >= _intCeil(order, 2) && len(vals) <= order) {
		t.Logf("!(len(vals),%d >= _intCeil(order, 2),%d && len(vals),%d <= order,%d)", len(vals), _intCeil(order, 2), len(vals), order)
		return false
	}
	return true
}

func _intCeil(n, d int) int {
	return int(math.Ceil(float64(n) / float64(d)))
}
