package bptree

import (
	"fmt"
	"strings"
)

type pathT []*interiorNodeS

//Constructs an empty pathT object.
func newPathT() pathT {
	return pathT(make([]*interiorNodeS, 0, 2))
}

//path.pop() returns the last entry inserted with path.push(...).
func (path *pathT) pop() *interiorNodeS {
	if len(*path) == 0 {
		//should I do this or let the runtime panic on index out of range
		return nil
	}
	tpath := (*path)[len(*path)-1]
	*path = (*path)[:len(*path)-1]
	return tpath

}

//Put a new *interiorNodeS in the path object.
//You should never push nil, but we are not checking to prevent this.
func (path *pathT) push(node *interiorNodeS) {
	//_ = ASSERT && Assert(node != nil, "pathT.push(nil) not allowed")
	*path = append(*path, node)
}

//path.isEmpty() returns true if there are no entries in the path object,
//otherwise it returns false.
func (path *pathT) isEmpty() bool {
	return len(*path) == 0
}

//Convert path to a string representation. This is only good for debug messages.
//It is not a string format to convert back from.
func (path *pathT) String() string {
	s := "["
	pvs := []*interiorNodeS(*path)
	strs := make([]string, 0, 2)
	for _, pv := range pvs {
		strs = append(strs, fmt.Sprintf("%p", pv))
	}
	s += strings.Join(strs, " ")
	s += "]"

	return s
}
