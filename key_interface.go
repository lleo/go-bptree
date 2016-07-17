package bptree

//BptKey is the interface the user must implement to create their own BptKey
//type. The only provided one is StringKey with my own interpretation of what
//less-than should mean for strings.
type BptKey interface {
	Equals(BptKey) bool
	LessThan(BptKey) bool
	String() string
}
