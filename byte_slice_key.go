package bptree

type ByteSliceKey []byte

func (k0 ByteSliceKey) Equals(K1 BptKey) bool {
	k1, ok := K1.(ByteSliceKey)
	if !ok {
		lgr.Printf("incompatable BptKey = %v\n", K1)
		return false
	}
	if len(k0) != len(k1) {
		return false
	}
	for i := range k0 {
		if k0[i] != k1[i] {
			return false
		}
	}
	return true
}

func (k0 ByteSliceKey) LessThan(K1 BptKey) bool {
	k1, ok := K1.(ByteSliceKey)
	if !ok {
		lgr.Printf("incompatable BptKey = %v\n", K1)
		return false
	}
	if len(k0) < len(k1) {
		return true
	}
	if len(k0) > len(k1) {
		return false
	}
	for i := range k0 {
		if k0[i] < k1[i] {
			return true
		}
	}
	return false
}

func (k ByteSliceKey) String() string {
	//FIXME: this is probably not good for non-utf8 string []byte
	return string(k)
}
