package main

import (
	"flag"
	"fmt"
	"github.com/lleo/bptree"
	"github.com/lleo/util"
	"math/rand"
	"os"
	"time"
)

type entry struct {
	key bptree.BptKey
	val int
}

func (e entry) String() string {
	return fmt.Sprintf("{%q %d}", e.key.String(), e.val)
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

	for i := len(randEnts) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		randEnts[i], randEnts[j] = randEnts[j], randEnts[i]
	}

	return randEnts
}

func main() {
	var numEnts int
	flag.IntVar(&numEnts, "n", 52, "number of entries Put() in tree")

	var dontPrintOps bool
	flag.BoolVar(&dontPrintOps, "dont-print-ops", false, "don't print ops like Put()/Get()/Del()")

	var random bool
	flag.BoolVar(&random, "r", false, "if random insertion should be used")

	var order int
	flag.IntVar(&order, "o", 3, "order of the B+Tree")

	var seed int64
	flag.Int64Var(&seed, "s", time.Now().UnixNano(), "number to seed the random number generator with")

	var dontDoDels bool
	flag.BoolVar(&dontDoDels, "dont-do-dels", false, "don't del each key from tree")

	var dontDoGets bool
	flag.BoolVar(&dontDoGets, "dont-do-gets", false, "don't get each key&val from tree")

	var printTreeAfterBuild bool
	flag.BoolVar(&printTreeAfterBuild, "print-tree-after-build", false, "print the tree after it's been built")

	var printTreeAtEnd bool
	flag.BoolVar(&printTreeAtEnd, "print-tree-at-end", false, "print the tree after end of all ops")

	var printTreeAfterPuts bool
	flag.BoolVar(&printTreeAfterPuts, "print-tree-after-puts", false, "print the tree after each tree.Put() call")

	var printTreeAfterDels bool
	flag.BoolVar(&printTreeAfterDels, "print-tree-after-dels", false, "print the tree after each tree.Del() call")

	var printEntries bool
	flag.BoolVar(&printEntries, "print-entries", false, "Print the pregenerated inorder entries and randomized entries")

	var dontPrintSeed bool
	flag.BoolVar(&dontPrintSeed, "dont-print-seed", false, "Print the rand seed to recreate a randomized test case")

	var genRandomTmpSlice bool
	flag.BoolVar(&genRandomTmpSlice, "gen-random-tmp-slice", false, "Generate randomized entries with the tmp-slice method. Or else we default to the in-place method.")

	//var genRandomInPlace
	//flag.BoolVar(&genRandomInPlace, "gen-random-in-place", true, "Generate randomized entries with the in-place method")

	flag.Parse()

	var genRandomizedEntries func(ents []entry) []entry
	if genRandomTmpSlice {
		genRandomizedEntries = genRandomizedEntriesTmpSlice
	} else {
		genRandomizedEntries = genRandomizedEntriesInPlace
	}

	//util.RandSeed()
	rand.Seed(seed)

	ents := make([]entry, 0, 2)
	s := util.Str("")
	for i := 0; i < numEnts; i++ {
		s = s.Inc(1) //get off of "" first
		ents = append(ents, entry{bptree.StringKey(string(s)), i + 1})
	}

	bpt := bptree.NewBpTree(order)

	fmt.Print(">>> Doing Puts ")
	if random {
		fmt.Println("...in random order.")

		randomEnts := genRandomizedEntries(ents)

		if printEntries {
			fmt.Println("ents =", ents)
			fmt.Println("randomEnts =", randomEnts)
		}

		for _, ent := range randomEnts {
			if !dontPrintOps {
				fmt.Printf("*** Put(%q, %d)\n", ent.key, ent.val)
			}
			added := bpt.Put(ent.key, ent.val)
			if !added {
				fmt.Printf("failed to bpt.Put(%q)\n", ent.key)
			}
			if printTreeAfterPuts {
				fmt.Println(bpt)
			}
		}
	} else {
		fmt.Println("...in order.")
		for _, ent := range ents {
			if !dontPrintOps {
				fmt.Printf("*** Put(%q, %d)\n", ent.key, ent.val)
			}
			bpt.Put(ent.key, ent.val)
			if printTreeAfterPuts {
				fmt.Println(bpt)
			}
		}
	}

	if printTreeAfterBuild {
		fmt.Println("------------------")
		fmt.Println("Printing Tree...")
		fmt.Println(bpt)
	} else {
		fmt.Println("------------------")
		fmt.Printf("Tree has %d entries after Put()s.\n", bpt.NumberOfEntries())
	}

	if !dontDoGets {
		fmt.Print(">>> Doing Gets ")
		if random {
			fmt.Println("...in random order.")

			randomEnts := genRandomizedEntries(ents)

			for _, ent := range randomEnts {
				if !dontPrintOps {
					fmt.Printf("*** Get(%q) ", ent.key)
				}
				val, found := bpt.Get(ent.key)
				if !found {
					if !dontPrintOps {
						fmt.Printf("not found\n")
					}
				} else {
					if !dontPrintOps {
						fmt.Printf("=> %v\n", val)
					}
					v := val.(int)
					if v != ent.val {
						fmt.Printf("Value retrieved: val,%d != ent.val,%d !!!\n", v, ent.val)
					}
				}
			}
		} else {
			fmt.Println("...in order.")
			for _, ent := range ents {
				val, found := bpt.Get(ent.key)
				if !dontPrintOps {
					fmt.Printf("*** Get(%q) ", ent.key)
				}
				if !found {
					if !dontPrintOps {
						fmt.Printf("not found\n")
					}
				} else {
					if !dontPrintOps {
						fmt.Printf("=> %v\n", val)
					}
					v := val.(int)
					if v != ent.val {
						fmt.Printf("Value retrieved: val,%d != ent.val,%d !!!\n", v, ent.val)
					}
				}
			}
		}
	}

	if !dontDoDels {
		//bptree.DebugKey = bptree.StringKey("a")
		//Delete
		fmt.Print(">>> Doing Dels ")
		if random {
			fmt.Println("...in random order.")

			randomEnts := genRandomizedEntries(ents)

			for _, ent := range randomEnts {
				if !dontPrintOps {
					fmt.Printf("*** Del(%q) ", ent.key)
				}
				val, found := bpt.Del(ent.key)
				if !found {
					if !dontPrintOps {
						fmt.Printf("not found\n", ent.key)
					}
					panic(fmt.Sprintf("tree.Del(%q) not found\n", ent.key))
				} else {
					if !dontPrintOps {
						fmt.Printf("=> %d\n", val)
					}
					v := val.(int)
					if v != ent.val {
						fmt.Printf("Value retrived: val,%d != ent.val,%d !!!\n", v, ent.val)
					}
				}
				if printTreeAfterDels {
					fmt.Printf("after Del(%q):\n%v", ent.key, bpt)
				}
			}
		} else {
			fmt.Println(" ...in order.")
			for _, ent := range ents {
				if !dontPrintOps {
					fmt.Printf("*** Del(%q) ", ent.key)
				}
				val, found := bpt.Del(ent.key)
				if !found {
					if !dontPrintOps {
						fmt.Printf("not found\n", ent.key)
					}
					panic(fmt.Sprintf("tree.Del(%q) not found\n", ent.key))
				} else {
					if !dontPrintOps {
						fmt.Printf("=> %d\n", val)
					}
					v := val.(int)
					if v != ent.val {
						fmt.Printf("Value retrieved: val,%d != ent.val,%d !!!\n", v, ent.val)
					}
				}
				if printTreeAfterDels {
					fmt.Printf("after Del(%q):\n%v", ent.key, bpt)
				}
			}
		}

	}

	if printTreeAtEnd {
		fmt.Println("------------------")
		fmt.Println("Printing Tree...")
		fmt.Println("[should be empty]")
		fmt.Println(bpt)
	} else {
		fmt.Println("------------------")
		fmt.Printf("Final tree has %d entries.\n", bpt.NumberOfEntries())
	}

	fmt.Println("------------------")
	fmt.Println("Settings:")
	fmt.Println("order   =", order)
	fmt.Println("numEnts =", numEnts)
	if random && !dontPrintSeed {
		fmt.Printf("seed    = %d\n", seed)
	}
	if random {
		if genRandomTmpSlice {
			fmt.Println("using tmp-slice method for generating randomized entries")
		} else {
			fmt.Println("using in-place method for generating randomized entries")
		}
	}

	os.Exit(0)
}
