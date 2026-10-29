package kvsrv

// An in-memory B-tree keyed by string, used by KVServer so that Range
// only touches the keys inside [low, high] instead of the whole store.
// kvsrv never deletes keys, so the tree supports lookup, insert/update
// and ordered range scans only.

const btreeDegree = 32 // every node except the root has [degree-1, 2*degree-1] items

type btreeItem struct {
	key string
	vv  ValueVersion
}

type btreeNode struct {
	items    []btreeItem
	children []*btreeNode // nil for leaves
}

type BTree struct {
	root *btreeNode
	size int
}

func NewBTree() *BTree {
	return &BTree{root: &btreeNode{}}
}

func (t *BTree) Len() int {
	return t.size
}

// find returns the index of the first item in n whose key is >= key,
// and whether that item's key equals key.
func (n *btreeNode) find(key string) (int, bool) {
	lo, hi := 0, len(n.items)
	for lo < hi {
		mid := (lo + hi) / 2
		if n.items[mid].key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, lo < len(n.items) && n.items[lo].key == key
}

func (t *BTree) Get(key string) (ValueVersion, bool) {
	n := t.root
	for {
		i, found := n.find(key)
		if found {
			return n.items[i].vv, true
		}
		if n.children == nil {
			return ValueVersion{}, false
		}
		n = n.children[i]
	}
}

// Set inserts key or replaces its value.
func (t *BTree) Set(key string, vv ValueVersion) {
	if len(t.root.items) == 2*btreeDegree-1 {
		old := t.root
		t.root = &btreeNode{children: []*btreeNode{old}}
		t.root.splitChild(0)
	}
	if t.root.insertNonFull(btreeItem{key, vv}) {
		t.size++
	}
}

// splitChild splits the full child i of n around its median item.
func (n *btreeNode) splitChild(i int) {
	child := n.children[i]
	mid := btreeDegree - 1
	median := child.items[mid]

	right := &btreeNode{items: append([]btreeItem(nil), child.items[mid+1:]...)}
	if child.children != nil {
		right.children = append([]*btreeNode(nil), child.children[mid+1:]...)
		child.children = child.children[:mid+1]
	}
	child.items = child.items[:mid]

	n.items = append(n.items, btreeItem{})
	copy(n.items[i+1:], n.items[i:])
	n.items[i] = median

	n.children = append(n.children, nil)
	copy(n.children[i+2:], n.children[i+1:])
	n.children[i+1] = right
}

// insertNonFull returns true if a new key was added (false on update).
func (n *btreeNode) insertNonFull(item btreeItem) bool {
	for {
		i, found := n.find(item.key)
		if found {
			n.items[i].vv = item.vv
			return false
		}
		if n.children == nil {
			n.items = append(n.items, btreeItem{})
			copy(n.items[i+1:], n.items[i:])
			n.items[i] = item
			return true
		}
		if len(n.children[i].items) == 2*btreeDegree-1 {
			n.splitChild(i)
			if item.key == n.items[i].key {
				n.items[i].vv = item.vv
				return false
			}
			if item.key > n.items[i].key {
				i++
			}
		}
		n = n.children[i]
	}
}

// AscendRange calls fn for every key in [low, high] in ascending order
// until fn returns false.
func (t *BTree) AscendRange(low, high string, fn func(key string, vv ValueVersion) bool) {
	t.root.ascendRange(low, high, fn)
}

func (n *btreeNode) ascendRange(low, high string, fn func(string, ValueVersion) bool) bool {
	i, _ := n.find(low)
	for ; i <= len(n.items); i++ {
		if n.children != nil {
			if !n.children[i].ascendRange(low, high, fn) {
				return false
			}
		}
		if i == len(n.items) {
			break
		}
		it := n.items[i]
		if it.key > high {
			return false
		}
		if !fn(it.key, it.vv) {
			return false
		}
	}
	return true
}
