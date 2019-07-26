package hashring

import (
	"crypto/md5"
	"math"
	"sort"
	"strconv"
)

// HashKey represents hash value
type HashKey uint32

// HashKeyOrder implements sort.Interface for []HashKey.
type HashKeyOrder []HashKey

func (h HashKeyOrder) Len() int           { return len(h) }
func (h HashKeyOrder) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h HashKeyOrder) Less(i, j int) bool { return h[i] < h[j] }

// HashRing provides consistent hashing.
//
// Suppose we have 8 nodes: (nodeName:HashKey)
//     	n1:k1, n2:k2, n3:k3, n4:k4, n5:k5, n6:k6, n7:k7, n8:k8
// What stored on ring is:
// 		k1 -> k2 -> k3 -> k4
//   	↑				  ↓
// 		k8 <- k7 <- k6 <- k5
// Where the keys are stored in ascending order, which means $k1 < k2 < k3 < ...$
// Given a key with HashKey kk, it belongs to the smallest node whose HashKey is larger than kk.
// 	- Suppose that $k5 < kk < k6$, then kk belongs to n6.
// 	- If $kk > k8$, it belongs to n1.
// Actually, one node has multiple keys(virtual nodes) on ring for more balanced key distribution.
type HashRing struct {
	ring       map[HashKey]string // HashKey to node. It includes virtual nodes.
	sortedKeys []HashKey          // sorted HashKeys on ring. for binary search.
	nodes      []string
	weights    map[string]int
}

// New creates an instance of HashRing from nodes.
func New(nodes []string) *HashRing {
	hashRing := &HashRing{
		ring:       make(map[HashKey]string),
		sortedKeys: make([]HashKey, 0),
		nodes:      nodes,
		weights:    make(map[string]int),
	}
	hashRing.generateCircle()
	return hashRing
}

// NewWithWeights creates an instance of HashRing according to weights map.
func NewWithWeights(weights map[string]int) *HashRing {
	nodes := make([]string, 0, len(weights))
	for node := range weights {
		nodes = append(nodes, node)
	}
	hashRing := &HashRing{
		ring:       make(map[HashKey]string),
		sortedKeys: make([]HashKey, 0),
		nodes:      nodes,
		weights:    weights,
	}
	hashRing.generateCircle()
	return hashRing
}

// Size returns the number of nodes in HashRing.
func (h *HashRing) Size() int {
	return len(h.nodes)
}

// UpdateWithWeights updates HashRing with weights map.
func (h *HashRing) UpdateWithWeights(weights map[string]int) {
	nodesChgFlg := false
	if len(weights) != len(h.weights) {
		nodesChgFlg = true
	} else {
		for node, newWeight := range weights {
			oldWeight, ok := h.weights[node]
			if !ok || oldWeight != newWeight {
				nodesChgFlg = true
				break
			}
		}
	}

	if nodesChgFlg {
		newhring := NewWithWeights(weights)
		h.weights = newhring.weights
		h.nodes = newhring.nodes
		h.ring = newhring.ring
		h.sortedKeys = newhring.sortedKeys
	}
}

func (h *HashRing) generateCircle() {
	totalWeight := 0
	for _, node := range h.nodes {
		if weight, ok := h.weights[node]; ok {
			totalWeight += weight
		} else {
			totalWeight++
			h.weights[node] = 1
		}
	}

	for _, node := range h.nodes {
		weight := h.weights[node]

		// math.Ceil makes sure that factor would not be zero (at least one).
		factor := math.Ceil(float64(40*len(h.nodes)*weight) / float64(totalWeight))

		for j := 0; j < int(factor); j++ {
			nodeKey := node + "-" + strconv.FormatInt(int64(j), 10)
			bKey := hashDigest(nodeKey)

			// It's still a mystery why the fourth byte is discarded.
			for i := 0; i < 3; i++ {
				key := hashVal(bKey[i*4 : i*4+4])
				h.ring[key] = node
				h.sortedKeys = append(h.sortedKeys, key)
			}
		}
	}

	sort.Sort(HashKeyOrder(h.sortedKeys))
}

// GetNode returns the node that stringKey belongs to.
func (h *HashRing) GetNode(stringKey string) (node string, ok bool) {
	pos, ok := h.GetNodePos(stringKey)
	if !ok {
		return "", false
	}
	return h.ring[h.sortedKeys[pos]], true
}

// GetNodePos returns the position on ring that stringKey belongs to.
func (h *HashRing) GetNodePos(stringKey string) (pos int, ok bool) {
	if len(h.ring) == 0 {
		return 0, false
	}

	key := h.GenKey(stringKey)

	nodes := h.sortedKeys
	pos = sort.Search(len(nodes), func(i int) bool { return nodes[i] > key })

	if pos == len(nodes) {
		// Wrap the search, should return first node
		return 0, true
	}
	return pos, true
}

// GenKey generates HashKey of key.
func (h *HashRing) GenKey(key string) HashKey {
	bKey := hashDigest(key)
	return hashVal(bKey[0:4])
}

// GetNodeFrom returns the node that stringKey belongs to.
// The node returned must be in nodes.
func (h *HashRing) GetNodeFrom(stringKey string, nodes []string) (node string, ok bool) {
	nm := make(map[string]struct{})
	for _, n := range nodes {
		nm[n] = struct{}{}
	}

	pos, ok := h.GetNodePos(stringKey)
	if !ok {
		return "", false
	}

	for i := pos; i < pos+len(h.sortedKeys); i++ {
		key := h.sortedKeys[i%len(h.sortedKeys)]
		val := h.ring[key]
		if _, ok := nm[val]; ok {
			return val, ok
		}
	}
	return "", false
}

// GetNodes returns size nodes from the ring.
//
// size should be less than or equal to the number of nodes on the ring.
//
// The first node returned is where stringKey belongs.
// The other $size-1$ nodes are unique ones following on the ring.
func (h *HashRing) GetNodes(stringKey string, size int) (nodes []string, ok bool) {
	if size > len(h.nodes) || size <= 0 {
		return nil, false
	}

	pos, ok := h.GetNodePos(stringKey)
	if !ok {
		return nil, false
	}

	returnedValues := make(map[string]bool, size)
	//mergedSortedKeys := append(h.sortedKeys[pos:], h.sortedKeys[:pos]...)
	resultSlice := make([]string, 0, size)

	for i := pos; i < pos+len(h.sortedKeys); i++ {
		key := h.sortedKeys[i%len(h.sortedKeys)]
		val := h.ring[key]
		if !returnedValues[val] {
			returnedValues[val] = true
			resultSlice = append(resultSlice, val)
		}
		if len(returnedValues) == size {
			break
		}
	}

	return resultSlice, len(resultSlice) == size
}

// AddNode adds node to ring, and returns the new HashRing.
func (h *HashRing) AddNode(node string) *HashRing {
	return h.AddWeightedNode(node, 1)
}

// AddWeightedNode adds node with weight to ring, and returns the new HashRing.
func (h *HashRing) AddWeightedNode(node string, weight int) *HashRing {
	if weight <= 0 {
		return h
	}

	if _, ok := h.weights[node]; ok {
		return h
	}

	nodes := make([]string, len(h.nodes), len(h.nodes)+1)
	copy(nodes, h.nodes)
	nodes = append(nodes, node)

	weights := make(map[string]int)
	for eNode, eWeight := range h.weights {
		weights[eNode] = eWeight
	}
	weights[node] = weight

	hashRing := &HashRing{
		ring:       make(map[HashKey]string),
		sortedKeys: make([]HashKey, 0),
		nodes:      nodes,
		weights:    weights,
	}
	hashRing.generateCircle()
	return hashRing
}

// UpdateWeightedNode updates node with weight, and returns the new HashRing.
func (h *HashRing) UpdateWeightedNode(node string, weight int) *HashRing {
	if weight <= 0 {
		return h
	}

	/* node is not need to update for node is not existed or weight is not changed */
	if oldWeight, ok := h.weights[node]; (!ok) || (ok && oldWeight == weight) {
		return h
	}

	nodes := make([]string, len(h.nodes), len(h.nodes))
	copy(nodes, h.nodes)

	weights := make(map[string]int)
	for eNode, eWeight := range h.weights {
		weights[eNode] = eWeight
	}
	weights[node] = weight

	hashRing := &HashRing{
		ring:       make(map[HashKey]string),
		sortedKeys: make([]HashKey, 0),
		nodes:      nodes,
		weights:    weights,
	}
	hashRing.generateCircle()
	return hashRing
}

// RemoveNode removes node from ring, and returns the new HashRing.
func (h *HashRing) RemoveNode(node string) *HashRing {
	/* if node isn't exist in hashring, don't refresh hashring */
	if _, ok := h.weights[node]; !ok {
		return h
	}

	nodes := make([]string, 0)
	for _, eNode := range h.nodes {
		if eNode != node {
			nodes = append(nodes, eNode)
		}
	}

	weights := make(map[string]int)
	for eNode, eWeight := range h.weights {
		if eNode != node {
			weights[eNode] = eWeight
		}
	}

	hashRing := &HashRing{
		ring:       make(map[HashKey]string),
		sortedKeys: make([]HashKey, 0),
		nodes:      nodes,
		weights:    weights,
	}
	hashRing.generateCircle()
	return hashRing
}

func hashVal(bKey []byte) HashKey {
	return ((HashKey(bKey[3]) << 24) |
		(HashKey(bKey[2]) << 16) |
		(HashKey(bKey[1]) << 8) |
		(HashKey(bKey[0])))
}

// hashDigest returns the md5 sum of key.
// One key's md5 sum never changes.
func hashDigest(key string) [md5.Size]byte {
	return md5.Sum([]byte(key))
}
