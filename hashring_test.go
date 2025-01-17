package hashring

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func expectNode(t *testing.T, hashRing *HashRing, key string, expectedNode string) {
	node, ok := hashRing.GetNode(key)
	if !ok || node != expectedNode {
		t.Error("GetNode(", key, ") expected", expectedNode, "but got", node)
	}
}

func expectNodes(t *testing.T, hashRing *HashRing, key string, expectedNodes []string) {
	nodes, ok := hashRing.GetNodes(key, 2)
	sliceEquality := reflect.DeepEqual(nodes, expectedNodes)
	if !ok || !sliceEquality {
		t.Error("GetNodes(", key, ") expected", expectedNodes, "but got", nodes)
	}
}

func expectWeights(t *testing.T, hashRing *HashRing, expectedWeights map[string]int) {
	weightsEquality := reflect.DeepEqual(hashRing.weights, expectedWeights)
	if !weightsEquality {
		t.Error("Weights expected", expectedWeights, "but got", hashRing.weights)
	}
}

func expectNodesABC(t *testing.T, hashRing *HashRing) {
	// Python hash_ring module test case
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "a")
}

func expectNodeRangesABC(t *testing.T, hashRing *HashRing) {
	expectNodes(t, hashRing, "test", []string{"a", "b"})
	expectNodes(t, hashRing, "test", []string{"a", "b"})
	expectNodes(t, hashRing, "test1", []string{"b", "c"})
	expectNodes(t, hashRing, "test2", []string{"b", "a"})
	expectNodes(t, hashRing, "test3", []string{"c", "a"})
	expectNodes(t, hashRing, "test4", []string{"c", "b"})
	expectNodes(t, hashRing, "test5", []string{"a", "c"})
	expectNodes(t, hashRing, "aaaa", []string{"b", "a"})
	expectNodes(t, hashRing, "bbbb", []string{"a", "b"})
}

func expectNodesABCD(t *testing.T, hashRing *HashRing) {
	// Somehow adding d does not load balance these keys...
	expectNodesABC(t, hashRing)
}

func TestNew(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	hashRing := New(nodes)

	expectNodesABC(t, hashRing)
	expectNodeRangesABC(t, hashRing)
}

func TestNewEmpty(t *testing.T) {
	nodes := []string{}
	hashRing := New(nodes)

	node, ok := hashRing.GetNode("test")
	if ok || node != "" {
		t.Error("GetNode(test) expected (\"\", false) but got (", node, ",", ok, ")")
	}

	nodes, rok := hashRing.GetNodes("test", 2)
	if rok || !(len(nodes) == 0) {
		t.Error("GetNode(test) expected ( [], false ) but got (", nodes, ",", rok, ")")
	}
}

func TestForMoreNodes(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	hashRing := New(nodes)

	nodes, ok := hashRing.GetNodes("test", 5)
	if ok || !(len(nodes) == 0) {
		t.Error("GetNode(test) expected ( [], false ) but got (", nodes, ",", ok, ")")
	}
}

func TestForEqualNodes(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	hashRing := New(nodes)

	nodes, ok := hashRing.GetNodes("test", 3)
	if !ok && (len(nodes) == 3) {
		t.Error("GetNode(test) expected ( [a b c], true ) but got (", nodes, ",", ok, ")")
	}
}

func TestNewSingle(t *testing.T) {
	nodes := []string{"a"}
	hashRing := New(nodes)

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "a")
	expectNode(t, hashRing, "test2", "a")
	expectNode(t, hashRing, "test3", "a")

	// This triggers the edge case where sortedKey search resulting in not found
	expectNode(t, hashRing, "test14", "a")

	expectNode(t, hashRing, "test15", "a")
	expectNode(t, hashRing, "test16", "a")
	expectNode(t, hashRing, "test17", "a")
	expectNode(t, hashRing, "test18", "a")
	expectNode(t, hashRing, "test19", "a")
	expectNode(t, hashRing, "test20", "a")
}

func TestNewWeighted(t *testing.T) {
	weights := make(map[string]int)
	weights["a"] = 1
	weights["b"] = 2
	weights["c"] = 1
	hashRing := NewWithWeights(weights)

	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "b")
	expectNode(t, hashRing, "test5", "b")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "a")

	expectNodes(t, hashRing, "test", []string{"b", "a"})
}

func TestNewUnbalancedWeighted(t *testing.T) {
	weights := make(map[string]int)
	weights["a"] = 1
	weights["b"] = 80
	hashRing := NewWithWeights(weights)

	expectNodes(t, hashRing, "test", []string{"b", "a"})
}

func TestRemoveNode(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	hashRing := New(nodes)
	hashRing = hashRing.RemoveNode("b")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "c") // Migrated to c from b
	expectNode(t, hashRing, "test2", "a") // Migrated to a from b
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "a") // Migrated to a from b
	expectNode(t, hashRing, "bbbb", "a")

	expectNodes(t, hashRing, "test", []string{"a", "c"})
}

func TestAddNode(t *testing.T) {
	nodes := []string{"a", "c"}
	hashRing := New(nodes)
	hashRing = hashRing.AddNode("b")

	expectNodesABC(t, hashRing)

	defaultWeights := map[string]int{
		"a": 1,
		"b": 1,
		"c": 1,
	}
	expectWeights(t, hashRing, defaultWeights)
}

func TestAddNode2(t *testing.T) {
	nodes := []string{"a", "c"}
	hashRing := New(nodes)
	hashRing = hashRing.AddNode("b")
	hashRing = hashRing.AddNode("b")

	expectNodesABC(t, hashRing)
	expectNodeRangesABC(t, hashRing)
}

func TestAddNode3(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	hashRing := New(nodes)
	hashRing = hashRing.AddNode("d")

	// Somehow adding d does not load balance these keys...
	expectNodesABCD(t, hashRing)

	hashRing = hashRing.AddNode("e")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "e") // Migrated to e from a

	expectNodes(t, hashRing, "test", []string{"a", "b"})

	hashRing = hashRing.AddNode("f")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "f") // Migrated to f from b
	expectNode(t, hashRing, "test3", "f") // Migrated to f from c
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "f") // Migrated to f from a
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "e")

	expectNodes(t, hashRing, "test", []string{"a", "b"})
}

func TestDuplicateNodes(t *testing.T) {
	nodes := []string{"a", "a", "a", "a", "b"}
	hashRing := New(nodes)

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "a")
	expectNode(t, hashRing, "test4", "b")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "a")
}

func TestAddWeightedNode(t *testing.T) {
	nodes := []string{"a", "c"}
	hashRing := New(nodes)
	hashRing = hashRing.AddWeightedNode("b", 0)
	hashRing = hashRing.AddWeightedNode("b", 2)
	hashRing = hashRing.AddWeightedNode("b", 2)

	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "b")
	expectNode(t, hashRing, "test5", "b")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "a")

	expectNodes(t, hashRing, "test", []string{"b", "a"})
}

func TestUpdateWeightedNode(t *testing.T) {
	nodes := []string{"a", "c"}
	hashRing := New(nodes)
	hashRing = hashRing.AddWeightedNode("b", 1)
	hashRing = hashRing.UpdateWeightedNode("b", 2)
	hashRing = hashRing.UpdateWeightedNode("b", 2)
	hashRing = hashRing.UpdateWeightedNode("b", 0)
	hashRing = hashRing.UpdateWeightedNode("d", 2)

	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "b")
	expectNode(t, hashRing, "test5", "b")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "a")

	expectNodes(t, hashRing, "test", []string{"b", "a"})
}

func TestRemoveAddNode(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	hashRing := New(nodes)

	expectNodesABC(t, hashRing)
	expectNodeRangesABC(t, hashRing)

	hashRing = hashRing.RemoveNode("b")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "c") // Migrated to c from b
	expectNode(t, hashRing, "test2", "a") // Migrated to a from b
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "a") // Migrated to a from b
	expectNode(t, hashRing, "bbbb", "a")

	expectNodes(t, hashRing, "test", []string{"a", "c"})
	expectNodes(t, hashRing, "test", []string{"a", "c"})
	expectNodes(t, hashRing, "test1", []string{"c", "a"})
	expectNodes(t, hashRing, "test2", []string{"a", "c"})
	expectNodes(t, hashRing, "test3", []string{"c", "a"})
	expectNodes(t, hashRing, "test4", []string{"c", "a"})
	expectNodes(t, hashRing, "test5", []string{"a", "c"})
	expectNodes(t, hashRing, "aaaa", []string{"a", "c"})
	expectNodes(t, hashRing, "bbbb", []string{"a", "c"})

	hashRing = hashRing.AddNode("b")

	expectNodesABC(t, hashRing)
	expectNodeRangesABC(t, hashRing)
}

func TestRemoveAddWeightedNode(t *testing.T) {
	weights := make(map[string]int)
	weights["a"] = 1
	weights["b"] = 2
	weights["c"] = 1
	hashRing := NewWithWeights(weights)

	expectWeights(t, hashRing, weights)

	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "b")
	expectNode(t, hashRing, "test5", "b")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "a")

	expectNodes(t, hashRing, "test", []string{"b", "a"})
	expectNodes(t, hashRing, "test", []string{"b", "a"})
	expectNodes(t, hashRing, "test1", []string{"b", "c"})
	expectNodes(t, hashRing, "test2", []string{"b", "a"})
	expectNodes(t, hashRing, "test3", []string{"c", "b"})
	expectNodes(t, hashRing, "test4", []string{"b", "a"})
	expectNodes(t, hashRing, "test5", []string{"b", "a"})
	expectNodes(t, hashRing, "aaaa", []string{"b", "a"})
	expectNodes(t, hashRing, "bbbb", []string{"a", "b"})

	hashRing = hashRing.RemoveNode("c")

	delete(weights, "c")
	expectWeights(t, hashRing, weights)

	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test", "b")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "b") // Migrated to b from c
	expectNode(t, hashRing, "test4", "b")
	expectNode(t, hashRing, "test5", "b")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "a")

	expectNodes(t, hashRing, "test", []string{"b", "a"})
	expectNodes(t, hashRing, "test", []string{"b", "a"})
	expectNodes(t, hashRing, "test1", []string{"b", "a"})
	expectNodes(t, hashRing, "test2", []string{"b", "a"})
	expectNodes(t, hashRing, "test3", []string{"b", "a"})
	expectNodes(t, hashRing, "test4", []string{"b", "a"})
	expectNodes(t, hashRing, "test5", []string{"b", "a"})
	expectNodes(t, hashRing, "aaaa", []string{"b", "a"})
	expectNodes(t, hashRing, "bbbb", []string{"a", "b"})
}

func TestAddRemoveNode(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	hashRing := New(nodes)
	hashRing = hashRing.AddNode("d")

	// Somehow adding d does not load balance these keys...
	expectNodesABCD(t, hashRing)

	expectNodes(t, hashRing, "test", []string{"a", "b"})
	expectNodes(t, hashRing, "test", []string{"a", "b"})
	expectNodes(t, hashRing, "test1", []string{"b", "d"})
	expectNodes(t, hashRing, "test2", []string{"b", "d"})
	expectNodes(t, hashRing, "test3", []string{"c", "d"})
	expectNodes(t, hashRing, "test4", []string{"c", "b"})
	expectNodes(t, hashRing, "test5", []string{"a", "d"})
	expectNodes(t, hashRing, "aaaa", []string{"b", "a"})
	expectNodes(t, hashRing, "bbbb", []string{"a", "b"})

	hashRing = hashRing.AddNode("e")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "e") // Migrated to e from a

	expectNodes(t, hashRing, "test", []string{"a", "b"})
	expectNodes(t, hashRing, "test", []string{"a", "b"})
	expectNodes(t, hashRing, "test1", []string{"b", "d"})
	expectNodes(t, hashRing, "test2", []string{"b", "d"})
	expectNodes(t, hashRing, "test3", []string{"c", "e"})
	expectNodes(t, hashRing, "test4", []string{"c", "b"})
	expectNodes(t, hashRing, "test5", []string{"a", "e"})
	expectNodes(t, hashRing, "aaaa", []string{"b", "e"})
	expectNodes(t, hashRing, "bbbb", []string{"e", "a"})

	hashRing = hashRing.AddNode("f")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "f") // Migrated to f from b
	expectNode(t, hashRing, "test3", "f") // Migrated to f from c
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "f") // Migrated to f from a
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "e")

	expectNodes(t, hashRing, "test", []string{"a", "b"})
	expectNodes(t, hashRing, "test", []string{"a", "b"})
	expectNodes(t, hashRing, "test1", []string{"b", "d"})
	expectNodes(t, hashRing, "test2", []string{"f", "b"})
	expectNodes(t, hashRing, "test3", []string{"f", "c"})
	expectNodes(t, hashRing, "test4", []string{"c", "b"})
	expectNodes(t, hashRing, "test5", []string{"f", "a"})
	expectNodes(t, hashRing, "aaaa", []string{"b", "e"})
	expectNodes(t, hashRing, "bbbb", []string{"e", "f"})

	hashRing = hashRing.RemoveNode("e")

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "f")
	expectNode(t, hashRing, "test3", "f")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "f")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "f") // Migrated to f from e

	expectNodes(t, hashRing, "test", []string{"a", "b"})
	expectNodes(t, hashRing, "test", []string{"a", "b"})
	expectNodes(t, hashRing, "test1", []string{"b", "d"})
	expectNodes(t, hashRing, "test2", []string{"f", "b"})
	expectNodes(t, hashRing, "test3", []string{"f", "c"})
	expectNodes(t, hashRing, "test4", []string{"c", "b"})
	expectNodes(t, hashRing, "test5", []string{"f", "a"})
	expectNodes(t, hashRing, "aaaa", []string{"b", "a"})
	expectNodes(t, hashRing, "bbbb", []string{"f", "a"})

	hashRing = hashRing.RemoveNode("f")

	expectNodesABCD(t, hashRing)

	expectNodes(t, hashRing, "test", []string{"a", "b"})
	expectNodes(t, hashRing, "test", []string{"a", "b"})
	expectNodes(t, hashRing, "test1", []string{"b", "d"})
	expectNodes(t, hashRing, "test2", []string{"b", "d"})
	expectNodes(t, hashRing, "test3", []string{"c", "d"})
	expectNodes(t, hashRing, "test4", []string{"c", "b"})
	expectNodes(t, hashRing, "test5", []string{"a", "d"})
	expectNodes(t, hashRing, "aaaa", []string{"b", "a"})
	expectNodes(t, hashRing, "bbbb", []string{"a", "b"})

	hashRing = hashRing.RemoveNode("d")

	expectNodesABC(t, hashRing)
	expectNodeRangesABC(t, hashRing)
}

func TestGetNodefrom(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	hashRing := New(nodes)

	n, ok := hashRing.GetNodeFrom("test", []string{"a"})
	assert.True(t, ok)
	assert.Equal(t, "a", n)
	n, ok = hashRing.GetNodeFrom("test", []string{"b"})
	assert.True(t, ok)
	assert.Equal(t, "b", n)
	n, ok = hashRing.GetNodeFrom("test", []string{"b"})
	assert.True(t, ok)
	assert.Equal(t, "b", n)

	n, ok = hashRing.GetNodeFrom("test", []string{"a", "b"})
	assert.True(t, ok)
	assert.Equal(t, "a", n)
	n, ok = hashRing.GetNodeFrom("test1", []string{"a", "b"})
	assert.True(t, ok)
	assert.Equal(t, "b", n)
	for i := 0; i < 99; i++ {
		n, ok = hashRing.GetNodeFrom(strconv.Itoa(i), []string{"a", "b"})
		assert.True(t, ok)
		assert.NotEqual(t, "c", n)
	}

	n, ok = hashRing.GetNodeFrom("test", []string{"a", "b", "c"})
	assert.True(t, ok)
	assert.Equal(t, "a", n)
	n, ok = hashRing.GetNodeFrom("test1", []string{"a", "b", "c"})
	assert.True(t, ok)
	assert.Equal(t, "b", n)
	n, ok = hashRing.GetNodeFrom("testtt", []string{"a", "b", "c"})
	assert.True(t, ok)
	assert.Equal(t, "c", n)
	n, ok = hashRing.GetNodeFrom("testtt", []string{"a"})
	assert.True(t, ok)
	assert.Equal(t, "a", n)
}

func TestHashDigest(t *testing.T) {
	key := "whatever"
	val := hashDigest(key)
	for i := 0; i < 10; i++ {
		newVal := hashDigest(key)
		assert.True(t, sameBytes(val[:], newVal[:]), "hashDigest value inconsistent %v:%v", val, newVal)
	}
}

func sameBytes(b1, b2 []byte) bool {
	if len(b1) != len(b2) {
		return false
	}
	for i := 0; i < len(b1); i++ {
		if b1[i] != b2[i] {
			return false
		}
	}
	return true
}

func BenchmarkHashes(b *testing.B) {
	nodes := []string{"a", "b", "c", "d", "e", "f", "g"}
	hashRing := New(nodes)
	tt := []struct {
		key   string
		nodes []string
	}{
		{"test", []string{"a", "b"}},
		{"test", []string{"a", "b"}},
		{"test1", []string{"b", "d"}},
		{"test2", []string{"f", "b"}},
		{"test3", []string{"f", "c"}},
		{"test4", []string{"c", "b"}},
		{"test5", []string{"f", "a"}},
		{"aaaa", []string{"b", "a"}},
		{"bbbb", []string{"f", "a"}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o := tt[i%len(tt)]
		hashRing.GetNodes(o.key, 2)
	}
}

func BenchmarkHashesSingle(b *testing.B) {
	nodes := []string{"a", "b", "c", "d", "e", "f", "g"}
	hashRing := New(nodes)
	tt := []struct {
		key   string
		nodes []string
	}{
		{"test", []string{"a", "b"}},
		{"test", []string{"a", "b"}},
		{"test1", []string{"b", "d"}},
		{"test2", []string{"f", "b"}},
		{"test3", []string{"f", "c"}},
		{"test4", []string{"c", "b"}},
		{"test5", []string{"f", "a"}},
		{"aaaa", []string{"b", "a"}},
		{"bbbb", []string{"f", "a"}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o := tt[i%len(tt)]
		hashRing.GetNode(o.key)
	}
}

func BenchmarkNew(b *testing.B) {
	nodes := []string{"a", "b", "c", "d", "e", "f", "g"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = New(nodes)
	}
}
