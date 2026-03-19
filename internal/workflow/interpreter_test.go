package workflow

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
)

// --- topoSort ---

func TestTopoSort_LinearChain(t *testing.T) {
	nodes := map[string]*types.DAGNode{
		"a": {ID: "a", Outputs: []string{"b"}},
		"b": {ID: "b", Outputs: []string{"c"}},
		"c": {ID: "c", Outputs: nil},
	}
	sorted, err := topoSort(nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sorted) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(sorted))
	}
	pos := func(id string) int {
		for i, v := range sorted {
			if v == id {
				return i
			}
		}
		return -1
	}
	if pos("a") >= pos("b") || pos("b") >= pos("c") {
		t.Errorf("wrong order: %v", sorted)
	}
}

func TestTopoSort_SingleNode(t *testing.T) {
	nodes := map[string]*types.DAGNode{
		"only": {ID: "only"},
	}
	sorted, err := topoSort(nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sorted) != 1 || sorted[0] != "only" {
		t.Errorf("got %v", sorted)
	}
}

func TestTopoSort_FanOut(t *testing.T) {
	nodes := map[string]*types.DAGNode{
		"src": {ID: "src", Outputs: []string{"enc1", "enc2"}},
		"enc1": {ID: "enc1"},
		"enc2": {ID: "enc2"},
	}
	sorted, err := topoSort(nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sorted) != 3 {
		t.Fatalf("expected 3, got %d: %v", len(sorted), sorted)
	}
	// src must come first
	if sorted[0] != "src" {
		t.Errorf("src should be first: %v", sorted)
	}
}

func TestTopoSort_Diamond(t *testing.T) {
	// a → b, a → c, b → d, c → d
	nodes := map[string]*types.DAGNode{
		"a": {ID: "a", Outputs: []string{"b", "c"}},
		"b": {ID: "b", Outputs: []string{"d"}},
		"c": {ID: "c", Outputs: []string{"d"}},
		"d": {ID: "d"},
	}
	sorted, err := topoSort(nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pos := func(id string) int {
		for i, v := range sorted {
			if v == id {
				return i
			}
		}
		return -1
	}
	if pos("a") >= pos("b") || pos("a") >= pos("c") {
		t.Errorf("a must come before b and c: %v", sorted)
	}
	if pos("b") >= pos("d") || pos("c") >= pos("d") {
		t.Errorf("d must come last: %v", sorted)
	}
}

func TestTopoSort_CycleReturnsError(t *testing.T) {
	nodes := map[string]*types.DAGNode{
		"a": {ID: "a", Outputs: []string{"b"}},
		"b": {ID: "b", Outputs: []string{"a"}},
	}
	_, err := topoSort(nodes)
	if err == nil {
		t.Error("expected cycle error")
	}
}

func TestTopoSort_EmptyGraph(t *testing.T) {
	sorted, err := topoSort(map[string]*types.DAGNode{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sorted) != 0 {
		t.Errorf("expected empty, got %v", sorted)
	}
}

func TestTopoSort_ParallelNodes(t *testing.T) {
	// No edges — order is arbitrary but all nodes present
	nodes := map[string]*types.DAGNode{
		"x": {ID: "x"},
		"y": {ID: "y"},
		"z": {ID: "z"},
	}
	sorted, err := topoSort(nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sorted) != 3 {
		t.Fatalf("expected 3, got %v", sorted)
	}
	got := make([]string, len(sorted))
	copy(got, sorted)
	sort.Strings(got)
	if got[0] != "x" || got[1] != "y" || got[2] != "z" {
		t.Errorf("missing nodes: %v", sorted)
	}
}

// --- gatherUpstream ---

func TestGatherUpstream_ReturnsOnlyNodeInputs(t *testing.T) {
	results := map[string]json.RawMessage{
		"a": json.RawMessage(`"resultA"`),
		"b": json.RawMessage(`"resultB"`),
		"c": json.RawMessage(`"resultC"`),
	}
	node := &types.DAGNode{Inputs: []string{"a", "c"}}
	upstream := gatherUpstream(node, results)
	if len(upstream) != 2 {
		t.Fatalf("expected 2, got %d", len(upstream))
	}
	if _, ok := upstream["b"]; ok {
		t.Error("b should not be in upstream")
	}
}

func TestGatherUpstream_EmptyWhenNoInputs(t *testing.T) {
	results := map[string]json.RawMessage{"a": json.RawMessage(`1`)}
	node := &types.DAGNode{}
	upstream := gatherUpstream(node, results)
	if len(upstream) != 0 {
		t.Errorf("expected empty, got %v", upstream)
	}
}

func TestGatherUpstream_SkipsMissingResults(t *testing.T) {
	results := map[string]json.RawMessage{
		"exists": json.RawMessage(`1`),
	}
	node := &types.DAGNode{Inputs: []string{"exists", "notexists"}}
	upstream := gatherUpstream(node, results)
	if len(upstream) != 1 {
		t.Fatalf("expected 1, got %d", len(upstream))
	}
}

// --- mustMarshal ---

func TestMustMarshal_Struct(t *testing.T) {
	b := mustMarshal(map[string]string{"k": "v"})
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["k"] != "v" {
		t.Errorf("got %q", m["k"])
	}
}

func TestMustMarshal_Nil(t *testing.T) {
	b := mustMarshal(nil)
	if string(b) != "null" {
		t.Errorf("got %s", b)
	}
}

func TestMustMarshal_Int(t *testing.T) {
	b := mustMarshal(42)
	if string(b) != "42" {
		t.Errorf("got %s", b)
	}
}
