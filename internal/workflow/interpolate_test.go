package workflow

import (
	"encoding/json"
	"testing"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
)

// helper: build a RawMessage from an ActivityOutput
func rawOutput(nodeID, s3Key string, data map[string]string) json.RawMessage {
	d, _ := json.Marshal(data)
	out := types.ActivityOutput{NodeID: nodeID, Success: true, S3Key: s3Key, Data: d}
	b, _ := json.Marshal(out)
	return b
}

// --- extractField ---

func TestExtractField_S3Key(t *testing.T) {
	raw := rawOutput("n1", "projects/p/file.mp4", nil)
	got := extractField(raw, "s3_key", "fallback")
	if got != "projects/p/file.mp4" {
		t.Errorf("got %q", got)
	}
}

func TestExtractField_NodeID(t *testing.T) {
	raw := rawOutput("mynode", "", nil)
	got := extractField(raw, "node_id", "fallback")
	if got != "mynode" {
		t.Errorf("got %q", got)
	}
}

func TestExtractField_DataMapKey(t *testing.T) {
	raw := rawOutput("n1", "", map[string]string{"local_path": "/tmp/out.mp4"})
	got := extractField(raw, "local_path", "fallback")
	if got != "/tmp/out.mp4" {
		t.Errorf("got %q", got)
	}
}

func TestExtractField_FallbackWhenMissing(t *testing.T) {
	raw := rawOutput("n1", "", map[string]string{})
	got := extractField(raw, "nonexistent", "fallback")
	if got != "fallback" {
		t.Errorf("got %q", got)
	}
}

func TestExtractField_FallbackOnBadJSON(t *testing.T) {
	got := extractField(json.RawMessage(`{invalid`), "s3_key", "fb")
	if got != "fb" {
		t.Errorf("got %q", got)
	}
}

func TestExtractField_EmptyS3KeyFallsThrough(t *testing.T) {
	// S3Key is empty → should fall through to data map
	raw := rawOutput("n1", "", map[string]string{"s3_key": "nested-key"})
	got := extractField(raw, "s3_key", "fb")
	// Top-level s3_key check fails (empty), data map has it
	if got != "nested-key" {
		t.Errorf("got %q want nested-key", got)
	}
}

// --- interpolateValue ---

func TestInterpolateValue_NoExpression(t *testing.T) {
	results := map[string]json.RawMessage{}
	got := interpolateValue("plain string", results)
	if got != "plain string" {
		t.Errorf("got %q", got)
	}
}

func TestInterpolateValue_ResolvesS3Key(t *testing.T) {
	results := map[string]json.RawMessage{
		"dl": rawOutput("dl", "projects/p/media/file.mp4", nil),
	}
	got := interpolateValue("${dl.s3_key}", results)
	if got != "projects/p/media/file.mp4" {
		t.Errorf("got %q", got)
	}
}

func TestInterpolateValue_ResolvesDataMapKey(t *testing.T) {
	results := map[string]json.RawMessage{
		"enc": rawOutput("enc", "", map[string]string{"local_path": "/tmp/enc.mp4"}),
	}
	got := interpolateValue("${enc.local_path}", results)
	if got != "/tmp/enc.mp4" {
		t.Errorf("got %q", got)
	}
}

func TestInterpolateValue_PreservesExpressionWhenNodeMissing(t *testing.T) {
	results := map[string]json.RawMessage{}
	expr := "${missing.s3_key}"
	got := interpolateValue(expr, results)
	if got != expr {
		t.Errorf("got %q, want original expression", got)
	}
}

func TestInterpolateValue_MultipleExpressions(t *testing.T) {
	results := map[string]json.RawMessage{
		"a": rawOutput("a", "k1", nil),
		"b": rawOutput("b", "k2", nil),
	}
	got := interpolateValue("${a.s3_key}+${b.s3_key}", results)
	if got != "k1+k2" {
		t.Errorf("got %q", got)
	}
}

func TestInterpolateValue_MalformedExpressionPreserved(t *testing.T) {
	// No dot → not a valid node.field expression
	results := map[string]json.RawMessage{}
	expr := "${nodefield}"
	got := interpolateValue(expr, results)
	if got != expr {
		t.Errorf("got %q", got)
	}
}

// --- interpolateParams ---

func TestInterpolateParams_ResolvesAllValues(t *testing.T) {
	results := map[string]json.RawMessage{
		"n1": rawOutput("n1", "my/key.mp4", nil),
	}
	params := map[string]string{
		"src":   "${n1.s3_key}",
		"plain": "no-interpolation",
	}
	got := interpolateParams(params, results)
	if got["src"] != "my/key.mp4" {
		t.Errorf("src=%q", got["src"])
	}
	if got["plain"] != "no-interpolation" {
		t.Errorf("plain=%q", got["plain"])
	}
}

func TestInterpolateParams_EmptyParamsReturnEmpty(t *testing.T) {
	got := interpolateParams(map[string]string{}, map[string]json.RawMessage{})
	if len(got) != 0 {
		t.Errorf("expected empty map, got %v", got)
	}
}

func TestInterpolateParams_DoesNotMutateOriginal(t *testing.T) {
	results := map[string]json.RawMessage{
		"n1": rawOutput("n1", "key", nil),
	}
	orig := map[string]string{"k": "${n1.s3_key}"}
	interpolateParams(orig, results)
	if orig["k"] != "${n1.s3_key}" {
		t.Error("original params were mutated")
	}
}

// --- applyGlobalInputs ---

func TestApplyGlobalInputs_MapsGlobalToLocal(t *testing.T) {
	dag := &types.WorkflowDAG{
		GlobalInputs: map[string]*types.GlobalInput{
			"src_resource": {Name: "src_resource", Type: "string", Default: ""},
		},
		Nodes: map[string]*types.DAGNode{
			"dl": {
				ID:   "dl",
				Type: "ResourceDownload",
				InputMap: map[string]string{
					"src_resource": "s3_key",
				},
				Params: map[string]string{},
			},
		},
	}
	applyGlobalInputs(dag, map[string]string{"src_resource": "projects/p/media/v.mp4"})
	if dag.Nodes["dl"].Params["s3_key"] != "projects/p/media/v.mp4" {
		t.Errorf("param not set: %v", dag.Nodes["dl"].Params)
	}
}

func TestApplyGlobalInputs_SkipsNodesWithoutInputMap(t *testing.T) {
	dag := &types.WorkflowDAG{
		GlobalInputs: map[string]*types.GlobalInput{
			"k": {Default: "val"},
		},
		Nodes: map[string]*types.DAGNode{
			"n": {ID: "n", Params: map[string]string{"existing": "yes"}},
		},
	}
	applyGlobalInputs(dag, map[string]string{"k": "v"})
	if _, ok := dag.Nodes["n"].Params["k"]; ok {
		t.Error("global input should not be applied when no InputMap")
	}
}

func TestApplyGlobalInputs_InitializesNilParams(t *testing.T) {
	dag := &types.WorkflowDAG{
		GlobalInputs: map[string]*types.GlobalInput{"g": {Default: ""}},
		Nodes: map[string]*types.DAGNode{
			"n": {
				ID:       "n",
				InputMap: map[string]string{"g": "local_g"},
				Params:   nil,
			},
		},
	}
	applyGlobalInputs(dag, map[string]string{"g": "value"})
	if dag.Nodes["n"].Params["local_g"] != "value" {
		t.Error("nil Params not initialized")
	}
}

func TestApplyGlobalInputs_IgnoresMissingGlobalValues(t *testing.T) {
	dag := &types.WorkflowDAG{
		GlobalInputs: map[string]*types.GlobalInput{"g": {Default: ""}},
		Nodes: map[string]*types.DAGNode{
			"n": {
				ID:       "n",
				InputMap: map[string]string{"g": "local_g"},
				Params:   map[string]string{},
			},
		},
	}
	// globalValues doesn't contain "g"
	applyGlobalInputs(dag, map[string]string{})
	if v, ok := dag.Nodes["n"].Params["local_g"]; ok {
		t.Errorf("should not set param: %q", v)
	}
}
