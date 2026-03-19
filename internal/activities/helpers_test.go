package activities

import (
	"encoding/json"
	"testing"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
)

// --- ok / fail / okJSON ---

func TestOk_SetsFieldsCorrectly(t *testing.T) {
	out := ok("n1", "bucket/key.mp4", map[string]string{"local_path": "/tmp/out.mp4"})
	if !out.Success {
		t.Error("expected Success=true")
	}
	if out.NodeID != "n1" {
		t.Errorf("NodeID=%q want n1", out.NodeID)
	}
	if out.S3Key != "bucket/key.mp4" {
		t.Errorf("S3Key=%q", out.S3Key)
	}
	var d map[string]string
	json.Unmarshal(out.Data, &d)
	if d["local_path"] != "/tmp/out.mp4" {
		t.Errorf("local_path=%q", d["local_path"])
	}
}

func TestOk_NilDataProducesNilRawMessage(t *testing.T) {
	out := ok("n1", "", nil)
	if out.Data != nil {
		t.Errorf("expected nil Data, got %s", out.Data)
	}
}

func TestOk_EmptyS3Key(t *testing.T) {
	out := ok("node", "", map[string]string{"x": "1"})
	if out.S3Key != "" {
		t.Errorf("S3Key should be empty, got %q", out.S3Key)
	}
}

func TestFail_SetsError(t *testing.T) {
	out := fail("n2", "something went wrong")
	if out.Success {
		t.Error("expected Success=false")
	}
	if out.Error != "something went wrong" {
		t.Errorf("Error=%q", out.Error)
	}
	if out.NodeID != "n2" {
		t.Errorf("NodeID=%q", out.NodeID)
	}
}

func TestFail_DataIsNil(t *testing.T) {
	out := fail("n", "err")
	if out.Data != nil {
		t.Error("fail should produce nil Data")
	}
}

func TestOkJSON_SerializesStruct(t *testing.T) {
	type myStruct struct {
		Value string `json:"value"`
	}
	out := okJSON("n3", "s3/key", myStruct{Value: "hello"})
	if !out.Success {
		t.Error("expected Success=true")
	}
	var s myStruct
	if err := json.Unmarshal(out.Data, &s); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if s.Value != "hello" {
		t.Errorf("Value=%q", s.Value)
	}
}

func TestOkJSON_EmptyS3KeyAllowed(t *testing.T) {
	out := okJSON("n", "", map[string]string{})
	if out.S3Key != "" {
		t.Error("S3Key should be empty")
	}
}

// --- resolveUpstreamS3Key ---

func upstreamResult(t *testing.T, out types.ActivityOutput) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestResolveUpstreamS3Key_ReturnsFirstMatch(t *testing.T) {
	input := types.ActivityInput{
		UpstreamResults: map[string]json.RawMessage{
			"n1": upstreamResult(t, types.ActivityOutput{NodeID: "n1", Success: true, S3Key: "projects/p/media/a.mp4"}),
		},
	}
	key := resolveUpstreamS3Key(input)
	if key != "projects/p/media/a.mp4" {
		t.Errorf("got %q", key)
	}
}

func TestResolveUpstreamS3Key_EmptyWhenNoS3Key(t *testing.T) {
	input := types.ActivityInput{
		UpstreamResults: map[string]json.RawMessage{
			"n1": upstreamResult(t, types.ActivityOutput{NodeID: "n1", Success: true, S3Key: ""}),
		},
	}
	if resolveUpstreamS3Key(input) != "" {
		t.Error("expected empty string")
	}
}

func TestResolveUpstreamS3Key_EmptyWhenNoUpstream(t *testing.T) {
	input := types.ActivityInput{}
	if resolveUpstreamS3Key(input) != "" {
		t.Error("expected empty string")
	}
}

func TestResolveUpstreamS3Key_SkipsMalformedJSON(t *testing.T) {
	input := types.ActivityInput{
		UpstreamResults: map[string]json.RawMessage{
			"bad":  json.RawMessage(`{not valid json`),
			"good": upstreamResult(t, types.ActivityOutput{S3Key: "k"}),
		},
	}
	// Either returns "k" or "" — must not panic, and if it finds "k" that's fine
	result := resolveUpstreamS3Key(input)
	if result != "k" && result != "" {
		t.Errorf("unexpected result %q", result)
	}
}

// --- resolveUpstreamLocalPath ---

func upstreamWithLocalPath(t *testing.T, path string) json.RawMessage {
	t.Helper()
	data, _ := json.Marshal(map[string]string{"local_path": path})
	out := types.ActivityOutput{Success: true, Data: data}
	b, _ := json.Marshal(out)
	return b
}

func TestResolveUpstreamLocalPath_ReturnsPath(t *testing.T) {
	input := types.ActivityInput{
		UpstreamResults: map[string]json.RawMessage{
			"n1": upstreamWithLocalPath(t, "/tmp/foo.mp4"),
		},
	}
	if got := resolveUpstreamLocalPath(input); got != "/tmp/foo.mp4" {
		t.Errorf("got %q", got)
	}
}

func TestResolveUpstreamLocalPath_EmptyWhenMissing(t *testing.T) {
	input := types.ActivityInput{
		UpstreamResults: map[string]json.RawMessage{
			"n1": upstreamResult(t, types.ActivityOutput{Success: true, S3Key: "bucket/key"}),
		},
	}
	if got := resolveUpstreamLocalPath(input); got != "" {
		t.Errorf("got %q", got)
	}
}

func TestResolveUpstreamLocalPath_EmptyWhenNoUpstream(t *testing.T) {
	if got := resolveUpstreamLocalPath(types.ActivityInput{}); got != "" {
		t.Errorf("got %q", got)
	}
}

// --- resolveUpstreamParam ---

func TestResolveUpstreamParam_ReturnsParamValue(t *testing.T) {
	data, _ := json.Marshal(map[string]string{"output_name": "metrics.json"})
	raw, _ := json.Marshal(types.ActivityOutput{Success: true, Data: data})
	input := types.ActivityInput{
		UpstreamResults: map[string]json.RawMessage{"n1": raw},
	}
	if got := resolveUpstreamParam(input, "output_name"); got != "metrics.json" {
		t.Errorf("got %q", got)
	}
}

func TestResolveUpstreamParam_EmptyWhenKeyAbsent(t *testing.T) {
	data, _ := json.Marshal(map[string]string{"other": "val"})
	raw, _ := json.Marshal(types.ActivityOutput{Success: true, Data: data})
	input := types.ActivityInput{
		UpstreamResults: map[string]json.RawMessage{"n1": raw},
	}
	if got := resolveUpstreamParam(input, "missing_key"); got != "" {
		t.Errorf("got %q", got)
	}
}

func TestResolveUpstreamParam_EmptyWhenNoUpstream(t *testing.T) {
	if got := resolveUpstreamParam(types.ActivityInput{}, "any"); got != "" {
		t.Errorf("got %q", got)
	}
}

// --- resolveUpstreamSceneCutFile ---

func TestResolveUpstreamSceneCutFile_ReturnsFile(t *testing.T) {
	scf := types.SceneCutFile{
		Version: "0.1",
		Source:  "video.mp4",
		Segments: []types.Segment{
			{Index: 0, StartTime: "00:00:00.000", EndTime: "00:00:10.000"},
		},
	}
	data, _ := json.Marshal(scf)
	raw, _ := json.Marshal(types.ActivityOutput{Success: true, Data: data})
	input := types.ActivityInput{
		UpstreamResults: map[string]json.RawMessage{"n1": raw},
	}
	got, ok := resolveUpstreamSceneCutFile(input)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.Source != "video.mp4" || len(got.Segments) != 1 {
		t.Errorf("unexpected result: %+v", got)
	}
}

func TestResolveUpstreamSceneCutFile_FalseWhenNoSegments(t *testing.T) {
	scf := types.SceneCutFile{Version: "0.1"}
	data, _ := json.Marshal(scf)
	raw, _ := json.Marshal(types.ActivityOutput{Success: true, Data: data})
	input := types.ActivityInput{
		UpstreamResults: map[string]json.RawMessage{"n1": raw},
	}
	_, ok := resolveUpstreamSceneCutFile(input)
	if ok {
		t.Error("expected ok=false for empty segments")
	}
}

func TestResolveUpstreamSceneCutFile_FalseWhenNoUpstream(t *testing.T) {
	_, ok := resolveUpstreamSceneCutFile(types.ActivityInput{})
	if ok {
		t.Error("expected ok=false")
	}
}
