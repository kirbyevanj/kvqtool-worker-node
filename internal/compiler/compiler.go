package compiler

import (
	"encoding/json"
	"fmt"
)

// DrawflowExport represents the top-level Drawflow JSON export.
type DrawflowExport struct {
	Drawflow map[string]DrawflowModule `json:"drawflow"`
}

type DrawflowModule struct {
	Data map[string]DrawflowNode `json:"data"`
}

type DrawflowNode struct {
	ID      int                        `json:"id"`
	Name    string                     `json:"name"`
	Data    map[string]json.RawMessage `json:"data"`
	Class   string                     `json:"class"`
	Inputs  map[string]DrawflowPort    `json:"inputs"`
	Outputs map[string]DrawflowPort    `json:"outputs"`
}

type DrawflowPort struct {
	Connections []DrawflowConnection `json:"connections"`
}

type DrawflowConnection struct {
	Node   string `json:"node"`
	Output string `json:"output"`
	Input  string `json:"input"`
}

// PipelineNode represents a compiled node ready for GStreamer construction.
type PipelineNode struct {
	ID         int
	NodeType   string
	Properties map[string]string
	Inputs     []int
	Outputs    []int
}

// Compile parses a Drawflow DAG JSON and returns an ordered list of pipeline nodes.
func Compile(dagJSON json.RawMessage) ([]PipelineNode, error) {
	var export DrawflowExport
	if err := json.Unmarshal(dagJSON, &export); err != nil {
		return nil, fmt.Errorf("unmarshal drawflow: %w", err)
	}

	module, ok := export.Drawflow["Home"]
	if !ok {
		return nil, fmt.Errorf("no Home module in drawflow export")
	}

	nodes := make(map[int]*PipelineNode, len(module.Data))
	for _, n := range module.Data {
		pn := &PipelineNode{
			ID:         n.ID,
			NodeType:   n.Name,
			Properties: parseProperties(n.Data),
		}

		for _, port := range n.Outputs {
			for _, conn := range port.Connections {
				var targetID int
				if _, err := fmt.Sscanf(conn.Node, "%d", &targetID); err == nil {
					pn.Outputs = append(pn.Outputs, targetID)
				}
			}
		}

		for _, port := range n.Inputs {
			for _, conn := range port.Connections {
				var sourceID int
				if _, err := fmt.Sscanf(conn.Node, "%d", &sourceID); err == nil {
					pn.Inputs = append(pn.Inputs, sourceID)
				}
			}
		}

		nodes[n.ID] = pn
	}

	return topoSort(nodes)
}

func parseProperties(data map[string]json.RawMessage) map[string]string {
	props := make(map[string]string, len(data))
	for k, v := range data {
		var s string
		if err := json.Unmarshal(v, &s); err == nil {
			props[k] = s
			continue
		}
		props[k] = string(v)
	}
	return props
}

func topoSort(nodes map[int]*PipelineNode) ([]PipelineNode, error) {
	inDegree := make(map[int]int, len(nodes))
	for id := range nodes {
		inDegree[id] = 0
	}
	for _, n := range nodes {
		for _, out := range n.Outputs {
			inDegree[out]++
		}
	}

	var queue []int
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	var sorted []PipelineNode
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		n := nodes[id]
		sorted = append(sorted, *n)
		for _, out := range n.Outputs {
			inDegree[out]--
			if inDegree[out] == 0 {
				queue = append(queue, out)
			}
		}
	}

	if len(sorted) != len(nodes) {
		return nil, fmt.Errorf("cycle detected in workflow DAG")
	}
	return sorted, nil
}
