package workflow

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
)

var exprRe = regexp.MustCompile(`\$\{([^}]+)\}`)

// interpolateParams resolves ${node_id.field} expressions in all param values
// using the accumulated results map from prior node executions.
func interpolateParams(params map[string]string, results map[string]json.RawMessage) map[string]string {
	out := make(map[string]string, len(params))
	for k, v := range params {
		out[k] = interpolateValue(v, results)
	}
	return out
}

func interpolateValue(val string, results map[string]json.RawMessage) string {
	if !strings.Contains(val, "${") {
		return val
	}
	return exprRe.ReplaceAllStringFunc(val, func(match string) string {
		inner := match[2 : len(match)-1] // strip ${ and }
		parts := strings.SplitN(inner, ".", 2)
		if len(parts) != 2 {
			return match
		}
		nodeID, field := parts[0], parts[1]
		raw, ok := results[nodeID]
		if !ok {
			return match
		}
		return extractField(raw, field, match)
	})
}

// extractField tries ActivityOutput.Data first (most common), then top-level ActivityOutput fields.
func extractField(raw json.RawMessage, field, fallback string) string {
	var out types.ActivityOutput
	if json.Unmarshal(raw, &out) != nil {
		return fallback
	}

	// Check top-level ActivityOutput fields
	switch field {
	case "s3_key":
		if out.S3Key != "" {
			return out.S3Key
		}
	case "node_id":
		return out.NodeID
	}

	// Check nested Data map
	if out.Data != nil {
		var data map[string]string
		if json.Unmarshal(out.Data, &data) == nil {
			if v, ok := data[field]; ok {
				return v
			}
		}
	}
	return fallback
}

// applyGlobalInputs substitutes global input values into node params via each node's InputMap.
func applyGlobalInputs(dag *types.WorkflowDAG, globalValues map[string]string) {
	for _, node := range dag.Nodes {
		if len(node.InputMap) == 0 {
			continue
		}
		for globalKey, localKey := range node.InputMap {
			if val, ok := globalValues[globalKey]; ok {
				if node.Params == nil {
					node.Params = make(map[string]string)
				}
				node.Params[localKey] = val
			}
		}
	}
}
