package workflow

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

func InterpreterWorkflow(ctx workflow.Context, dag types.WorkflowDAG) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting workflow", "name", dag.Name, "nodes", len(dag.Nodes))

	sorted, err := topoSort(dag.Nodes)
	if err != nil {
		return fmt.Errorf("topo sort: %w", err)
	}

	// Create a session to pin sequential activities to the same worker node.
	// This allows local file I/O between stages without S3 round-trips.
	sessCtx, err := workflow.CreateSession(ctx, &workflow.SessionOptions{
		CreationTimeout:  5 * time.Minute,
		ExecutionTimeout: 2 * time.Hour,
	})
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer workflow.CompleteSession(sessCtx)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumAttempts:    3,
		},
	}
	actCtx := workflow.WithActivityOptions(sessCtx, ao)

	results := make(map[string]json.RawMessage)
	totalNodes := len(sorted)

	for i, nodeID := range sorted {
		node := dag.Nodes[nodeID]

		upstream := make(map[string]json.RawMessage)
		for _, inputID := range node.Inputs {
			if r, ok := results[inputID]; ok {
				upstream[inputID] = r
			}
		}

		input := types.ActivityInput{
			NodeID:          node.ID,
			NodeType:        node.Type,
			Params:          node.Params,
			ProjectID:       node.Params["project_id"],
			UpstreamResults: upstream,
		}

		var output types.ActivityOutput
		err := workflow.ExecuteActivity(actCtx, node.Type, input).Get(ctx, &output)
		if err != nil {
			return fmt.Errorf("node %s (%s) failed: %w", node.ID, node.Type, err)
		}

		if !output.Success {
			return fmt.Errorf("node %s (%s): %s", node.ID, node.Type, output.Error)
		}

		resultBytes, _ := json.Marshal(output)
		results[node.ID] = resultBytes

		logger.Info("Node complete", "node", node.ID, "type", node.Type, "progress", fmt.Sprintf("%d/%d", i+1, totalNodes))
	}

	logger.Info("Workflow complete", "name", dag.Name)
	return nil
}

func topoSort(nodes map[string]*types.DAGNode) ([]string, error) {
	inDegree := make(map[string]int, len(nodes))
	for id := range nodes {
		inDegree[id] = 0
	}
	for _, n := range nodes {
		for _, out := range n.Outputs {
			inDegree[out]++
		}
	}

	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	var sorted []string
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		sorted = append(sorted, id)
		node := nodes[id]
		for _, out := range node.Outputs {
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
