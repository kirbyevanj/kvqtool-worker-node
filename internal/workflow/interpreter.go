package workflow

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kirbyevanj/kvqtool-kvq-models/types"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const maxFanOutConcurrency = 16

func InterpreterWorkflow(ctx workflow.Context, dag types.WorkflowDAG) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting workflow", "name", dag.Name, "version", dag.Version, "nodes", len(dag.Nodes))

	// Resolve global inputs into node params via InputMap.
	if len(dag.GlobalInputs) > 0 {
		globalValues := make(map[string]string, len(dag.GlobalInputs))
		for key, gi := range dag.GlobalInputs {
			globalValues[key] = gi.Default
		}
		applyGlobalInputs(&dag, globalValues)
	}

	sorted, err := topoSort(dag.Nodes)
	if err != nil {
		return fmt.Errorf("topo sort: %w", err)
	}

	// Build node-to-session-group lookup from SessionGroups.
	nodeSessionGroup := make(map[string]string)
	for _, sg := range dag.SessionGroups {
		for _, nid := range sg.Nodes {
			nodeSessionGroup[nid] = sg.ID
		}
	}
	// Also honor per-node SessionGroup field as override.
	for _, node := range dag.Nodes {
		if node.SessionGroup != "" {
			nodeSessionGroup[node.ID] = node.SessionGroup
		}
	}

	sm := newSessionManager(ctx)
	defer sm.completeAll()

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumAttempts:    3,
		},
	}

	results := make(map[string]json.RawMessage)
	totalNodes := len(sorted)

	for i, nodeID := range sorted {
		node := dag.Nodes[nodeID]

		upstream := gatherUpstream(node, results)

		switch node.Type {
		case types.MetaCompositeWorkflow:
			if err := executeCompositeWorkflow(ctx, ao, node, upstream, results); err != nil {
				return fmt.Errorf("composite %s: %w", nodeID, err)
			}
		case types.MetaSceneCutDispatch:
			if err := executeSceneCutDispatch(ctx, node, upstream, results); err != nil {
				return fmt.Errorf("dispatch %s: %w", nodeID, err)
			}
		default:
			groupID := nodeSessionGroup[nodeID]
			actCtx, err := sm.getContext(groupID)
			if err != nil {
				return err
			}
			actCtx = workflow.WithActivityOptions(actCtx, ao)

			params := interpolateParams(node.Params, results)

			input := types.ActivityInput{
				NodeID:          node.ID,
				NodeType:        node.Type,
				Params:          params,
				ProjectID:       params["project_id"],
				SessionGroup:    groupID,
				UpstreamResults: upstream,
			}

			var output types.ActivityOutput
			if err := workflow.ExecuteActivity(actCtx, node.Type, input).Get(ctx, &output); err != nil {
				return fmt.Errorf("node %s (%s) failed: %w", node.ID, node.Type, err)
			}
			if !output.Success {
				return fmt.Errorf("node %s (%s): %s", node.ID, node.Type, output.Error)
			}

			resultBytes, _ := json.Marshal(output)
			results[node.ID] = resultBytes
		}

		logger.Info("Node complete", "node", node.ID, "type", node.Type, "progress", fmt.Sprintf("%d/%d", i+1, totalNodes))
	}

	logger.Info("Workflow complete", "name", dag.Name)
	return nil
}

func gatherUpstream(node *types.DAGNode, results map[string]json.RawMessage) map[string]json.RawMessage {
	upstream := make(map[string]json.RawMessage)
	for _, inputID := range node.Inputs {
		if r, ok := results[inputID]; ok {
			upstream[inputID] = r
		}
	}
	return upstream
}

// executeCompositeWorkflow fetches a referenced workflow DAG and runs it as a child workflow.
func executeCompositeWorkflow(
	ctx workflow.Context,
	ao workflow.ActivityOptions,
	node *types.DAGNode,
	upstream map[string]json.RawMessage,
	results map[string]json.RawMessage,
) error {
	var childDAG types.WorkflowDAG

	// If the frontend inlined the DAG, use it directly.
	if dagJSON, ok := node.Params["dag_json"]; ok && dagJSON != "" {
		if err := json.Unmarshal([]byte(dagJSON), &childDAG); err != nil {
			return fmt.Errorf("parse inline dag_json: %w", err)
		}
	} else if node.WorkflowRef != "" {
		// Fetch from API via activity.
		actCtx := workflow.WithActivityOptions(ctx, ao)
		fetchInput := types.ActivityInput{
			NodeID:   node.ID,
			NodeType: types.ActivityFetchWorkflowDAG,
			Params:   map[string]string{"workflow_id": node.WorkflowRef, "project_id": node.Params["project_id"]},
		}
		var fetchOutput types.ActivityOutput
		if err := workflow.ExecuteActivity(actCtx, types.ActivityFetchWorkflowDAG, fetchInput).Get(ctx, &fetchOutput); err != nil {
			return fmt.Errorf("fetch workflow %s: %w", node.WorkflowRef, err)
		}
		if !fetchOutput.Success {
			return fmt.Errorf("fetch workflow: %s", fetchOutput.Error)
		}
		if err := json.Unmarshal(fetchOutput.Data, &childDAG); err != nil {
			return fmt.Errorf("parse fetched dag: %w", err)
		}
	} else {
		return fmt.Errorf("CompositeWorkflow node %s has no dag_json or workflow_ref", node.ID)
	}

	// Map parent params into child global inputs.
	if childDAG.GlobalInputs != nil {
		for key, gi := range childDAG.GlobalInputs {
			if val, ok := node.Params[key]; ok {
				gi.Default = val
			}
		}
	}

	childOpts := workflow.ChildWorkflowOptions{
		WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID + "/composite/" + node.ID,
	}
	childCtx := workflow.WithChildOptions(ctx, childOpts)

	if err := workflow.ExecuteChildWorkflow(childCtx, InterpreterWorkflow, childDAG).Get(ctx, nil); err != nil {
		return err
	}

	resultBytes, _ := json.Marshal(types.ActivityOutput{
		NodeID:  node.ID,
		Success: true,
	})
	results[node.ID] = resultBytes
	return nil
}

// executeSceneCutDispatch fans out child workflows for each segment in a SceneCutFile.
func executeSceneCutDispatch(
	ctx workflow.Context,
	node *types.DAGNode,
	upstream map[string]json.RawMessage,
	results map[string]json.RawMessage,
) error {
	// Extract SceneCutFile from upstream.
	var scf types.SceneCutFile
	found := false
	for _, raw := range upstream {
		var out types.ActivityOutput
		if json.Unmarshal(raw, &out) != nil || out.Data == nil {
			continue
		}
		if json.Unmarshal(out.Data, &scf) == nil && len(scf.Segments) > 0 {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("SceneCutDispatch node %s: no SceneCutFile in upstream", node.ID)
	}

	workflowRef := node.WorkflowRef
	if workflowRef == "" {
		workflowRef = node.Params["workflow_ref"]
	}
	if workflowRef == "" {
		return fmt.Errorf("SceneCutDispatch node %s: no workflow_ref", node.ID)
	}

	sourceURI := node.Params["source_uri"]
	if sourceURI == "" {
		sourceURI = scf.Source
	}

	// Fan out with bounded concurrency using workflow.Go and a buffered channel.
	sem := workflow.NewBufferedChannel(ctx, maxFanOutConcurrency)
	errCh := workflow.NewBufferedChannel(ctx, len(scf.Segments))

	for idx, seg := range scf.Segments {
		seg := seg
		idx := idx

		sem.Send(ctx, true)

		workflow.Go(ctx, func(gCtx workflow.Context) {
			defer func() {
				var v bool
				sem.Receive(gCtx, &v)
			}()

			childDAG := types.WorkflowDAG{
				Version: "2.0",
				Name:    fmt.Sprintf("%s-segment-%d", node.ID, idx),
				GlobalInputs: map[string]*types.GlobalInput{
					"source_uri": {Name: "source_uri", Type: "string", Default: sourceURI},
					"start_time": {Name: "start_time", Type: "string", Default: seg.StartTime},
					"end_time":   {Name: "end_time", Type: "string", Default: seg.EndTime},
				},
			}

			// Copy parent params to child for the fetch activity.
			childDAG.Nodes = map[string]*types.DAGNode{
				"fetch": {
					ID:          "fetch",
					Type:        types.ActivityFetchWorkflowDAG,
					Params:      map[string]string{"workflow_id": workflowRef, "project_id": node.Params["project_id"]},
					Inputs:      nil,
					Outputs:     []string{"run"},
					WorkflowRef: workflowRef,
				},
			}

			// Actually, we need to run a child workflow directly.
			// The child workflow will be the referenced workflow with segment params.
			childOpts := workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("%s/dispatch/%s/seg-%d", workflow.GetInfo(gCtx).WorkflowExecution.ID, node.ID, idx),
			}
			childCtx := workflow.WithChildOptions(gCtx, childOpts)

			// We pass a minimal DAG that the child interpreter will expand by fetching the ref.
			// Instead, build a CompositeWorkflow DAG wrapper.
			wrapperDAG := types.WorkflowDAG{
				Version: "2.0",
				Name:    fmt.Sprintf("dispatch-seg-%d", idx),
				GlobalInputs: map[string]*types.GlobalInput{
					"source_uri": {Name: "source_uri", Type: "string", Default: sourceURI},
					"start_time": {Name: "start_time", Type: "string", Default: seg.StartTime},
					"end_time":   {Name: "end_time", Type: "string", Default: seg.EndTime},
				},
				Nodes: map[string]*types.DAGNode{
					"composite": {
						ID:          "composite",
						Type:        types.MetaCompositeWorkflow,
						WorkflowRef: workflowRef,
						Params:      map[string]string{"project_id": node.Params["project_id"]},
						InputMap: map[string]string{
							"source_uri": "source_uri",
							"start_time": "start_time",
							"end_time":   "end_time",
						},
					},
				},
			}

			if err := workflow.ExecuteChildWorkflow(childCtx, InterpreterWorkflow, wrapperDAG).Get(gCtx, nil); err != nil {
				errCh.Send(gCtx, fmt.Sprintf("segment %d: %v", idx, err))
			}
		})
	}

	// Wait for all goroutines to complete by draining the semaphore.
	for i := 0; i < maxFanOutConcurrency; i++ {
		sem.Send(ctx, true)
	}

	// Check for errors.
	var firstErr string
	for {
		var errMsg string
		ok := errCh.ReceiveAsync(&errMsg)
		if !ok {
			break
		}
		if firstErr == "" {
			firstErr = errMsg
		}
	}
	if firstErr != "" {
		return fmt.Errorf("SceneCutDispatch errors: %s", firstErr)
	}

	resultBytes, _ := json.Marshal(types.ActivityOutput{
		NodeID:  node.ID,
		Success: true,
		Data:    mustMarshal(map[string]string{"segments_dispatched": fmt.Sprintf("%d", len(scf.Segments))}),
	})
	results[node.ID] = resultBytes
	return nil
}

func mustMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
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
