package scheduling

import (
	"fmt"
	"time"

	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/container"
	"github.com/grussorusso/serverledge/internal/executor"
	"github.com/grussorusso/serverledge/internal/node"
)

const HANDLER_DIR = "/app"

// Execute serves a request on the specified container.
func Execute(contID container.ContainerID, r *scheduledRequest) error {
	// log.Printf("[%s] Executing on container: %v", r, contID)

	var req executor.InvocationRequest
	if r.Fun.Runtime == container.CUSTOM_RUNTIME {
		req = executor.InvocationRequest{
			Params: r.Params,
		}
	} else {
		cmd := container.RuntimeToInfo[r.Fun.Runtime].InvocationCmd
		req = executor.InvocationRequest{
			cmd,
			r.Params,
			r.Fun.Handler,
			HANDLER_DIR,
		}
	}

	t0 := time.Now()

	response, invocationWait, err := container.Execute(contID, &req)
	if err != nil {
		// notify scheduler
		completions <- &completion{scheduledRequest: r, contID: contID}
		// log.Printf("[%s] Function execution failed: %v", r, contID)
		return fmt.Errorf("[%s] Execution failed: %v", r, err)
	}

	if !response.Success {
		// notify scheduler
		completions <- &completion{scheduledRequest: r, contID: contID}
		// log.Printf("[%s] Function execution failed: %v", r, contID)
		return fmt.Errorf("Function execution failed")
	}

	r.ExecReport.Result = response.Result
	r.ExecReport.Duration = time.Now().Sub(t0).Seconds() - invocationWait.Seconds()
	r.ExecReport.ResponseTime = time.Now().Sub(r.Arrival).Seconds()
	r.ExecReport.Cost = config.GetFloat(config.EDGE_COST_FACTOR, 0.01) * r.ExecReport.Duration / 3600 * (float64(r.Fun.MemoryMB) / 1024)
	node.Resources.NodeExpenses += r.ExecReport.Cost

	// initializing containers may require invocation retries, adding
	// latency
	r.ExecReport.InitTime += invocationWait.Seconds()

	// notify scheduler
	completions <- &completion{scheduledRequest: r, contID: contID}
	// log.Printf("[%s] Execution completed on: %v", r, contID)

	return nil
}
