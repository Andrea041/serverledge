package scheduling

import (
	"log"

	"github.com/grussorusso/serverledge/internal/node"
)

type DQNPolicy struct {
}

var e decisionEngine

func (p *DQNPolicy) Init() {
	// initialize decision engine
	e = &decisionEngineDQN{}
	e.InitDecisionEngine()
}

func (p *DQNPolicy) OnCompletion(r *scheduledRequest) {
	// log.Printf("Completed execution of %s in %f\n", r.Fun.Name, r.ExecReport.ResponseTime)
	if r.ExecReport.HasBeenDropped {
		e.Completed(r, 1)
	} else {
		e.Completed(r, 0)
	}
}

func (p *DQNPolicy) OnArrival(r *scheduledRequest) {
	dec := e.Decide(r)

	if dec == LOCAL_EXEC_REQUEST {
		containerID, err := node.AcquireWarmContainer(r.Fun)
		if err == nil {
			log.Printf("Using a warm container for: %v", r)
			execLocallyWithBudget(r, containerID, true)
		} else if !handleColdStart(r) {
			log.Printf("No warm containers for: %v - COLD START", r)
			// panic("ERRORE (dqnPolicy): quì non dovrebbe entrare perchè il filtro dovrebbe impedirglielo!")
		}
	} else if dec == CLOUD_OFFLOAD_REQUEST {
		tryCloudOffloadDQN(r)
	} else if dec == EDGE_OFFLOAD_REQUEST {
		url := pickEdgeNodeForOffloading(r)
		if url == "" {
			// può succedere per probllemi di aggiornamento tra nodi
			dropRequest(r)
			// panic("ERRORE (dqnPolicy): quì non dovrebbe entrare perchè se sceglie OFFLOADED_EDGE deve poterlo fare!")
		}
		tryEdgeOffload(r, url)
	} else if dec == DROP_REQUEST {
		dropRequest(r)
	} else {
		panic("ERRORE (dqnPolicy): è entrato nell'else (quindi -1 in decisionEngineDQN)!")
	}
}

func tryCloudOffloadDQN(r *scheduledRequest) {
	if canAffordCloudOffloading(r, true) {
		handleCloudOffload(r)
	} else {
		dropRequest(r)
	}
}
