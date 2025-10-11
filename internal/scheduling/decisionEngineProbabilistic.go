package scheduling

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

type decisionEngineProbabilistic struct {
	mg *metricGrabberDQN
}

func (d *decisionEngineProbabilistic) CanAffordCloudOffloading(r *scheduledRequest) bool {
	//TODO implement me
	panic("implement me")
}

func (d *decisionEngineProbabilistic) CalculateExpectedCost(r *scheduledRequest) float64 {
	//TODO implement me
	panic("implement me")
}

var probabilities map[string]map[string][]float64

var globalRand *rand.Rand

func (d *decisionEngineProbabilistic) Decide(r *scheduledRequest) int {
	function := r.Fun.Name
	class := r.ClassService.Name

	var pL float64
	var pC float64
	var pE float64
	var pD float64

	if classes, ok := probabilities[function]; ok {
		if probs, ok := classes[class]; ok {
			log.Printf("Probability for %s - %s: %v\n", function, class, probs)
			pL = probs[0]
			pC = probs[1]
			pE = probs[2]
			pD = probs[3]
		} else {
			panic("Class not found")
		}
	} else {
		panic("Function not found")
	}

	if !r.CanDoOffloading {
		// Can be executed only locally or dropped
		pD = pD / (pD + pL)
		pL = pL / (pD + pL)
		pC = 0
		pE = 0
	} else if !canExecute(r.Fun) {
		// Node can't execute function locally
		if pD == 0 && pC == 0 && pE == 0 {
			pD = 0
			pC = 0.5
			pE = 0.5
			pL = 0
		} else {
			pD = pD / (pD + pC + pE)
			pC = pC / (pD + pC + pE)
			pE = pE / (pD + pC + pE)
			pL = 0
		}
	}

	//log.Printf("Probabilities after evaluation for %s-%s are pL:%f pE:%f pC:%f pD:%f", function, class, pL, pE, pC, pD)

	prob := globalRand.Float64()
	log.Printf("prob: %f -> [%f,%f,%f,%f]", prob, pL, pE, pC, pD)
	if prob <= pL {
		//log.Println("Execute LOCAL")
		return LOCAL_EXEC_REQUEST
	} else if prob <= pL+pE {
		//log.Println("Execute EDGE OFFLOAD")
		return EDGE_OFFLOAD_REQUEST
	} else if prob <= pL+pE+pC {
		//log.Println("Execute CLOUD OFFLOAD")
		return CLOUD_OFFLOAD_REQUEST
	} else {
		//log.Println("Execute DROP")
		d.mg.addStats(r, true, false)
		return DROP_REQUEST
	}
}

func (d *decisionEngineProbabilistic) InitDecisionEngine() {
	// Initializing probabilities
	probFilePath := "dqn_utils/probs.json"

	file, err := os.Open(probFilePath)
	if err != nil {
		log.Println("Errore nell'apertura del file:", err)
		return
	}
	defer file.Close()
	var data map[string][]float64
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		log.Println("Errore nella decodifica JSON:", err)
		return
	}
	probabilities = make(map[string]map[string][]float64)
	for key, values := range data {
		parts := strings.Split(key, "_")
		if len(parts) != 2 {
			log.Println("Chiave non valida:", key)
			continue
		}
		function := parts[0]
		class := parts[1]
		if _, exists := probabilities[function]; !exists {
			probabilities[function] = make(map[string][]float64)
		}
		probabilities[function][class] = values
	}
	// log.Println("Probability Map:")
	// for function, classes := range probabilities {
	//     log.Printf("%s: {\n", function)
	//     for class, values := range classes {
	//         log.Printf("  %s: %v\n", class, values)
	//     }
	//     log.Println("}")
	// }

	globalRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	d.mg = InitMG()
}

func (d *decisionEngineProbabilistic) Completed(r *scheduledRequest, offloaded int) {
	// FIXME AUDIT log.Println("COMPLETED: in decisionEngineProbabilistic")
	offloadDrop := offloaded != 0
	d.mg.addStats(r, false, offloadDrop)
}

func (d *decisionEngineProbabilistic) GetGrabber() metricGrabber {
	return nil
}
