package scheduling

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
	"unicode"

	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/node"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

const INFLUXDB = "[INFLUXDB]:"

type ClassPenalties struct {
	Name            string  `json:"name"`
	DeadlinePenalty float64 `json:"deadlinePenalty"`
	DropPenalty     float64 `json:"dropPenalty"`
}

type DQNStats struct {
	Exec  []float64 // list of seconds in which exec append
	Cloud []float64 // list of seconds in which offload cloud append
	Edge  []float64 // list of seconds in which offload edge append
	Drop  []float64 // list of seconds in which drop append

	Reward          []float64 // list of rewards obtained
	DeadlinePenalty []float64 // list of deadline penalties
	DropPenalty     []float64 // list of drop penalties
	Cost            []float64 // list of costs

	// counts how many times an action has been taken for that class
	// LOCAL(0)-CLOUD(1)-EDGE(2)-DROP(3)
	Standard  []int
	Critical1 []int
	Critical2 []int
	Batch     []int

	// counts how many times an action has been taken for that function
	// LOCAL(0)-CLOUD(1)-EDGE(2)-DROP(3)
	f1 []int
	f2 []int
	f3 []int
	f4 []int
	f5 []int

	// stats for function (f1,f2,f3,f4,f5)
	ResponseTime        [][]float64
	IsWarmStart         [][]int // [[f1_warm,f1_cold], ... ]
	InitTime            [][]float64
	Duration            [][]float64
	OffloadLatencyCloud [][]float64
	OffloadLatencyEdge  [][]float64

	// [utility,deadlinePenalty,dropPenalty] per action
	UPExec  []float64
	UPCloud []float64
	UPEdge  []float64

	// [utility,deadlinePenalty,dropPenalty] per class
	UPStandard  []float64
	UPCritical1 []float64
	UPCritical2 []float64
	UPBatch     []float64

	// [utility,deadlinePenalty,dropPenalty] per function
	UPf1 []float64
	UPf2 []float64
	UPf3 []float64
	UPf4 []float64
	UPf5 []float64
}

var penaltyMap map[string][]float64

var stats DQNStats
var muStats sync.Mutex

var initTime time.Time

var updateEvery = config.GetInt(config.DQN_STORE_STATS_EVERY, 3600)
var updateRound = 0

// metricGrabberDQN encapsulates the InfluxDB client and configuration
type metricGrabberDQN struct {
	client   influxdb2.Client
	org      string
	bucket   string
	writeAPI api.WriteAPIBlocking
	m        map[string]*functionInfo
}

func (mg *metricGrabberDQN) InitMetricGrabber() {
	// TODO Per ora sembra non servire
}

func (mg *metricGrabberDQN) Completed(r *scheduledRequest, offloaded int) {
	requestChannel <- completedRequest{
		scheduledRequest: r,
		location:         offloaded,
		dropped:          false,
	}
}

/*
Function that:
- Handles the evaluation and calculation of the local, edge and cloud probabilities.
- Writes the report of the request completion into the data store (influxdb).
- With the arrival of a new request, initializes new functionInfo and classFunctionInfo objects.
*/
func (mg *metricGrabberDQN) handler() {
	evaluationTicker :=
		time.NewTicker(evaluationInterval)
	for {
		select {
		case _ = <-evaluationTicker.C: // Evaluation handler
			s := rand.NewSource(time.Now().UnixNano())
			rGen = rand.New(s)
			//log.Println("Evaluating")

			//Check if there are some instances with 0 arrivals
			for fName, fInfo := range mg.m {
				for cName, cFInfo := range fInfo.invokingClasses {
					//Cleanup
					if cFInfo.arrivalCount == 0 {
						cFInfo.timeSlotsWithoutArrivals++
						if cFInfo.timeSlotsWithoutArrivals >= maxTimeSlots {
							log.Println("DELETING", fName, cName)
							mg.Delete(fName, cName)
						}
					}
				}
			}

			//d.deleteOldData(24 * time.Hour)
			mg.queryMetrics()
			mg.updateProbabilities()

		case r := <-requestChannel: // Result storage handler
			// New request completed or dropped - added data to influxdb - need to differentiate between edge offloading and cloud offloading
			// completed: true if the completed request is not dropped
			// offloaded: true if the request is offloaded to another node
			// offloaded_cloud: true if the completed request is offloaded vertically
			// offloaded_edge: true if the completed request is offloaded horizontally
			// warm_start: true if there were available instances to execute the function locally
			// fKeys: contains extra information about the function execution
			// - duration
			// - init_time
			// FIXME AUDIT log.Println("Result storage handler - adding data to influxdb")

			var fKeys map[string]interface{}
			offloaded := "false"
			offloadedCloud := "false"
			warmStart := "false"
			completed := "false"

			// If the request was dropped, then update the respective value in the node structure
			if r.dropped {
				node.Resources.DropRequestsCount += 1
				fKeys = map[string]interface{}{
					"duration":   r.ExecReport.Duration,
					"init_time":  r.ExecReport.InitTime,
					"input_size": r.ExecReport.InputSize,
				}
				completed = "false"
			} else {
				fKeys = map[string]interface{}{
					"duration":   r.ExecReport.Duration,
					"init_time":  r.ExecReport.InitTime,
					"input_size": r.ExecReport.InputSize,
				}
				completed = "true"
			}

			if r.ExecReport.OffloadLatencyCloud != 0 {
				offloaded = "true"
				offloadedCloud = "true"
				fKeys["offload_latency_cloud"] = r.ExecReport.OffloadLatencyCloud
			}

			if r.ExecReport.OffloadLatencyEdge != 0 {
				offloaded = "true"
				offloadedCloud = "false"
				fKeys["offload_latency_edge"] = r.ExecReport.OffloadLatencyEdge
			}

			if r.ExecReport.IsWarmStart {
				warmStart = "true"
			}

			p := influxdb2.NewPoint(r.Fun.Name,
				map[string]string{
					"class":           r.ClassService.Name,
					"offloaded":       offloaded,
					"offloaded_cloud": offloadedCloud,
					"warm_start":      warmStart,
					"completed":       completed},
				fKeys,
				time.Now())

			writeAPI.WritePoint(p)
			// FIXME AUDIT log.Println("ADDED NEW POINT INTO INFLUXDB")

		case arr := <-arrivalChannel: // Arrival handler - structures initialization
			// A new request is arrived: update the counter of incoming request in the node structure
			// FIXME AUDIT log.Println("NEW ARRIVAL!")
			node.Resources.RequestsCount += 1

			name := arr.Fun.Name
			fInfo, prs := mg.m[name]
			if !prs {
				fInfo = &functionInfo{
					name:            name,
					memory:          arr.Fun.MemoryMB,
					cpu:             arr.Fun.CPUDemand,
					probCold:        [3]float64{0, 0, 0},
					bandwidthCloud:  0,
					bandwidthEdge:   0,
					meanInputSize:   100,
					invokingClasses: make(map[string]*classFunctionInfo)}

				mg.m[name] = fInfo
			}

			cFInfo, prs := fInfo.invokingClasses[arr.class]
			if !prs {
				cFInfo = &classFunctionInfo{functionInfo: fInfo,
					probExecuteLocal:         startingLocalProb,
					probOffloadCloud:         startingCloudOffloadProb,
					probOffloadEdge:          startingEdgeOffloadProb,
					probDrop:                 1 - (startingLocalProb + startingCloudOffloadProb + startingEdgeOffloadProb),
					arrivals:                 0,
					arrivalCount:             0,
					timeSlotsWithoutArrivals: 0,
					className:                arr.class}

				fInfo.invokingClasses[arr.class] = cFInfo
			}

			cFInfo.timeSlotsWithoutArrivals = 0

		}
	}
}

func (mg *metricGrabberDQN) GrabFunctionInfo(functionName string) (*functionInfo, bool) {
	fInfo, prs := mg.m[functionName]

	return fInfo, prs
}

func EmptyStats() DQNStats {
	stats = DQNStats{
		Exec:                []float64{},
		Cloud:               []float64{},
		Edge:                []float64{},
		Drop:                []float64{},
		Reward:              []float64{},
		DeadlinePenalty:     []float64{},
		DropPenalty:         []float64{},
		Cost:                []float64{},
		Standard:            []int{0, 0, 0, 0},
		Critical1:           []int{0, 0, 0, 0},
		Critical2:           []int{0, 0, 0, 0},
		Batch:               []int{0, 0, 0, 0},
		f1:                  []int{0, 0, 0, 0},
		f2:                  []int{0, 0, 0, 0},
		f3:                  []int{0, 0, 0, 0},
		f4:                  []int{0, 0, 0, 0},
		f5:                  []int{0, 0, 0, 0},
		ResponseTime:        [][]float64{{}, {}, {}, {}, {}},
		IsWarmStart:         [][]int{{0, 0}, {0, 0}, {0, 0}, {0, 0}, {0, 0}},
		InitTime:            [][]float64{{}, {}, {}, {}, {}},
		Duration:            [][]float64{{}, {}, {}, {}, {}},
		OffloadLatencyCloud: [][]float64{{}, {}, {}, {}, {}},
		OffloadLatencyEdge:  [][]float64{{}, {}, {}, {}, {}},
		UPExec:              []float64{0, 0, 0},
		UPCloud:             []float64{0, 0, 0},
		UPEdge:              []float64{0, 0, 0},
		UPStandard:          []float64{0, 0, 0},
		UPCritical1:         []float64{0, 0, 0},
		UPCritical2:         []float64{0, 0, 0},
		UPBatch:             []float64{0, 0, 0},
		UPf1:                []float64{0, 0, 0},
		UPf2:                []float64{0, 0, 0},
		UPf3:                []float64{0, 0, 0},
		UPf4:                []float64{0, 0, 0},
		UPf5:                []float64{0, 0, 0},
	}
	return stats
}

// Initializes and returns a new metricGrabberDQN instance
func InitMG() *metricGrabberDQN {
	stats = EmptyStats()

	initTime = time.Now()

	// retrieve penalties from json classes file
	file, err := os.Open("serverledge-classes.json")
	if err != nil {
		log.Println("%s Error opening classes file\n", INFLUXDB)
		panic(err)
	}
	defer file.Close()
	var penalties []ClassPenalties
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&penalties)
	if err != nil {
		log.Println("%s Error during JSON decode\n", INFLUXDB)
		panic(err)
	}
	penaltyMap = make(map[string][]float64)
	for _, penalty := range penalties {
		penaltyMap[penalty.Name] = []float64{penalty.DeadlinePenalty, penalty.DropPenalty}
	}

	org := config.GetString(config.STORAGE_DB_ORGNAME, "serverledge")
	url := config.GetString(config.STORAGE_DB_ADDRESS, "http://localhost:8086")
	token := config.GetString(config.STORAGE_DB_TOKEN, "serverledge")
	bucket := config.GetString(config.STORAGE_DB_BUCKET, "dqn")

	client := influxdb2.NewClient(url, token)
	writeAPI := client.WriteAPIBlocking(org, bucket)
	return &metricGrabberDQN{
		client:   client,
		org:      org,
		bucket:   bucket,
		writeAPI: writeAPI,
	}
}

func (mg *metricGrabberDQN) addStats(r *scheduledRequest, actionDrop bool, offloadDrop bool) {
	muStats.Lock()
	defer muStats.Unlock()

	dropped := false
	if actionDrop || offloadDrop {
		dropped = true
	}
	outOfTime := false
	if !dropped && r.ExecReport.ResponseTime > r.ClassService.MaximumResponseTime {
		outOfTime = true
	}

	elapsedTime := r.Arrival.Sub(initTime)
	elapsedTimeInSeconds := float64(elapsedTime.Seconds())

	// actionDrop (and not dropped) cause i want the chosen action not the outcome of the request
	if actionDrop {
		stats.Drop = append(stats.Drop, elapsedTimeInSeconds)
	} else {
		switch r.ExecReport.SchedAction {
		case "O_C":
			stats.Cloud = append(stats.Cloud, float64(elapsedTime.Seconds()))
		case "O_E":
			stats.Edge = append(stats.Edge, float64(elapsedTime.Seconds()))
		default:
			stats.Exec = append(stats.Exec, float64(elapsedTime.Seconds()))
		}
	}

	// 'dropped' cause i want the outcome of the request
	if !dropped && !outOfTime {
		stats.Reward = append(stats.Reward, r.ClassService.Utility)
		stats.DropPenalty = append(stats.DropPenalty, 0)
		stats.DeadlinePenalty = append(stats.DeadlinePenalty, 0)
	} else {
		stats.Reward = append(stats.Reward, 0)
		penalties, exists := penaltyMap[r.ClassService.Name]
		if !exists {
			penalties = []float64{0, 0} // for default class
		}
		if dropped {
			stats.DropPenalty = append(stats.DropPenalty, penalties[0])
			stats.DeadlinePenalty = append(stats.DeadlinePenalty, 0)
		} else {
			stats.DropPenalty = append(stats.DropPenalty, 0)
			stats.DeadlinePenalty = append(stats.DeadlinePenalty, penalties[1])
		}
	}

	stats.Cost = append(stats.Cost, r.ExecReport.Cost)

	// actionDrop (and not dropped) cause i want the chosen action per class, not the outcome of the request
	switch r.ClassService.Name {
	case "batch":
		updateActionStats(&stats.Batch, actionDrop, r.ExecReport.SchedAction)
	case "critical-1":
		updateActionStats(&stats.Critical1, actionDrop, r.ExecReport.SchedAction)
	case "critical-2":
		updateActionStats(&stats.Critical2, actionDrop, r.ExecReport.SchedAction)
	default:
		updateActionStats(&stats.Standard, actionDrop, r.ExecReport.SchedAction)
	}

	// actionDrop (and not dropped) cause i want the chosen action per function, not the outcome of the request
	switch r.Fun.Name {
	case "f1":
		updateActionStats(&stats.f1, actionDrop, r.ExecReport.SchedAction)
	case "f2":
		updateActionStats(&stats.f2, actionDrop, r.ExecReport.SchedAction)
	case "f3":
		updateActionStats(&stats.f3, actionDrop, r.ExecReport.SchedAction)
	case "f4":
		updateActionStats(&stats.f4, actionDrop, r.ExecReport.SchedAction)
	default:
		updateActionStats(&stats.f5, actionDrop, r.ExecReport.SchedAction)
	}

	// 'dropped' cause those are stats for completions
	if !dropped {
		var numStr string
		for _, char := range r.Fun.Name {
			if unicode.IsDigit(char) {
				numStr += string(char)
			}
		}
		index, err := strconv.Atoi(numStr)
		if err != nil {
			log.Fatalf("%s Error during function number conversion:%v\n", INFLUXDB, err)
		}
		index--

		stats.ResponseTime[index] = append(stats.ResponseTime[index], r.ExecReport.ResponseTime)
		if r.ExecReport.IsWarmStart {
			stats.IsWarmStart[index][0]++
		} else {
			stats.IsWarmStart[index][1]++
		}
		stats.InitTime[index] = append(stats.InitTime[index], r.ExecReport.InitTime)
		stats.Duration[index] = append(stats.Duration[index], r.ExecReport.Duration)
		if r.ExecReport.SchedAction == "O_C" {
			stats.OffloadLatencyCloud[index] = append(stats.OffloadLatencyCloud[index], r.ExecReport.OffloadLatencyCloud)
		} else if r.ExecReport.SchedAction == "O_E" {
			stats.OffloadLatencyEdge[index] = append(stats.OffloadLatencyEdge[index], r.ExecReport.OffloadLatencyEdge)
		}
	}

	if r.ExecReport.SchedAction == "O_C" {
		if offloadDrop {
			stats.UPCloud[2]++
		} else if outOfTime {
			stats.UPCloud[1]++
		} else {
			stats.UPCloud[0]++
		}
	} else if r.ExecReport.SchedAction == "O_E" {
		if offloadDrop {
			stats.UPEdge[2]++
			// panic("ERRORE (metricGrabberDQN): quì non dovrebbe entrare perchè se sceglie OFFLOADED_EDGE deve poterlo fare!")
		} else if outOfTime {
			stats.UPEdge[1]++
		} else {
			stats.UPEdge[0]++
		}
	} else {
		if actionDrop {
			stats.UPExec[2]++
		} else if outOfTime {
			stats.UPExec[1]++
		} else {
			stats.UPExec[0]++
		}
	}

	switch r.ClassService.Name {
	case "batch":
		if dropped {
			stats.UPBatch[2]++
		} else if outOfTime {
			stats.UPBatch[1]++
		} else {
			stats.UPBatch[0]++
		}
	case "critical-1":
		if dropped {
			stats.UPCritical1[2]++
		} else if outOfTime {
			stats.UPCritical1[1]++
		} else {
			stats.UPCritical1[0]++
		}
	case "critical-2":
		if dropped {
			stats.UPCritical2[2]++
		} else if outOfTime {
			stats.UPCritical2[1]++
		} else {
			stats.UPCritical2[0]++
		}
	default:
		if dropped {
			stats.UPStandard[2]++
		} else if outOfTime {
			stats.UPStandard[1]++
		} else {
			stats.UPStandard[0]++
		}
	}

	switch r.Fun.Name {
	case "f1":
		if dropped {
			stats.UPf1[2]++
		} else if outOfTime {
			stats.UPf1[1]++
		} else {
			stats.UPf1[0]++
		}
	case "f2":
		if dropped {
			stats.UPf2[2]++
		} else if outOfTime {
			stats.UPf2[1]++
		} else {
			stats.UPf2[0]++
		}
	case "f3":
		if dropped {
			stats.UPf3[2]++
		} else if outOfTime {
			stats.UPf3[1]++
		} else {
			stats.UPf3[0]++
		}
	case "f4":
		if dropped {
			stats.UPf4[2]++
		} else if outOfTime {
			stats.UPf4[1]++
		} else {
			stats.UPf4[0]++
		}
	default:
		if dropped {
			stats.UPf5[2]++
		} else if outOfTime {
			stats.UPf5[1]++
		} else {
			stats.UPf5[0]++
		}
	}

	// add stats to InfluxDB every 'updateEvery'
	if elapsedTimeInSeconds-float64(updateEvery*updateRound) > float64(updateEvery) {
		if elapsedTimeInSeconds-float64(updateEvery*updateRound) > float64(updateEvery) {
			mg.WriteJSON()
			stats = EmptyStats()
			updateRound++
		}
	}
}

func updateActionStats(slice *[]int, dropped bool, schedAction string) {
	index := 3
	if !dropped {
		switch schedAction {
		case "O_C":
			index = 1
		case "O_E":
			index = 2
		default:
			index = 0
		}
	}
	(*slice)[index]++
}

// Writes stats as a JSON object to InfluxDB
func (mg *metricGrabberDQN) WriteJSON() {
	// Mappa di nomi delle variabili e i loro valori
	parts := map[string]interface{}{
		"Exec":                stats.Exec,
		"Cloud":               stats.Cloud,
		"Edge":                stats.Edge,
		"Drop":                stats.Drop,
		"Reward":              stats.Reward,
		"DeadlinePenalty":     stats.DeadlinePenalty,
		"DropPenalty":         stats.DropPenalty,
		"Cost":                stats.Cost,
		"Standard":            stats.Standard,
		"Critical1":           stats.Critical1,
		"Critical2":           stats.Critical2,
		"Batch":               stats.Batch,
		"f1":                  stats.f1,
		"f2":                  stats.f2,
		"f3":                  stats.f3,
		"f4":                  stats.f4,
		"f5":                  stats.f5,
		"ResponseTime":        stats.ResponseTime,
		"IsWarmStart":         stats.IsWarmStart,
		"InitTime":            stats.InitTime,
		"Duration":            stats.Duration,
		"OffloadLatencyCloud": stats.OffloadLatencyCloud,
		"OffloadLatencyEdge":  stats.OffloadLatencyEdge,
		"UPExec":              stats.UPExec,
		"UPCloud":             stats.UPCloud,
		"UPEdge":              stats.UPEdge,
		"UPStandard":          stats.UPStandard,
		"UPCritical1":         stats.UPCritical1,
		"UPCritical2":         stats.UPCritical2,
		"UPBatch":             stats.UPBatch,
		"UPf1":                stats.UPf1,
		"UPf2":                stats.UPf2,
		"UPf3":                stats.UPf3,
		"UPf4":                stats.UPf4,
		"UPf5":                stats.UPf5,
	}

	for name, part := range parts {
		// Convert DQNStats to JSON string
		jsonData, err := json.Marshal(part)
		if err != nil {
			log.Fatalf("%s Error marshalling JSON part '%s': %v\n", INFLUXDB, name, err)
		}

		// Create a new data point
		point := influxdb2.NewPointWithMeasurement("dqn_stats").
			AddTag("name", name).
			AddField("json_data", string(jsonData)).
			SetTime(time.Now().UTC())

		// Write the point to InfluxDB
		err = mg.writeAPI.WritePoint(context.Background(), point)
		if err != nil {
			log.Fatalf("%s Error writing point to InfluxDB for part '%s': %v\n", INFLUXDB, name, err)
		}
	}

	log.Println(INFLUXDB, "Statistics successfully written to InfluxDB")
}

// Closes the InfluxDB client connection
func (mg *metricGrabberDQN) Close() {
	mg.client.Close()
}

func (mg *metricGrabberDQN) Delete(name string, name2 string) {

}

func (mg *metricGrabberDQN) queryMetrics() {
	//TODO edit time window
	searchInterval := 24 * time.Hour

	//Query for arrivals
	for _, fInfo := range mg.m {
		for _, cFInfo := range fInfo.invokingClasses {
			cFInfo.arrivals = 0
		}
	}

	start := time.Now().Add(-evaluationInterval)
	query := fmt.Sprintf(`from(bucket: "%s")
										|> range(start: %d)
										|> filter(fn: (r) => r["_field"] == "duration")
										|> group(columns: ["_measurement", "class"])
									    |> aggregateWindow(every: 1s, fn: count, createEmpty: true)
									    |> mean()`, bucketName, start.Unix())

	result, err := queryAPI.Query(context.Background(), query)
	if err == nil {
		// Iterate over query response
		for result.Next() {
			x := result.Record().Values()
			val := result.Record().Value().(float64)
			funct := x["_measurement"].(string)
			class := x["class"].(string)

			fInfo, prs := mg.m[funct] // access function map in Decision Engine
			if !prs {
				f, _ := function.GetFunction(funct)
				fInfo = &functionInfo{
					name:            funct,
					memory:          f.MemoryMB,
					cpu:             f.CPUDemand,
					probCold:        [3]float64{0, 0, 0},
					invokingClasses: make(map[string]*classFunctionInfo)}

				mg.m[funct] = fInfo
			}

			//timeWindow := 25 * 60.0
			cFInfo, prs := fInfo.invokingClasses[class]
			if !prs {
				cFInfo = &classFunctionInfo{functionInfo: fInfo,
					probExecuteLocal:         startingLocalProb,
					probOffloadCloud:         startingCloudOffloadProb,
					probDrop:                 1 - (startingLocalProb + startingCloudOffloadProb),
					arrivals:                 0,
					arrivalCount:             0,
					timeSlotsWithoutArrivals: 0,
					className:                class}

				fInfo.invokingClasses[class] = cFInfo
			}
			cFInfo.arrivals = val
			// FIXME REMOVE log.Println("Recovered arrivals from influxDb: ", cFInfo.arrivals)

			//Reset deletion
			cFInfo.timeSlotsWithoutArrivals = 0
		}

		// check for an error
		if result.Err() != nil {
			log.Printf("query parsing error: %s\n", result.Err().Error())
		}
	} else {
		log.Println("DB error", err)
	}

	// Query for meanDuration
	start = time.Now().Add(-searchInterval)
	query = fmt.Sprintf(`from(bucket: "%s")
										|> range(start: %d)
										|> group(columns: ["_measurement", "offloaded", "offloaded_cloud"])
										|> filter(fn: (r) => r["_field"] == "duration" and r["completed"] == "true")
										|> tail(n: %d)
										|> exponentialMovingAverage(n: %d)`, bucketName, start.Unix(), 100, 100)

	result, err = queryAPI.Query(context.Background(), query)
	if err == nil {
		// Iterate over query response
		for result.Next() {
			x := result.Record().Values()
			val := result.Record().Value().(float64)

			funct := x["_measurement"].(string)
			off := x["offloaded"].(string)
			offCloud := x["offloaded_cloud"].(string)

			// retrieve location value to check if the function was executed locally, on cloud or on edge
			location := LOCAL
			if off == "true" && offCloud == "true" {
				location = OFFLOADED_CLOUD
			} else if off == "true" && offCloud == "false" {
				location = OFFLOADED_EDGE
			}
			fInfo, prs := mg.m[funct]
			if !prs {
				continue
			}

			fInfo.meanDuration[location] = val
		}

		// check for an error
		if result.Err() != nil {
			log.Printf("query parsing error: %s\n", result.Err().Error())
		}
	} else {
		log.Println("DB error", err)
	}

	// Query for OffloadLatencyCloud
	query = fmt.Sprintf(`from(bucket: "%s")
										|> range(start: %d)
										|> filter(fn: (r) => r["_field"] == "offload_latency_cloud" and r["completed"] == "true")
										|> group()
										|> median()`, bucketName, start.Unix())

	result, err = queryAPI.Query(context.Background(), query)
	if err == nil {
		// Iterate over query response
		for result.Next() {
			CloudOffloadLatency = result.Record().Values()["_value"].(float64)
		}

		// check for an error
		if result.Err() != nil {
			log.Printf("query parsing error: %s\n", result.Err().Error())
		}
	} else {
		log.Println("DB error", err)
	}

	// Query for offloadLatencyEdge
	query = fmt.Sprintf(`from(bucket: "%s")
										|> range(start: %d)
										|> filter(fn: (r) => r["_field"] == "offload_latency_edge" and r["completed"] == "true")
										|> group()
										|> median()`, bucketName, start.Unix())

	result, err = queryAPI.Query(context.Background(), query)
	if err == nil {
		// Iterate over query response
		for result.Next() {
			EdgeOffloadLatency = result.Record().Values()["_value"].(float64)
		}

		// check for an error
		if result.Err() != nil {
			log.Printf("query parsing error: %s\n", result.Err().Error())
		}
	} else {
		log.Println("DB error", err)
	}

	//Query for initTime
	query = fmt.Sprintf(`from(bucket: "%s")
										|> range(start: %d)
										|> group(columns: ["_measurement", "offloaded", "offloaded_cloud"])
										|> filter(fn: (r) => r["_field"] == "init_time" and r["warm_start"] == "false" and r["completed"] == "true")
										|> tail(n: %d)
										|> exponentialMovingAverage(n: %d)`, bucketName, start.Unix(), 100, 100)
	result, err = queryAPI.Query(context.Background(), query)
	if err == nil {
		// Iterate over query response
		for result.Next() {
			x := result.Record().Values()
			val := result.Record().Value().(float64)

			funct := x["_measurement"].(string)
			off := x["offloaded"].(string)
			offCloud := x["offloaded_cloud"].(string)

			location := LOCAL
			if off == "true" && offCloud == "true" {
				location = OFFLOADED_CLOUD
			} else if off == "true" && offCloud == "false" {
				location = OFFLOADED_EDGE
			}

			fInfo, prs := mg.m[funct]
			if !prs {
				continue
			}

			fInfo.initTime[location] = val
		}

		// check for an error
		if result.Err() != nil {
			log.Printf("query parsing error: %s\n", result.Err().Error())
		}
	} else {
		log.Println("DB error", err)
	}

	// Query for input size
	query = fmt.Sprintf(`from(bucket: "%s")
										|> range(start: %d)
										|> group(columns: ["_measurement"])
										|> filter(fn: (r) => r["_field"] == "input_size" and r["completed"] == "true")
										|> tail(n: %d)
										|> exponentialMovingAverage(n: %d)`, bucketName, start.Unix(), 100, 100)
	result, err = queryAPI.Query(context.Background(), query)
	if err == nil {
		// Iterate over query response
		for result.Next() {
			x := result.Record().Values()
			val := result.Record().Value().(float64)

			funct := x["_measurement"].(string)
			fInfo, prs := mg.m[funct]
			if !prs {
				continue
			}
			fInfo.meanInputSize = val
		}

		// check for an error
		if result.Err() != nil {
			log.Printf("query parsing error: %s\n", result.Err().Error())
		}
	} else {
		log.Println("DB error", err)
	}

	// Query for count and coldStartCount
	query = fmt.Sprintf(`from(bucket: "%s")
										|> range(start: %d)
  										|> filter(fn: (r) => r["_field"] == "duration" and r["completed"] == "true")
										|> group(columns: ["_measurement", "offloaded", "offloaded_cloud", "warm_start"])
										|> count()`, bucketName, start.Unix())

	result, err = queryAPI.Query(context.Background(), query)
	if err == nil {
		// Iterate over query response
		for result.Next() {
			x := result.Record().Values()
			val := result.Record().Value().(int64)

			funct := x["_measurement"].(string)
			off := x["offloaded"].(string)
			offCloud := x["offloaded_cloud"].(string)
			warmStart := x["warm_start"].(string)

			location := LOCAL
			if off == "true" && offCloud == "true" {
				location = OFFLOADED_CLOUD
			} else if off == "true" && offCloud == "false" {
				location = OFFLOADED_EDGE
			}

			fInfo, prs := mg.m[funct]
			if !prs {
				continue
			}

			if warmStart == "true" {
				fInfo.count[location] = val
			} else {
				fInfo.coldStartCount[location] = val
			}
		}

		// check for an error
		if result.Err() != nil {
			log.Printf("query parsing error: %s\n", result.Err().Error())
		}
	} else {
		log.Println("DB error", err)
	}

	for _, fInfo := range mg.m {
		// If none cold start happened in a specific location (local, cloud or edge), then the cold start probability is optimistically 0
		for location := 0; location < 3; location++ {
			if fInfo.coldStartCount[location] == 0 {
				fInfo.probCold[location] = 0.0
			} else {
				fInfo.probCold[location] = float64(fInfo.coldStartCount[location]) / float64(fInfo.count[location]+fInfo.coldStartCount[location])
			}
		}
	}
}

func (mg *metricGrabberDQN) updateProbabilities() {
	solve(mg.m)
}
