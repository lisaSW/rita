package beacon

import (
	"fmt"
	"plugin"
	"sync"

	dataBeacon "github.com/activecm/rita/datatypes/beacon"

	"github.com/activecm/rita/util"
)

type (
	// analyzer implements the bulk of beaconing analysis, creating the scores
	// for a given set of timestamps and data sizes
	analyzer struct {
		connectionThreshold int                              // the minimum number of connections to be considered a beacon
		minTime             int64                            // beginning of the observation period
		maxTime             int64                            // ending of the observation period
		analyzedCallback    func(*dataBeacon.AnalysisOutput) // called on each analyzed result
		closedCallback      func()                           // called when .close() is called and no more calls to analyzedCallback will be made
		analysisChannel     chan *beaconAnalysisInput        // holds unanalyzed data
		analysisWg          sync.WaitGroup                   // wait for analysis to finish
	}
)

type MyFunction interface {
	Greet(*BeaconAnalysisInput, int64, int64, int) *dataBeacon.BeaconAnalysisOutput
}

// newAnalyzer creates a new analyzer for computing beaconing scores.
func newAnalyzer(connectionThreshold int, minTime, maxTime int64,
	analyzedCallback func(*dataBeacon.AnalysisOutput), closedCallback func()) *analyzer {
	return &analyzer{
		connectionThreshold: connectionThreshold,
		minTime:             minTime,
		maxTime:             maxTime,
		analyzedCallback:    analyzedCallback,
		closedCallback:      closedCallback,
		analysisChannel:     make(chan *BeaconAnalysisInput),
	}
}

// analyze sends a group of timestamps and data sizes in for analysis.
// Note: this function may block
func (a *analyzer) analyze(data *BeaconAnalysisInput) {
	//	fmt.Println("analyzer.go : analyzeChannel <- data")
	a.analysisChannel <- data

}

// close waits for the analysis threads to finish
func (a *analyzer) close() {
	//	fmt.Println("analyzer.go : close()")
	close(a.analysisChannel)
	a.analysisWg.Wait()
	a.closedCallback()
	//	fmt.Println("analyzer.go : close2")
}

func test(data *BeaconAnalysisInput, minTime int64, maxTime int64, thresh int) *dataBeacon.BeaconAnalysisOutput {
	// fmt.Println(data)
	var mod string
	mod = "../RITA-Labs/beacons/beacons.so"

	// load module
	// 1. open the so file to load the symbols
	plug, err := plugin.Open(mod)
	if err != nil {
		fmt.Println(err)
		fmt.Println("1")
	}

	// 2. look up a symbol (an exported function or variable)
	// in this case, variable Greeter
	symGreeter, err := plug.Lookup("Greeter")
	if err != nil {
		fmt.Println(err)
		fmt.Println("2")
		// os.Exit(1)
		return nil
	}

	// 3. Assert that loaded symbol is of a desired type
	// in this case interface type Greeter (defined above)
	var greeter MyFunction
	greeter, ok := symGreeter.(MyFunction)
	if !ok {
		fmt.Println("unexpected type from module symbol")
		// fmt.Println("3")
		// os.Exit(1)
		return nil
	}

	// 4. use the module
	// fmt.Println(reflect.TypeOf(greeter.Greet(data)))
	// fmt.Println(greeter.Greet(data, minTime, maxTime))
	return (greeter.Greet(data, minTime, maxTime, thresh))
}

// start kicks off a new analysis thread
func analyzer_start(dataChunk []*BeaconAnalysisInput, minTime int64, maxTime int64, thresh int) []*dataBeacon.BeaconAnalysisOutput {
	fmt.Println("analyzer.go : start()")
	// a.analysisWg.Add(1)
	var outputChunk []*dataBeacon.BeaconAnalysisOutput
	// go func() {
	//	fmt.Println("analyzer.go : go func")
	// counter := 0
	for _, data := range dataChunk {
		// fmt.Println(data)
		output := test(data, minTime, maxTime, thresh)

		// This adds the analyzed result to the writer channel, which writes that
		// individual result to the database collection
		if output != nil {
			// a.analyzedCallback(output)

			outputChunk = append(outputChunk, output)
		}
		// counter++
	}
	// a.analysisWg.Done()
	// }()
	// fmt.Println(outputChunk)
	return outputChunk
}

// createCountMap returns a distinct data array, data count array, the mode,
// and the number of times the mode occured
func createCountMap(sortedIn []int64) ([]int64, []int64, int64, int64) {
	//	fmt.Println("analyzer.go : create count map")
	//Since the data is already sorted, we can call this without fear
	distinct, countsMap := util.CountAndRemoveConsecutiveDuplicates(sortedIn)
	countsArr := make([]int64, len(distinct))
	mode := distinct[0]
	max := countsMap[mode]
	for i, datum := range distinct {
		count := countsMap[datum]
		countsArr[i] = count
		if count > max {
			max = count
			mode = datum
		}
	}
	return distinct, countsArr, mode, max
}
