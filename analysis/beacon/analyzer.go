package beacon

import (
	"fmt"
	"plugin"

	dataBeacon "github.com/activecm/rita/datatypes/beacon"
)

type MyFunction interface {
	GreetChunk([]*BeaconAnalysisInput, int64, int64, int) []*dataBeacon.BeaconAnalysisOutput
	Greet(*BeaconAnalysisInput, int64, int64, int) *dataBeacon.BeaconAnalysisOutput
}

func test(data []*BeaconAnalysisInput, minTime int64, maxTime int64, thresh int) []*dataBeacon.BeaconAnalysisOutput {
	// fmt.Println(data)
	var mod string
	mod = "../RITA-Labs/beacons/beacons.so"

	// load module
	// 1. open the so file to load the symbols
	plug, err := plugin.Open(mod)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	// 2. look up a symbol (an exported function or variable)
	// in this case, variable Greeter
	symGreeter, err := plug.Lookup("Greeter")
	if err != nil {
		fmt.Println(err)
		return nil
	}

	// 3. Assert that loaded symbol is of a desired type
	// in this case interface type Greeter (defined above)
	var greeter MyFunction
	greeter, ok := symGreeter.(MyFunction)
	if !ok {
		fmt.Println("unexpected type from module symbol")
		return nil
	}

	// 4. use the module
	return (greeter.GreetChunk(data, minTime, maxTime, thresh))
}

// start kicks off a new analysis thread
func analyzer_start(dataChunk []*BeaconAnalysisInput, minTime int64, maxTime int64, thresh int) []*dataBeacon.BeaconAnalysisOutput {
	var outputChunk []*dataBeacon.BeaconAnalysisOutput
	outputChunk = test(dataChunk, minTime, maxTime, thresh)
	return outputChunk
}
