package beacon

import (
	"math"
	"sort"

	"github.com/activecm/rita/datatypes/beacon"
	"github.com/activecm/rita/datatypes/structure"
)

// type MyFunction interface {
// 	GreetChunk([]structure.UniqueConnection, int64, int64, int) []*dataBeacon.AnalysisOutput
// 	Greet(structure.UniqueConnection, int64, int64, int) *dataBeacon.AnalysisOutput
// }

// func test(data []structure.UniqueConnection, minTime int64, maxTime int64, thresh int) []*dataBeacon.AnalysisOutput {
// 	// fmt.Println(data)
// 	var mod string
// 	mod = "../RITA-Labs/beacons/beacons.so"
//
// 	// load module
// 	// 1. open the so file to load the symbols
// 	plug, err := plugin.Open(mod)
// 	if err != nil {
// 		fmt.Println(err)
// 		return nil
// 	}
//
// 	// 2. look up a symbol (an exported function or variable)
// 	// in this case, variable Greeter
// 	symGreeter, err := plug.Lookup("Greeter")
// 	if err != nil {
// 		fmt.Println(err)
// 		return nil
// 	}
//
// 	// 3. Assert that loaded symbol is of a desired type
// 	// in this case interface type Greeter (defined above)
// 	var greeter MyFunction
// 	greeter, ok := symGreeter.(MyFunction)
// 	if !ok {
// 		fmt.Println("unexpected type from module symbol")
// 		return nil
// 	}
//
// 	// 4. use the module
// 	return (greeter.GreetChunk(data, minTime, maxTime, thresh))
// }

// start kicks off a new analysis thread
func analyzer_start(dataChunk []structure.UniqueConnection, minTime int64, maxTime int64) []*beacon.AnalysisOutput {
	// fmt.Println("start analyzer", len(dataChunk))
	var outputChunk []*beacon.AnalysisOutput
	outputChunk = analyzeChunk(dataChunk, minTime, maxTime)
	return outputChunk
}

// SortableInt64
type SortableInt64 []int64

func (s SortableInt64) Len() int           { return len(s) }
func (s SortableInt64) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s SortableInt64) Less(i, j int) bool { return s[i] < s[j] }

// rounding
func Round(f float64) int64 {
	return int64(math.Floor(f + .5))
}

//two's complement abs value
func Abs(a int64) int64 {
	mask := a >> 63
	a = a ^ mask
	return a - mask
}

func analyzeChunk(data []structure.UniqueConnection, minTime int64, maxTime int64) []*beacon.AnalysisOutput {
	var results []*beacon.AnalysisOutput
	for _, uconnPair := range data {
		res := analyzePair(uconnPair, minTime, maxTime)
		if res != nil {
			results = append(results, res)
		}

	}
	return results
}

func analyzePair(data structure.UniqueConnection, minTime int64, maxTime int64) *beacon.AnalysisOutput {

	// sort the size and timestamps since they may have arrived out of order
	sort.Sort(SortableInt64(data.Ts))
	sort.Sort(SortableInt64(data.OrigIPBytes))

	// store the diff slice length since we use it a lot
	// for timestamps this is one less then the data slice length
	// since we are calculating the times in between readings
	tsLength := len(data.Ts) - 1
	dsLength := len(data.OrigIPBytes)

	//find the duration of this connection
	//perfect beacons should fill the observation period
	duration := float64(data.Ts[tsLength]-data.Ts[0]) /
		float64(maxTime-minTime)

	//find the delta times between the timestamps
	diff := make([]int64, tsLength)
	for i := 0; i < tsLength; i++ {
		diff[i] = data.Ts[i+1] - data.Ts[i]
	}

	//perfect beacons should have symmetric delta time and size distributions
	//Bowley's measure of skew is used to check symmetry
	sort.Sort(SortableInt64(diff))
	tsSkew := float64(0)
	dsSkew := float64(0)

	//tsLength -1 is used since diff is a zero based slice
	tsLow := diff[Round(.25*float64(tsLength-1))]
	tsMid := diff[Round(.5*float64(tsLength-1))]
	tsHigh := diff[Round(.75*float64(tsLength-1))]
	tsBowleyNum := tsLow + tsHigh - 2*tsMid
	tsBowleyDen := tsHigh - tsLow

	//we do the same for datasizes
	dsLow := data.OrigIPBytes[Round(.25*float64(dsLength-1))]
	dsMid := data.OrigIPBytes[Round(.5*float64(dsLength-1))]
	dsHigh := data.OrigIPBytes[Round(.75*float64(dsLength-1))]
	dsBowleyNum := dsLow + dsHigh - 2*dsMid
	dsBowleyDen := dsHigh - dsLow

	//tsSkew should equal zero if the denominator equals zero
	//bowley skew is unreliable if Q2 = Q1 or Q2 = Q3
	if tsBowleyDen != 0 && tsMid != tsLow && tsMid != tsHigh {
		tsSkew = float64(tsBowleyNum) / float64(tsBowleyDen)
	}

	if dsBowleyDen != 0 && dsMid != dsLow && dsMid != dsHigh {
		dsSkew = float64(dsBowleyNum) / float64(dsBowleyDen)
	}

	// perfect beacons should have very low dispersion around the
	// median of their delta times
	// Median Absolute Deviation About the Median
	// is used to check dispersion
	devs := make([]int64, tsLength)
	for i := 0; i < tsLength; i++ {
		devs[i] = Abs(diff[i] - tsMid)
	}

	dsDevs := make([]int64, dsLength)
	for i := 0; i < dsLength; i++ {
		dsDevs[i] = Abs(data.OrigIPBytes[i] - dsMid)
	}

	sort.Sort(SortableInt64(devs))
	sort.Sort(SortableInt64(dsDevs))

	tsMadm := devs[Round(.5*float64(tsLength-1))]
	dsMadm := dsDevs[Round(.5*float64(dsLength-1))]

	//Store the range for human analysis
	tsIntervalRange := diff[tsLength-1] - diff[0]
	dsRange := data.OrigIPBytes[dsLength-1] - data.OrigIPBytes[0]

	//get a list of the intervals found in the data,
	//the number of times the interval was found,
	//and the most occurring interval
	intervals, intervalCounts, tsMode, tsModeCount := createCountMap(diff)
	dsSizes, dsCounts, dsMode, dsModeCount := createCountMap(data.OrigIPBytes)

	//more skewed distributions recieve a lower score
	//less skewed distributions recieve a higher score
	tsSkewScore := 1.0 - math.Abs(tsSkew) //smush tsSkew
	dsSkewScore := 1.0 - math.Abs(dsSkew) //smush dsSkew

	//lower dispersion is better, cutoff dispersion scores at 30 seconds
	tsMadmScore := 1.0 - float64(tsMadm)/30.0
	if tsMadmScore < 0 {
		tsMadmScore = 0
	}

	//lower dispersion is better, cutoff dispersion scores at 32 bytes
	dsMadmScore := 1.0 - float64(dsMadm)/32.0
	if dsMadmScore < 0 {
		dsMadmScore = 0
	}

	tsDurationScore := duration

	//smaller data sizes receive a higher score
	dsSmallnessScore := 1.0 - (float64(dsMode) / 65535.0)
	if dsSmallnessScore < 0 {
		dsSmallnessScore = 0
	}

	output := &beacon.AnalysisOutput{
		Src:              data.Src,
		Dst:              data.Dst,
		LocalSrc:         data.LocalSrc,
		LocalDst:         data.LocalDst,
		UconnID:          data.ID,
		TSISkew:          tsSkew,
		TSIDispersion:    tsMadm,
		TSDuration:       duration,
		TSIRange:         tsIntervalRange,
		TSIMode:          tsMode,
		TSIModeCount:     tsModeCount,
		TSIntervals:      intervals,
		TSIntervalCounts: intervalCounts,
		DSSkew:           dsSkew,
		DSDispersion:     dsMadm,
		DSRange:          dsRange,
		DSSizes:          dsSizes,
		DSSizeCounts:     dsCounts,
		DSMode:           dsMode,
		DSModeCount:      dsModeCount,
	}

	//score numerators
	tsSum := (tsSkewScore + tsMadmScore + tsDurationScore)
	dsSum := (dsSkewScore + dsMadmScore + dsSmallnessScore)

	//score averages
	output.TSScore = tsSum / 3.0
	output.DSScore = dsSum / 3.0
	output.Score = (tsSum + dsSum) / 6.0

	return output
}

// createCountMap returns a distinct data array, data count array, the mode,
// and the number of times the mode occured
func createCountMap(sortedIn []int64) ([]int64, []int64, int64, int64) {

	//Since the data is already sorted, we can call this without fear
	distinct, countsMap := countAndRemoveConsecutiveDuplicates(sortedIn)
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

//CountAndRemoveConsecutiveDuplicates removes consecutive
//duplicates in an array of integers and counts how many
//instances of each number exist in the array.
func countAndRemoveConsecutiveDuplicates(numberList []int64) ([]int64, map[int64]int64) {
	//Avoid some reallocations
	result := make([]int64, 0, len(numberList)/2)
	counts := make(map[int64]int64)

	last := numberList[0]
	result = append(result, last)
	counts[last]++

	for idx := 1; idx < len(numberList); idx++ {
		if last != numberList[idx] {
			result = append(result, numberList[idx])
		}
		last = numberList[idx]
		counts[last]++
	}
	return result, counts
}
