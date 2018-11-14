package beacon

import (
	"math"
	"time"

	mgo "github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"

	"github.com/activecm/rita/database"
	"github.com/activecm/rita/datatypes/data"
	"github.com/activecm/rita/resources"

	log "github.com/sirupsen/logrus"
)

type (
	//beaconAnalysisInput binds a src, dst pair with their analysis data
	BeaconAnalysisInput struct {
		Src         string        `bson:"src"`           // Source IP
		Dst         string        `bson:"dst"`           // Destination IP
		LocalSrc    bool          `bson:"local_src"`     // local src bool
		LocalDst    bool          `bson:"local_dst"`     // local dst bool
		UconnID     bson.ObjectId `bson:"_id"`           // Unique Connection ID
		Ts          []int64       `bson:"ts"`            // Connection timestamps for this src, dst pair
		OrigIPBytes []int64       `bson:"orig_ip_bytes"` // Src to dst connection sizes for each connection
	}
)

// BuildBeaconCollection pulls data from the unique connections collection
// and performs statistical analysis searching for command and control
// beacons.
func BuildBeaconCollection(res *resources.Resources) {

	// create the actual collection
	collectionName := res.Config.T.Beacon.BeaconTable
	collectionKeys := []mgo.Index{
		{Key: []string{"uconn_id"}, Unique: true},
		{Key: []string{"ts_score"}},
	}
	err := res.DB.CreateCollection(collectionName, collectionKeys)
	if err != nil {
		res.Log.Error("Failed: ", collectionName, err.Error())
		return
	}

	// If the threshold is incorrectly specified, fix it up.
	// We require at least four delta times to analyze
	// (Q1, Q2, Q3, Q4). So we need at least 5 connections
	thresh := res.Config.S.Beacon.DefaultConnectionThresh
	if thresh < 5 {
		thresh = 5
	}

	//Find the observation period (returns the min and max timestamps for
	// for all connections)
	minTime, maxTime := findAnalysisPeriod(
		res.DB,
		res.Config.T.Structure.ConnTable,
		res.Log,
	)

	// Get number of local hosts

	localHostCount, _ := res.DB.Session.DB(res.DB.GetSelectedDB()).
		C(res.Config.T.Structure.UniqueConnTable).
		Find(bson.M{"local_src": true, "connection_count": bson.M{"$gt": 47, "$lt": 150000}}).
		Count()

	limit := 300
	pages := int(math.Ceil(float64(localHostCount) / float64(limit)))

	for i := 0; i < pages; i++ {

		t := collector_start(res.DB, res.Config, i, limit)

		outputChunk := analyzer_start(t, minTime, maxTime, thresh)

		writer_start(outputChunk, res.DB, res.Config)

	}

	// fmt.Println("collected:", collectedCounter)
	// fmt.Println("analyzed:", analyzedCounter)
	// fmt.Println("wrote:", writeCounter)

	// tempy := collector_start(res.DB, res.Config, thresh, 0, limit)
	// fmt.Println("collected:", len(tempy))

	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("===              input after aggregation                   ===")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// for _, p := range tempy {
	// 	fmt.Println(p.UconnID)
	// }
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	//
	// outputChunk := analyzer_start(tempy, minTime, maxTime, thresh)
	// fmt.Println("analyzed:", len(outputChunk))

	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("===                     output chunk                       ===")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")

	// for _, p := range outputChunk {
	// 	fmt.Println(p.UconnID)
	// }
	//
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("===                   writing erorrs                       ===")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	//
	// // writer_start(outputChunk, res.DB, res.Config)
	// writer_start_non_bulk(outputChunk, res.DB, res.Config)

}

func findAnalysisPeriod(db *database.DB, connCollection string,
	logger *log.Logger) (int64, int64) {

	session := db.Session.Copy()
	defer session.Close()

	//Find first time
	logger.Debug("Looking for first connection timestamp")
	start := time.Now()

	//In practice having mongo do two lookups is
	//faster than pulling the whole collection
	//This could be optimized with an aggregation
	var conn data.Conn
	session.DB(db.GetSelectedDB()).
		C(connCollection).
		Find(nil).Limit(1).Sort("ts").Iter().Next(&conn)

	minTime := conn.Ts

	logger.Debug("Looking for last connection timestamp")
	session.DB(db.GetSelectedDB()).
		C(connCollection).
		Find(nil).Limit(1).Sort("-ts").Iter().Next(&conn)

	maxTime := conn.Ts

	logger.WithFields(log.Fields{
		"time_elapsed": time.Since(start),
		"first":        minTime,
		"last":         maxTime,
	}).Debug("First and last timestamps found")
	return minTime, maxTime
}

//GetBeaconResultsView finds beacons greater than a given cutoffScore
//and links the data from the unique connections table back in to the results
func GetBeaconResultsView(res *resources.Resources, ssn *mgo.Session, cutoffScore float64) *mgo.Iter {
	pipeline := getViewPipeline(res, cutoffScore)
	return res.DB.AggregateCollection(res.Config.T.Beacon.BeaconTable, ssn, pipeline)
}

// GetViewPipeline creates an aggregation for user views since the beacon collection
// stores uconn uid's rather than src, dest pairs. cuttoff is the lowest overall
// score to report on. Setting cuttoff to 0 retrieves all the records from the
// beaconing collection. Setting cuttoff to 1 will prevent the aggregation from
// returning any records.
func getViewPipeline(res *resources.Resources, cuttoff float64) []bson.D {
	return []bson.D{
		{
			{"$match", bson.D{
				{"score", bson.D{
					{"$gt", cuttoff},
				}},
			}},
		},
		{
			{"$lookup", bson.D{
				{"from", res.Config.T.Structure.UniqueConnTable},
				{"localField", "uconn_id"},
				{"foreignField", "_id"},
				{"as", "uconn"},
			}},
		},
		{
			{"$unwind", "$uconn"},
		},
		{
			{"$sort", bson.D{
				{"score", -1},
			}},
		},
		{
			{"$project", bson.D{
				{"score", 1},
				{"src", "$uconn.src"},
				{"dst", "$uconn.dst"},
				{"local_src", "$uconn.local_src"},
				{"local_dst", "$uconn.local_dst"},
				{"connection_count", "$uconn.connection_count"},
				{"avg_bytes", "$uconn.avg_bytes"},
				{"ts_iRange", 1},
				{"ts_iMode", 1},
				{"ts_iMode_count", 1},
				{"ts_iSkew", 1},
				{"ts_duration", 1},
				{"ts_iDispersion", 1},
				{"ds_dispersion", 1},
				{"ds_range", 1},
				{"ds_mode", 1},
				{"ds_mode_count", 1},
				{"ds_skew", 1},
			}},
		},
	}
}
