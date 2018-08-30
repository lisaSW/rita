package beacon

import (
	"fmt"
	"sync"

	"github.com/activecm/rita/config"
	"github.com/activecm/rita/database"
	"github.com/activecm/rita/datatypes/data"
	"github.com/activecm/rita/datatypes/structure"
	"github.com/globalsign/mgo/bson"
)

type (
	// collector collects Conn records into groups based on destination given
	// a source host
	collector struct {
		db                  *database.DB               // provides access to MongoDB
		conf                *config.Config             // contains details needed to access MongoDB
		connectionThreshold int                        // the minimum number of connections to be considered a beacon
		collectedCallback   func(*BeaconAnalysisInput) // called on each collected set of connections
		closedCallback      func()                     // called when .close() is called and no more calls to collectedCallback will be made
		collectChannel      chan string                // holds ip addresses
		collectWg           sync.WaitGroup             // wait for collection to finish
	}
)

// newCollector creates a new collector for creating BeaconAnalysisInput objects
// which group the given source, a detected destination, and all of their
// connection analysis details (timestamps, data sizes, etc.)
func newCollector(db *database.DB, conf *config.Config, connectionThreshold int,
	collectedCallback func(*BeaconAnalysisInput), closedCallback func()) *collector {
	//	fmt.Println("collector.go : new()")
	return &collector{
		db:                  db,
		conf:                conf,
		connectionThreshold: connectionThreshold,
		collectedCallback:   collectedCallback,
		closedCallback:      closedCallback,
		collectChannel:      make(chan string),
	}
}

// collect queues a host for collection
// Note: this function may block
func (c *collector) collect(srcHost string) {
	//	fmt.Println("collector.go : collect <- src")
	c.collectChannel <- srcHost

}

// close waits for the collection threads to finish
func (c *collector) close() {
	//	fmt.Println("collector.go : close()")
	close(c.collectChannel)
	c.collectWg.Wait()
	c.closedCallback()
	//	fmt.Println("collector.go : close2")
}

// start kicks off a new collection thread
func collector_start(resDB *database.DB, resConf *config.Config, thresh int, hostList []string) []*BeaconAnalysisInput {
	fmt.Println("collector.go : start()")
	session := resDB.Session.Copy()
	defer session.Close()

	var inputList []*BeaconAnalysisInput
	// go func() {

	for _, host := range hostList {

		//grab all destinations related with this host
		var uconn structure.UniqueConnection
		// destIter := session.DB(c.db.GetSelectedDB()).
		destIter := session.DB(resDB.GetSelectedDB()).
			// C(c.conf.T.Structure.UniqueConnTable).
			C(resConf.T.Structure.UniqueConnTable).
			Find(bson.M{"src": host}).Iter()

		for destIter.Next(&uconn) {
			//skip the connection pair if they are under the threshold
			if uconn.ConnectionCount < thresh {
				continue
			}

			//create our new input
			newInput := &BeaconAnalysisInput{
				UconnID: uconn.ID,
				src:     uconn.Src,
				dst:     uconn.Dst,
			}

			//Grab connection data
			var conn data.Conn
			connIter := session.DB(resDB.GetSelectedDB()).
				C(resConf.T.Structure.ConnTable).
				Find(bson.M{"id_orig_h": uconn.Src, "id_resp_h": uconn.Dst}).
				Iter()

			for connIter.Next(&conn) {
				//filter out unestablished connections
				//We expect at least SYN ACK SYN-ACK [FIN ACK FIN ACK/ RST]
				if conn.Proto == "tcp" && conn.OriginPackets+conn.ResponsePackets <= 3 {
					continue
				}

				newInput.Ts = append(newInput.Ts, conn.Ts)
				newInput.OrigIPBytes = append(newInput.OrigIPBytes, conn.OriginIPBytes)
			}

			//filtering may have reduced the amount of connections
			//check again if we should skip this unique connection
			if len(newInput.Ts) < thresh {
				continue
			}

			inputList = append(inputList, newInput)
		}

	}

	// }()

	return inputList
}
