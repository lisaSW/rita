package beacon

import (
	"github.com/activecm/rita/config"
	"github.com/activecm/rita/database"
	"github.com/activecm/rita/datatypes/data"
	"github.com/activecm/rita/datatypes/structure"
	"github.com/globalsign/mgo/bson"
)

func collector_start(resDB *database.DB, resConf *config.Config, thresh int) []*BeaconAnalysisInput {

	var inputList []*BeaconAnalysisInput

	session := resDB.Session.Copy()
	defer session.Close()

	//grab all destinations related with this host
	var uconn structure.UniqueConnection
	// destIter := session.DB(c.db.GetSelectedDB()).
	localHostIter := session.DB(resDB.GetSelectedDB()).
		// C(c.conf.T.Structure.UniqueConnTable).
		C(resConf.T.Structure.UniqueConnTable).
		Find(bson.M{"local_src": true}).Iter()

	for localHostIter.Next(&uconn) {
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

	return inputList
}
