package beacon

import (
	"github.com/activecm/rita/config"
	"github.com/activecm/rita/database"
	dataBeacon "github.com/activecm/rita/datatypes/beacon"
)

// start kicks off a new write thread
func writer_start(output []*dataBeacon.BeaconAnalysisOutput, resDB *database.DB, resConf *config.Config) {

	// go func() {

	ssn := resDB.Session.Copy()
	defer ssn.Close()

	// counter := 0
	for _, data := range output {
		ssn.DB(resDB.GetSelectedDB()).C(resConf.T.Beacon.BeaconTable).Insert(data)
		// counter++
	}

	// }()
}
