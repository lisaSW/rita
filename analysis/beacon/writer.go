package beacon

import (
	"fmt"

	"github.com/activecm/rita/config"
	"github.com/activecm/rita/database"
	dataBeacon "github.com/activecm/rita/datatypes/beacon"
	mgo "github.com/globalsign/mgo"
)

// start kicks off a new write thread
func writer_start(output []*dataBeacon.BeaconAnalysisOutput, resDB *database.DB, resConf *config.Config) {
	// fmt.Println("start writer")
	// for _, data := range output {
	// 	ssn.DB(resDB.GetSelectedDB()).C(resConf.T.Beacon.BeaconTable).Insert(data)
	//
	// }

	// buffer length controls amount of ram used while exporting
	bufferLen := resConf.S.Bro.ImportBuffer
	// bufferLen := 10

	//Create a buffer to hold a portion of the results
	buffer := make([]interface{}, 0, bufferLen)

	//while we can still iterate through the data add to the buffer
	// var datum interface{}
	// for iter.Next(&datum) {
	for _, data := range output {
		//if the buffer is full, send to the remote database and clear buffer
		if len(buffer) == bufferLen {

			err := bulk_write(buffer, resDB, resConf)
			if err != nil && err.Error() != "invalid BulkError instance: no errors" {
				fmt.Println("write error 1", err)
			}

			buffer = buffer[:0]
		}

		buffer = append(buffer, data)
	}

	//send any data left in the buffer to the remote database
	//
	err := bulk_write(buffer, resDB, resConf)
	if err != nil && err.Error() != "invalid BulkError instance: no errors" {
		fmt.Println(buffer)
		fmt.Println("write error 2", err)
	}

}

func bulk_write(buffer []interface{}, resDB *database.DB, resConf *config.Config) error {
	ssn := resDB.Session.Copy()
	defer ssn.Close()
	// bulk := remoteSession.DB(remoteDB).C(name).Bulk()
	// set up for bulk write to database
	bulk := ssn.DB(resDB.GetSelectedDB()).C(resConf.T.Beacon.BeaconTable).Bulk()
	// writes can be sent out of order
	bulk.Unordered()
	// inserts everything in the buffer into the bulk write object as a list
	// of single interfaces
	bulk.Insert(buffer...)

	// runs all queued operations
	_, err := bulk.Run()

	return err

}

// start kicks off a new write thread
func writer_start_non_bulk(output []*dataBeacon.BeaconAnalysisOutput, resDB *database.DB, resConf *config.Config) int {
	// fmt.Println("writing:", len(output))
	ssn := resDB.Session.Copy()
	defer ssn.Close()

	// var noErr []*dataBeacon.BeaconAnalysisOutput
	counter := 0
	for _, data := range output {
		err := ssn.DB(resDB.GetSelectedDB()).C(resConf.T.Beacon.BeaconTable).Insert(data)

		if err != nil {
			if mgo.IsDup(err) {
				// Is a duplicate key, but we don't know which one
				fmt.Println("write error duplicate flag", err)
			} else {
				fmt.Println("write error other", err)
			}

			fmt.Println("data", data)
			// Is another error
		} else {
			counter++
			// noErr = append(noErr, data)
		}

	}

	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("===                  writing success                       ===")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println("wrote:", counter)
	// fmt.Println("==============================================================")
	// fmt.Println("==============================================================")
	// fmt.Println(noErr)
	return counter
}
