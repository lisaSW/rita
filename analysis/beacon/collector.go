package beacon

import (
	"fmt"

	"github.com/activecm/rita/config"
	"github.com/activecm/rita/database"
	"github.com/activecm/rita/datatypes/structure"
	"github.com/globalsign/mgo/bson"
)

func collector_start(resDB *database.DB, resConf *config.Config, page int, pageSize int) (inputList []structure.UniqueConnection) {
	// fmt.Println("start collector:", page)
	session := resDB.Session.Copy()
	defer session.Close()

	skip := page * pageSize
	limit := pageSize
	// fmt.Println("skip: ", skip, "  limit: ", limit)

	beaconsQuery := []bson.D{
		{
			{"$match", bson.D{
				{"connection_count", bson.D{
					{"$gt", 45},
				}},
				{"connection_count", bson.D{
					{"$lt", 150000},
				}},
			}},
		},
		// {
		// 	{"$sort", bson.D{
		// 		{"connection_count", -1},
		// 	}},
		// },
		{
			{"$skip", skip},
		},
		{
			{"$limit", limit},
		},
	}

	/******** ERROR CHECKING  ******/
	// var m bson.M
	// err := session.DB(resDB.GetSelectedDB()).
	// 	C(resConf.T.Structure.UniqueConnTable).
	// 	Pipe(beaconsQuery).AllowDiskUse().
	// 	Explain(&m)
	// if err == nil {
	// 	// fmt.Printf("Explain: %#v\n", m)
	// } else {
	// 	fmt.Println("Error", err)
	// }
	/*******************************/

	/******** ERROR CHECKING  ******/
	// var temprez interface{}
	// countz := session.DB(resDB.GetSelectedDB()).
	// 	C(resConf.T.Structure.UniqueConnTable).
	// 	Pipe(beaconsQuery).
	//
	// fmt.Println("aggregation count", countz)

	/*******************************/

	err := session.DB(resDB.GetSelectedDB()).
		C(resConf.T.Structure.UniqueConnTable).
		Pipe(beaconsQuery).AllowDiskUse().
		All(&inputList)

	// Check for errors and parse results
	if err != nil {
		fmt.Printf("Error beacons ", err)
		return
	}

	return inputList
}

// db.getCollection('uconn').aggregate([
//     {$match: {local_src:true}},
//     {$skip: 0},
//     {$limit: 10},
//     {
//         $lookup:
//         {
//             from: "conn",
//             localField: "src",
//             foreignField: "id_orig_h",
//             as: "conn"
//         }
//     },
//     {$unwind: "$conn"},
//     //redact...
//
//     {$match: {$expr: {$eq: ["$conn.id_resp_h","$dst"]}}},
//     {$group: {_id: {"src":"$src","dst":"$dst"}, "ts": {$push: "$conn.ts"}, "orig_ip_bytes":{$push: "$conn.orig_ip_bytes"}}}
// ])

// db.getCollection('uconn').aggregate([
// {$match: {local_src:true}},
// {$skip: 0},
// {$limit: 10},
// {
//     "$lookup": {
//                     "from": "conn",
//                     "localField": "src",
//                     "foreignField": "id_orig_h",
//                     "as": "c2"
//                     }
// },
// {
//     "$unwind": "$c2"
// },
// {
//     "$project": {
//                     "user2Eq": {"$eq": ["$dst", "$c2.id_resp_h"]},
//                     "src": 1, "dst": 1,
//                     "percent1": "$c2.id_orig_h", "percent2": "$c2.id_resp_h"
//                     }
// },
// {
//     "$match": {
//         "user2Eq": {"$eq": true},
//               }
// },
// {"$project":
//     {
//       "user2Eq": 0
//     }
// }
// ])

// db.getCollection('uconn').aggregate([
//        {
//           $lookup: {
//              from: "conn",
//              let: {
//                 matchSrc: "$src",
//                 matchDst: "$dst"
//              },
//              pipeline: [
//                 {
//                    $match: {
//                       $expr: {
//                          $and: [
//                             {
//                                $eq: [
//                                   "$id_orig_h",
//                                   "$$matchSrc"
//                                ]
//                             },
//                             {
//                                $eq: [
//                                   "$id_resp_h",
//                                   "$$matchDst"
//                                ]
//                             }
//                          ]
//                       }
//                    }
//                 }
// //                 { $project: { stock_item: 0, _id: 0 } }
//
//              ],
//              as: "result"
//           }
//        }
// //        {
// //           $replaceRoot: {
// //              newRoot: {
// //                 $mergeObjects:[
// //                    {
// //                       $arrayElemAt: [
// //                          "$result",
// //                          0
// //                       ]
// //                    },
// //                    {
// //                       percent1: "$$ROOT.src"
// //                    }
// //                 ]
// //              }
// //           }
// //        }
//
//     ]
// )

// db.getCollection('uconn').aggregate([
//     {$match: {local_src:true,connection_count: {$gt:5}}},
//     {$skip: 0},
//     {$limit: 10},
//        {
//           $lookup: {
//              from: "conn",
//              let: {
//                 matchSrc: "$src",
//                 matchDst: "$dst"
//              },
//              pipeline: [
//                 {
//                    $match: {
//                       $expr: {
//                          $and: [
//                             {
//                                $eq: [
//                                   "$id_orig_h",
//                                   "$$matchSrc"
//                                ]
//                             },
//                             {
//                                $eq: [
//                                   "$id_resp_h",
//                                   "$$matchDst"
//                                ]
//                             }
//                          ]
//                       }
//                    }
//                 }
// //                 { $project: { stock_item: 0, _id: 0 } }
//
//              ],
//              as: "result"
//           }
//        },
//        { "$project": {
//         "src": 1,
//         "dst": 1,
//         "result": {
//             "$map": {
//                 "input": "$result",
//                 "as": "r",
//                 "in": {
//                     "ts": "$$r.ts",
//                     "orig_ip_bytes": "$$r.orig_ip_bytes"
//                     }
//                 }
//             }
//         }}
//
//     ]
// )

//
// db.getCollection('uconn').aggregate([
//     {$match: {local_src:true,connection_count: {$gt:5}}},
//     {$skip: 0},
//     {$limit: 10},
//        {
//           $lookup: {
//              from: "conn",
//              let: {
//                 matchSrc: "$src",
//                 matchDst: "$dst"
//              },
//              pipeline: [
//                 {
//                    $match: {
//                       $expr: {
//                          $and: [
//                             {
//                                $eq: [
//                                   "$id_orig_h",
//                                   "$$matchSrc"
//                                ]
//                             },
//                             {
//                                $eq: [
//                                   "$id_resp_h",
//                                   "$$matchDst"
//                                ]
//                             }
//                          ]
//                       }
//                    }
//                 }
//              ],
//              as: "result"
//           }
//        },
//        {"$unwind": "$result"},
//        { "$group": {
//            "_id": "$_id",
//             "src": {"$first": "$src"},
//             "dst": {"$first": "$dst"},
// 	"ts": {"$push": "$result.ts"},
// 	"orig_ip_bytes": {"$push": "$result.orig_ip_bytes"},
//         }}
//     ]
// )
