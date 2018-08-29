// +build integration

package beacon

import (
	"testing"

	"github.com/activecm/rita/datatypes/structure"
	"github.com/activecm/rita/parser/parsetypes"
	"github.com/activecm/rita/resources"
	"github.com/stretchr/testify/require"
)

func TestCollector(t *testing.T) {

	res := resources.InitIntegrationTestingResources(t)
	setUpCollectorTest(t, res)
	defer tearDownCollectorConnRecords(t, res)

	var collectedChannel chan *BeaconAnalysisInput

	collect := func(srcHost string) {
		collectedChannel = make(chan *BeaconAnalysisInput, 2)
		collector := newCollector(
			res.DB, res.Config, res.Config.S.Beacon.DefaultConnectionThresh,
			func(collected *BeaconAnalysisInput) {
				collectedChannel <- collected
			},
			func() {
				close(collectedChannel)
			},
		)
		collector.start()
		collector.collect(srcHost)
		collector.close()
	}

	collect(collectorTestDataHostList[0])

	t.Run(collectorTestDataList[0].description, func(t *testing.T) {
		collectedHost, ok := <-collectedChannel
		require.True(t, ok)
		collectionSuccessful(t, &collectorTestDataList[0], collectedHost)
	})

	t.Run(collectorTestDataList[1].description, func(t *testing.T) {
		collectedHost, ok := <-collectedChannel
		require.True(t, ok)
		collectionSuccessful(t, &collectorTestDataList[1], collectedHost)
	})

	collect(collectorTestDataHostList[1])

	t.Run(collectorTestDataList[2].description, func(t *testing.T) {
		collectedHost, ok := <-collectedChannel
		require.True(t, ok)
		collectionSuccessful(t, &collectorTestDataList[2], collectedHost)
	})

	t.Run(collectorTestDataList[3].description, func(t *testing.T) {
		_, ok := <-collectedChannel
		require.False(t, ok)
	})

	collect(collectorTestDataHostList[2])

	t.Run(collectorTestDataList[5].description, func(t *testing.T) {
		collectedHost, ok := <-collectedChannel
		require.True(t, ok)
		collectionSuccessful(t, &collectorTestDataList[5], collectedHost)
	})

	t.Run(collectorTestDataList[4].description, func(t *testing.T) {
		_, ok := <-collectedChannel
		require.False(t, ok)
	})

}

func collectionSuccessful(t *testing.T, collectorTestData *collectorTestData, collectedData *BeaconAnalysisInput) {
	require.Equal(t, collectorTestData.src, collectedData.src)
	require.Equal(t, collectorTestData.dst, collectedData.dst)
	for i := range collectorTestData.ts {
		require.Equal(t, collectorTestData.ts[i], collectedData.Ts[i])
		require.Equal(t, collectorTestData.ds[i], collectedData.OrigIPBytes[i])
	}
}

func setUpCollectorTest(t *testing.T, res *resources.Resources) {
	session := res.DB.Session.Copy()
	defer session.Close()
	for i, record := range collectorTestDataList {
		if len(record.ts) != len(record.ds) {
			t.Fatalf("number of timestamps and data measures are not equal for test record %d", i)
		}
		uconn := structure.UniqueConnection{
			Src:             record.src,
			Dst:             record.dst,
			ConnectionCount: len(record.ts),
		}
		session.DB(res.DB.GetSelectedDB()).C(res.Config.T.Structure.UniqueConnTable).Insert(&uconn)
		for i, timestamp := range record.ts {
			dataSize := record.ds[i]
			connRecord := parsetypes.Conn{
				TimeStamp:   timestamp,
				OrigIPBytes: dataSize,
				Source:      record.src,
				Destination: record.dst,
				Proto:       record.proto,
				OrigPkts:    record.origPackets,
				RespPkts:    record.respPackets,
			}
			session.DB(res.DB.GetSelectedDB()).C(res.Config.T.Structure.ConnTable).Insert(&connRecord)
		}
	}
}

func tearDownCollectorConnRecords(t *testing.T, res *resources.Resources) {
	session := res.DB.Session.Copy()
	defer session.Close()
	session.DB(res.DB.GetSelectedDB()).C(res.Config.T.Structure.ConnTable).DropCollection()
	session.DB(res.DB.GetSelectedDB()).C(res.Config.T.Structure.UniqueConnTable).DropCollection()
}
