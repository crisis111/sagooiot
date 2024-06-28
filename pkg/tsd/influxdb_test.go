package tsd

import (
	"fmt"
	"sagooiot/pkg/iotModel"
	"sagooiot/pkg/tsd/comm"
	"testing"

	"github.com/gogf/gf/v2/os/gtime"
)

func TestDataWrite(t *testing.T) {
	db := influxdbDB(t)
	data := GenData()
	// db.BatchInsertDeviceData("test", data)
	// db.InsertDeviceData("test", data[0])
	db.BatchInsertMultiDeviceData(map[string][]iotModel.ReportPropertyData{
		"test": data,
	})
}

func TestLogWrite(t *testing.T) {
	db := influxdbDB(t)
	data := GenLogData()

	// db.InsertLogData(data[0])
	db.BatchInsertLogData(map[string][]iotModel.DeviceLog{"kongtiao": data})

}

func TestQueryPage(t *testing.T) {
	db := influxdbDB(t)
	stime := 1718964549
	etime := 1718964549 + 10
	rs, err := db.QueryPage([]string{"press"}, "test", stime, etime, "desc", 1, 10)
	if err != nil {
		fmt.Println(err)
		t.Fatal(err)
	}
	fmt.Println(rs)
}

func TestInfluxDBCount(t *testing.T) {
	db := influxdbDB(t)
	db.Count("test", "press")
}

func influxdbDB(t *testing.T) Database {
	// 定义数据库连接选项
	option2 := comm.Option{
		Database: "mydb", // bucket 等同与tdengine的database
		Link:     "http://localhost:8086",
		Org:      "jxg", //
		Token:    "Ihn2Y1EDNjDZ7mXwlxQ_2i3Q8PzqXPg-WWA2ejScmQBA-9ZtyisvkiX3gFtgipRHOxnnksYxVBHUG2o_tNY4fw==",
	}
	// 使用工厂函数创建 Influxdb 数据库实例
	idb := DatabaseFactory(comm.DBInfluxdb, option2)
	if idb == nil {
		t.Fail()
	}
	return idb
}

func GenLogData() (logs []iotModel.DeviceLog) {
	var log iotModel.DeviceLog
	log.Content = "test"
	log.Type = "info"
	log.Ts = gtime.Now()
	log.Device = "kongtiao"

	var log2 iotModel.DeviceLog
	log2.Content = "test2"
	log2.Type = "warning"
	log2.Ts = gtime.Now()
	log2.Device = "kongtiao"

	logs = append(logs, log, log2)
	return
}
func GenData() (rs []iotModel.ReportPropertyData) {
	now := int64(1718964549)

	for i := 1; i < 30; i++ {
		temp := iotModel.ReportPropertyNode{}
		temp.Value = float64(3 + i)
		temp.CreateTime = now + int64(i)

		press := iotModel.ReportPropertyNode{}
		press.Value = float64(18 + i)
		press.CreateTime = now + int64(i)

		data := iotModel.ReportPropertyData{"temp": temp, "press": press}
		rs = append(rs, data)
	}
	return
}
