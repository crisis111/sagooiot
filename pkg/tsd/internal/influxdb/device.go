package influxdb

import (
	"context"
	"errors"
	"sagooiot/pkg/iotModel"
	"sagooiot/pkg/tsd/comm"
	"time"

	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

// InsertDeviceData 插入设备数据
func (m *Influxdb) InsertDeviceData(deviceKey string, data iotModel.ReportPropertyData, subKey ...string) (err error) {

	if m.db == nil {
		_, err = m.connect()
		if err != nil {
			return err
		}
	}
	if len(data) == 0 {
		err = errors.New("数据为空")
		return
	}

	table := comm.DeviceTableName(deviceKey)
	if len(subKey) > 0 {
		// 子设备
		table = comm.DeviceTableName(subKey[0])
	}

	var points = make([]*write.Point, 0)

	for key, val := range data {

		var tags = make(map[string]string)
		tags["device"] = deviceKey

		fields := map[string]any{
			comm.TdPropertyPrefix + key: val.Value,
		}

		ts := time.Unix(val.CreateTime, 0)

		point := write.NewPoint(table, tags, fields, ts)
		points = append(points, point)
	}

	return m.writePoints(points...)

}

// BatchInsertDeviceData 批量插入单设备的数据
func (m *Influxdb) BatchInsertDeviceData(deviceKey string, deviceDataList []iotModel.ReportPropertyData) (resultNum int, err error) {
	if m.db == nil {
		_, err = m.connect()
		if err != nil {
			return
		}
	}
	if len(deviceDataList) == 0 {
		err = errors.New("数据为空")
		return
	}

	var points []*write.Point

	// 生成points列表
	for _, deviceData := range deviceDataList {
		for key, val := range deviceData {
			point := write.NewPoint(
				comm.DeviceTableName(deviceKey),
				map[string]string{"device": deviceKey}, map[string]any{comm.TdPropertyPrefix + key: val.Value},
				time.Unix(val.CreateTime, 0),
			)
			points = append(points, point)
		}
	}

	err = m.writePoints(points...)
	if err != nil {
		return
	}

	resultNum = len(deviceDataList)
	return
}

// BatchInsertMultiDeviceData 批量插入多设备的数据
func (m *Influxdb) BatchInsertMultiDeviceData(multiDeviceDataList map[string][]iotModel.ReportPropertyData) (resultNum int, err error) {

	for deviceKey, deviceData := range multiDeviceDataList {
		length, err := m.BatchInsertDeviceData(deviceKey, deviceData)
		if err != nil {
			return 0, err
		} else {
			resultNum += length
		}
	}
	return
}

// 监听设备数据日志
func (m *Influxdb) WatchDeviceData(deviceKey string, callback func(data iotModel.ReportPropertyData)) (err error) {

	return
}

func (m *Influxdb) InsertLogData(log iotModel.DeviceLog) (resultNum int, err error) {
	if m.db == nil {
		_, err = m.connect()
		if err != nil {
			return
		}
	}

	table := comm.DeviceLogTable(log.Device)

	var points = make([]*write.Point, 0)

	pointType := write.NewPoint(table, map[string]string{"device": log.Device}, map[string]any{"type": log.Type}, time.Unix(log.Ts.Timestamp(), 0))
	pointContent := write.NewPoint(table, map[string]string{"device": log.Device}, map[string]any{"content": log.Content}, time.Unix(log.Ts.Timestamp(), 0))
	points = append(points, pointType, pointContent)

	err = m.writePoints(points...)

	resultNum = 2
	return
}
func (m *Influxdb) BatchInsertLogData(deviceLogList map[string][]iotModel.DeviceLog) (resultNum int, err error) {
	if m.db == nil {
		_, err = m.connect()
		if err != nil {
			return
		}
	}

	var points = make([]*write.Point, 0)
	for deviceKey, row := range deviceLogList {
		table := comm.DeviceLogTable(deviceKey)
		resultNum += len(row)
		for _, log := range row {
			pointType := write.NewPoint(table, map[string]string{"device": log.Device}, map[string]any{"type": log.Type}, time.Unix(log.Ts.Timestamp(), 0))
			pointContent := write.NewPoint(table, map[string]string{"device": log.Device}, map[string]any{"content": log.Content}, time.Unix(log.Ts.Timestamp(), 0))
			points = append(points, pointType, pointContent)
		}
	}

	err = m.writePoints(points...)
	if err != nil {
		return
	}

	return
}
func (m *Influxdb) writePoints(points ...*write.Point) (err error) {

	writeAPI := m.db.WriteAPIBlocking(m.Option.Org, m.Option.Database)

	// 允许批量写入
	writeAPI.EnableBatching()

	// 写入数据到缓存
	err = writeAPI.WritePoint(context.Background(), points...)
	if err != nil {
		return
	}

	// 确保所有数据点都被写入磁盘
	return writeAPI.Flush(context.Background())
}
