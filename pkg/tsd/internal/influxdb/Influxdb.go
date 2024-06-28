package influxdb

import (
	"context"
	"database/sql"
	"fmt"
	"sagooiot/pkg/iotModel"
	"sagooiot/pkg/tsd/comm"
	"time"

	"github.com/gogf/gf/v2/database/gdb"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

type Influxdb struct {
	Option comm.Option
	db     influxdb2.Client
}

type ReportReq struct {
	Timestamp  int64 //
	Metric     string
	Dimensions map[string]string
	Value      float64
}

func (m *Influxdb) connect() (_ influxdb2.Client, err error) {
	// 创建写入选项并设置 50 个点写入一次数据
	options := influxdb2.DefaultOptions()
	options.SetPrecision(time.Millisecond)
	options.SetBatchSize(50)

	client := influxdb2.NewClientWithOptions(m.Option.Link, m.Option.Token, options)
	m.db = client
	return
}
func (m *Influxdb) client() {

}

func (m *Influxdb) Close() {
	m.db.Close()
}

func (m *Influxdb) Query(sql string) (rows *sql.Rows, err error) {
	return nil, nil
}

func (m *Influxdb) Count(deviceKey string, field string) (int, error) {

	if m.db == nil {
		_, err := m.connect()
		if err != nil {
			return 0, err
		}
	}

	fluxSql := `data = from(bucket: "%s")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "%s")
	|> filter(fn: (r) => r._field == "%s")
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    data |> count(column: "%s")`

	table := comm.DeviceTableName(deviceKey)
	fluxSql = fmt.Sprintf(fluxSql, m.Option.Database, table, comm.TdPropertyPrefix+field, comm.TdPropertyPrefix+field)

	fmt.Println(fluxSql)

	query := m.db.QueryAPI(m.Option.Org)
	results, err := query.Query(context.Background(), fluxSql)
	if err != nil {
		return 0, err
	}
	fmt.Println(results)
	results.Record()
	return 50, nil
}

// GetAllDatabaseName 获取所有数据库名称
func (m *Influxdb) GetAllDatabaseName(ctx context.Context) (names []string, err error) {

	return
}

// GetTableListByDatabase 获取指定的数据库下所有的表
func (m *Influxdb) GetTableListByDatabase(ctx context.Context, dbName string) (tableList []iotModel.TsdTables, err error) {

	return
}

// GetTableInfo 获取指定数据表结构信息
func (m *Influxdb) GetTableInfo(ctx context.Context, tableName string) (table []*iotModel.TsdTableInfo, err error) {
	return
}

// GetTableData 获取指定数据表数据信息
func (m *Influxdb) GetTableData(ctx context.Context, tableName string) (table *iotModel.TsdTableDataInfo, err error) {

	return
}

// GetTableDataOne 获取超级表的单条数据
func (m *Influxdb) GetTableDataOne(ctx context.Context, sqlStr string, args ...any) (rs gdb.Record, err error) {

	return
}

// GetTableDataAll 获取超级表的多条数据
func (m *Influxdb) GetTableDataAll(ctx context.Context, sqlStr string, args ...any) (rs gdb.Result, err error) {

	return
}
