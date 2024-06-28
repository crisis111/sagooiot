package influxdb

import (
	"context"
	"errors"
	"fmt"
	"sagooiot/internal/consts"
	"sagooiot/pkg/tsd/comm"
	"strings"
	"time"

	"github.com/gogf/gf/v2/container/gvar"
	"github.com/gogf/gf/v2/database/gdb"
)

func (m *Influxdb) QueryPage(fields []string, deviceKey string, stime, etime int, order string, page, limit int) (rs gdb.Result, err error) {
	if m.db == nil {
		_, err = m.connect()
		if err != nil {
			return
		}
	}

	if len(fields) == 0 {
		return nil, errors.New("field is empty")
	}
	st := time.Unix(int64(stime), 0).UTC().Format(time.RFC3339)
	et := time.Unix(int64(etime), 0).UTC().Format(time.RFC3339)
	tmp := []string{}
	for i := 0; i < len(fields); i++ {
		tmp = append(tmp, fmt.Sprintf(`r._field == "%s"`, comm.TdPropertyPrefix+fields[i]))
	}

	table := comm.DeviceTableName(deviceKey)

	sql := fmt.Sprintf(`from(bucket: "%s")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "%s")
	|> filter(fn: (r) => %s)
	|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	`, m.Option.Database, st, et, table, strings.Join(tmp, " or "))

	var b bool
	if strings.ToLower(order) == "desc" {
		b = true
	} else {
		b = false
	}

	if page == 0 {
		page = 1
	}
	if limit == 0 {
		limit = 10
	}

	if limit > 0 && page > 0 {
		sql += fmt.Sprintf(`
	|> limit(n:%d,offset:%d)`, limit, (page-1)*limit)
	}

	if order != "" {
		sql += fmt.Sprintf(`
	|> sort(columns:["_time"],desc:%t)`, b)
	}

	query := m.db.QueryAPI(m.Option.Org)
	results, err := query.Query(context.Background(), sql)
	if err != nil {
		return
	}

	for results.Next() {
		fmt.Println(results.Record())
		m := make(gdb.Record)
		vals := results.Record().Values() // 转换为 map[_measurement:device_prop, , _time:2024-05-27 03:00:00 +0000 UTC, p_press:149, p_temp: 23.9 ,result:_result ,stop:...,start:... ]

		ts, ok := vals["_time"].(time.Time)
		tsStr := ""
		if ok {
			ts = ts.UTC()
			shanghaiLocation, err := time.LoadLocation("Asia/Shanghai")
			if err != nil {
				return nil, err
			}
			tsStr = ts.In(shanghaiLocation).Format(time.DateTime)
		}

		for i := 0; i < len(fields); i++ {
			if value, ok := vals[consts.TdPropertyPrefix+fields[i]]; ok {
				m[fields[i]] = gvar.New(value)         // 存入字段值
				m[fields[i]+"_time"] = gvar.New(tsStr) // 字段对应的时间戳
				rs = append(rs, m)
			}
		}

	}
	return
}
