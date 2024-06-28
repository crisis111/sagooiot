package tdengine

import (
	"fmt"
	"sagooiot/internal/consts"
	"sagooiot/pkg/tsd/comm"
	"sort"
	"strings"
	"time"

	"github.com/gogf/gf/v2/container/gvar"
	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/frame/g"
)

// Query 查询
func (m *TdEngine) QueryPage(fields []string, deviceKey string, stime, etime int, order string, page, limit int) (rs gdb.Result, err error) {
	if m.db == nil {
		_, err = m.connect()
		if err != nil {
			return
		}
	}

	var fieldMore []string

	for _, k := range fields {
		k = comm.TsdColumnName(k)
		fieldMore = append(fieldMore, k)
		// 属性上报时间
		fieldMore = append(fieldMore, k+"_time")
	}
	sort.Strings(fieldMore)

	table := comm.DeviceTableName(deviceKey)
	sql := fmt.Sprintf("select %s from %s ", strings.Join(fields, ","), table)

	if stime > 0 && etime > 0 {
		sql += fmt.Sprintf(" where ts >= %d and ts <= %d ", stime, etime)
	}

	if order != "" {
		sql += fmt.Sprintf(" order by ts %s ", order)
	}

	if page > 0 && limit > 0 {
		sql += fmt.Sprintf(" limit %d,%d ", (page-1)*limit, limit)
	}

	rows, err := m.db.Query(sql)
	if err != nil {
		return
	}
	columns, _ := rows.Columns()
	for rows.Next() {
		values := make([]any, len(columns))
		for i := range values {
			values[i] = new(any)
		}

		err = rows.Scan(values...)
		if err != nil {
			return
		}

		m := make(gdb.Record, len(columns))
		for i, c := range columns {
			// 去除前缀
			if c[:2] == consts.TdPropertyPrefix {
				c = c[2:]
			}
			m[c] = toTime(gvar.New(values[i]))
		}
		rs = append(rs, m)
	}
	return
}

func toTime(v *g.Var) (rs *g.Var) {
	if t, err := time.Parse("2006-01-02 15:04:05 +0000 UTC", v.String()); err == nil {
		rs = gvar.New(t.Local().Format("2006-01-02 15:04:05"))
		return
	}

	rs = v
	return
}
