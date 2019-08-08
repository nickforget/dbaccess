package dbaccess

import (
	"database/sql"
	"github.com/golang/protobuf/proto"
	"github.com/nickforget/dbaccess/basetype"
	"reflect"
	"strings"
)

func ProtoToMap(pb proto.Message) map[string]interface{} {
	var strName string
	var strFieldType string

	elem := reflect.ValueOf(pb).Elem()
	elemLen := reflect.ValueOf(pb).Elem().NumField()

	revMap := make(map[string]interface{})

	for i := 0; i < elemLen; i++ {
		// 字段为空不做处理
		if elem.Field(i).IsNil() {
			continue
		}

		strName = elem.Type().Field(i).Name
		strFieldType = elem.Field(i).Type().String()

		switch strFieldType {
		case "*basetype.Float":
			revMap[strName] = (**(elem.Field(i).Addr().Interface().(**basetype.Float))).Data
		case "*basetype.Double":
			revMap[strName] = (**(elem.Field(i).Addr().Interface().(**basetype.Double))).Data
		case "*basetype.Int32":
			revMap[strName] = (**(elem.Field(i).Addr().Interface().(**basetype.Int32))).Data
		case "*basetype.UInt32":
			revMap[strName] = (**(elem.Field(i).Addr().Interface().(**basetype.UInt32))).Data
		case "*basetype.Int64":
			revMap[strName] = (**(elem.Field(i).Addr().Interface().(**basetype.Int64))).Data
		case "*basetype.UInt64":
			revMap[strName] = (**(elem.Field(i).Addr().Interface().(**basetype.UInt64))).Data
		case "*basetype.String":
			revMap[strName] = (**(elem.Field(i).Addr().Interface().(**basetype.String))).Data
		case "*basetype.Bool":
			revMap[strName] = (**(elem.Field(i).Addr().Interface().(**basetype.Bool))).Data
		case "*basetype.Date":
			revMap[strName] = (**(elem.Field(i).Addr().Interface().(**basetype.Date))).Data
		}
	}

	return revMap
}

func DataToProto(data []interface{}, inMap map[string]int, pb proto.Message) {
	var strName string
	var strFieldType string
	var value interface{}

	elem := reflect.ValueOf(pb).Elem()
	elemLen := reflect.ValueOf(pb).Elem().NumField()

	for i := 0; i < elemLen; i++ {
		strName = elem.Type().Field(i).Name
		value = elem.Field(i).Addr().Interface()
		strFieldType = elem.Field(i).Type().String()

		if index, ok := inMap[strName]; ok {
			switch strFieldType {
			case "*basetype.Float":
				valueSrc := (data[index]).(*sql.NullFloat64)
				if valueSrc.Valid {
					*(value.(**basetype.Float)) =  &basetype.Float{Data:(float32)(valueSrc.Float64)}
				} else {
					value = nil
				}

			case "*basetype.Double":
				valueSrc := (data[index]).(*sql.NullFloat64)
				if valueSrc.Valid {
					*(value.(**basetype.Double)) =  &basetype.Double{Data:(float64)(valueSrc.Float64)}
				} else {
					value = nil
				}

			case "*basetype.Int32":
				valueSrc := (data[index]).(*sql.NullInt64)
				if valueSrc.Valid {
					*(value.(**basetype.Int32)) =  &basetype.Int32{Data:(int32)(valueSrc.Int64)}
				} else {
					value = nil
				}

			case "*basetype.UInt32":
				valueSrc := (data[index]).(*sql.NullInt64)
				if valueSrc.Valid {
					*(value.(**basetype.UInt32)) =  &basetype.UInt32{Data:(uint32)(valueSrc.Int64)}
				} else {
					value = nil
				}

			case "*basetype.Int64":
				valueSrc := (data[index]).(*sql.NullInt64)
				if valueSrc.Valid {
					*(value.(**basetype.Int64)) =  &basetype.Int64{Data:(int64)(valueSrc.Int64)}
				} else {
					value = nil
				}

			case "*basetype.UInt64":
				valueSrc := (data[index]).(*sql.NullInt64)
				if valueSrc.Valid {
					*(value.(**basetype.UInt64)) =  &basetype.UInt64{Data:(uint64)(valueSrc.Int64)}
				} else {
					value = nil
				}

			case "*basetype.String":
				valueSrc := (data[index]).(*sql.NullString)
				if valueSrc.Valid {
					*(value.(**basetype.String)) =  &basetype.String{Data:(string)(valueSrc.String)}
				} else {
					value = nil
				}

			case "*basetype.Bool":
				valueSrc := (data[index]).(*sql.NullBool)
				if valueSrc.Valid {
					*(value.(**basetype.Bool)) =  &basetype.Bool{Data:(bool)(valueSrc.Bool)}
				} else {
					value = nil
				}

			case "*basetype.Date":
				valueSrc := (data[index]).(*sql.NullString)
				if valueSrc.Valid {
					*(value.(**basetype.String)) =  &basetype.String{Data:(string)(valueSrc.String)}
				} else {
					value = nil
				}
			}
		}
	}
}

// 获取需要查询的字段列表和类型
func GetQueryField(pb proto.Message, queryField []string) map[string]string {
	var strName string
	var strFieldType string

	fieldLen := len(queryField)
	revMap := make(map[string]string)
	elem := reflect.ValueOf(pb).Elem()
	elemLen := reflect.ValueOf(pb).Elem().NumField()

	for i := 0; i < elemLen; i++ {
		strName = elem.Type().Field(i).Name
		strFieldType = elem.Field(i).Type().String()

		// 不是XXX_开头的字段
		if strings.HasPrefix(strName, "XXX_") {
			continue
		}

		// 查看是否是需要查出的字段
		if 0 != fieldLen {
			for _, v := range queryField {
				if v == strName {
					revMap[strName] = strFieldType
					break
				}
			}
		} else {
			revMap[strName] = strFieldType
		}
	}

	return revMap
}

// 返回查询的SQL,获取结果的参数和字段列表
func GetQueryInfo(tableName string, pb proto.Message, queryField []string) (string, []interface{}, map[string]int) {
	var revResult []interface{}

	iNum := 0
	revSQL := "select"
	revFieldMap := make(map[string]int)

	// 获取字段列表和类型
	queryFieldMap := GetQueryField(pb, queryField)

	for filedName, filedType := range queryFieldMap {
		revSQL += (" " + filedName + ",")
		revFieldMap[filedName] = iNum

		switch filedType {
		case "*basetype.Float":
			revResult = append(revResult, new(sql.NullFloat64))
		case "*basetype.Double":
			revResult = append(revResult, new(sql.NullFloat64))
		case "*basetype.Int32":
			revResult = append(revResult, new(sql.NullInt64))
		case "*basetype.UInt32":
			revResult = append(revResult, new(sql.NullInt64))
		case "*basetype.Int64":
			revResult = append(revResult, new(sql.NullInt64))
		case "*basetype.UInt64":
			revResult = append(revResult, new(sql.NullInt64))
		case "*basetype.String":
			revResult = append(revResult, new(sql.NullString))
		case "*basetype.Date":
			revResult = append(revResult, new(sql.NullString))
		}

		iNum++
	}

	// 去除右边的","
	revSQL = strings.TrimRight(revSQL, ",")

	// 加上表名
	revSQL += " from " + tableName

	return revSQL, revResult, revFieldMap
}

// 返回条件的SQL和条件的参数
func GetWhereInfo(pb proto.Message) (string, []interface{}) {
	// 定义SQL字符串变量和返回参数
	sqlStr := " where "
	var param []interface{}

	// 将protobuf变量转换成MAP
	dataMap := ProtoToMap(pb)

	// 判断是否有填值
	if 0 == len(dataMap) {
		return "", nil
	}

	// 拼装SQL语句
	for k, v := range dataMap {
		sqlStr += k
		sqlStr += " = ? and "
		param = append(param, v)
	}

	// 去除右边的"and "
	sqlStr = strings.TrimRight(sqlStr, "and ")

	return sqlStr, param
}

// 获取插入SQL和插入参数
func GetInsertInfo(tableName string, data proto.Message) (string, []interface{}) {
	// 定义插入参数列表
	var param []interface{}

	// 插入字段的数目
	iFieldNum := 0

	// 将protobuf变量转换成MAP
	dataMap := ProtoToMap(data)

	// 定义SQL字符串变量
	sqlStr := "insert into " + tableName + "("

	// 拼装SQL语句
	for k, v := range dataMap {
		iFieldNum++
		sqlStr += k
		sqlStr += ", "
		param = append(param, v)
	}

	// 去除右边的","
	sqlStr = strings.TrimRight(sqlStr, ", ")

	sqlStr += ") values ( "

	// 添加"?"
	for i := 0; i < iFieldNum; i++ {
		sqlStr += "?, "
	}

	// 去除右边的","
	sqlStr = strings.TrimRight(sqlStr, ", ")

	sqlStr += ")"

	return sqlStr, param
}

// 获取更新语句和参数
func GetUpdateInfo(tableName string, data proto.Message) (string, []interface{}) {
	// 定义插入参数列表
	var param []interface{}

	// 将protobuf变量转换成MAP
	dataMap := ProtoToMap(data)

	// 定义SQL字符串变量
	sqlStr := " update " + tableName + " set "

	// 拼装SQL语句
	for k, v := range dataMap {
		sqlStr += k
		sqlStr += " = ?, "
		param = append(param, v)
	}

	// 去除右边的","
	sqlStr = strings.TrimRight(sqlStr, ", ")

	return sqlStr, param
}
