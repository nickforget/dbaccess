package dbaccess

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/proto"
)

type DBAccess struct {
	dbType  string
	connStr string
	db      *sql.DB
}

func NewDBAccess(inDBType string, inConnstr string) *DBAccess {
	return &DBAccess{
		dbType:  inDBType,
		connStr: inConnstr,
		db:      nil,
	}
}

func (pDB *DBAccess) ConnDB() error {
	db, err := sql.Open(pDB.dbType, pDB.connStr)
	pDB.db = db
	return err
}

func (pDB *DBAccess) SetAutoCommit() error {
	_, err := pDB.db.Exec("set autocommit=1")
	return err
}

func (pDB *DBAccess) SetNotAutoCommit() error {
	_, err := pDB.db.Exec("set autocommit=0")
	return err
}

func (pDB *DBAccess) Commit() error {
	_, err := pDB.db.Exec("commit")
	return err
}

func (pDB *DBAccess) Rollback() error {
	_, err := pDB.db.Exec("rollback")
	return err
}

func (pDB *DBAccess) Insert(tableName string, data proto.Message) error {
	sqlStr, param := GetInsertInfo(tableName, data)

	_, err := pDB.db.Exec(sqlStr, param...)

	return err
}

func (pDB *DBAccess) Query(tableName string, queryFiled []string, extra string, where proto.Message) ([]interface{}, error) {
	revData := make([]interface{}, 0)

	// 返回查询的SQL和获取结果的参数
	sql, param, filedMap := GetQueryInfo(tableName, where, queryFiled)

	// 返回条件的SQL和条件的参数
	sqlWhere, paramWhere := GetWhereInfo(where)

	// 调用操作
	rows, err := pDB.db.Query(sql + sqlWhere + extra, paramWhere...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	// 返回结果集
	for rows.Next() {
		err = rows.Scan(param...)

		if nil == err {
			tmpData := proto.Clone(where)
			tmpData.Reset()

			DataToProto(param, filedMap, tmpData)
			revData = append(revData, tmpData)
		} else {
			return nil, err
		}
	}

	return revData, nil
}

func (pDB *DBAccess) Delete(tableName string, where proto.Message) error {
	// 返回条件的SQL和条件的参数
	sqlWhere, paramWhere := GetWhereInfo(where)

	sql := "delete from " + tableName + sqlWhere

	// 调用操作
	_, err := pDB.db.Exec(sql, paramWhere...)

	return err
}

func (pDB *DBAccess) Update(tableName string, data proto.Message, where proto.Message) error {
	// 获取更新语句和参数
	sqlUpdate, dataParam := GetUpdateInfo(tableName, data)

	// 获取条件语句和参数
	sqlWhere, paramWhere := GetWhereInfo(where)

	// 组合SQL
	sql := sqlUpdate + sqlWhere

	// 组合参数
	dataParam = append(dataParam, paramWhere...)

	// 执行更新操作
	_, err := pDB.db.Exec(sql, dataParam...)

	return err
}