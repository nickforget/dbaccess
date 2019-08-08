package dbaccess_test

import (
	"fmt"
	"github.com/nickforget/dbaccess/basetype"
	"github.com/nickforget/dbaccess/dbaccess"
	"github.com/nickforget/dbaccess/test"
	"testing"
)

/*
CREATE TABLE student (
  name varchar(50) DEFAULT NULL,
  age int(11) DEFAULT NULL,
  no int(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (no),
  KEY index_student_name (name) USING BTREE
);
*/

var db *dbaccess.DBAccess

func init() {
	db = dbaccess.NewDBAccess("mysql", "root:muchinfo@/test")
	db.ConnDB()
}

func TestDBAccessInsert(t *testing.T) {
	stu := &test.Student{
		Name: &basetype.String{Data:"zouqiang"},
		Age:  &basetype.Int32{Data:30},
		NO:   &basetype.Int32{Data:2},
	}

	err := db.Insert("student", stu)

	if err != nil {
		t.Error("Insert err ", err)
	}
}

func TestDBAccessQuery(t *testing.T) {
	data, err := db.Query("student", []string{}, "", &test.Student{})

	if err == nil {
		for _, v := range data {
			tmp := v.(*test.Student)
			t.Log(tmp.GetAge(), tmp.GetName(), tmp.GetNO())
		}

	} else {
		t.Error("Query err ", err)
	}
}

func TestDBAccessUpdate(t *testing.T) {
	stu := &test.Student{
		Name: &basetype.String{Data:"chenyirui"},
		Age:  &basetype.Int32{Data:20},
	}

	err := db.Update("student", stu, &test.Student{
		NO: &basetype.Int32{Data:2},
	})

	if err != nil {
		t.Error("Update err ", err)
	}
}

func TestDBAccessDelete(t *testing.T) {
	err := db.Delete("student", &test.Student{})

	if err != nil {
		t.Error("Delete err ", err)
	}
}

func TestDBAccessCommit(t *testing.T) {
	stu := &test.Student{
		Name: &basetype.String{Data:"chenyirui"},
		Age:  &basetype.Int32{Data:20},
		NO:  &basetype.Int32{Data:0},
	}

	err := db.SetNotAutoCommit()

	if err != nil {
		fmt.Println("SetNotAutoCommit: ", err)
	}

	for i := int32(0); i < 10; i++ {
		stu.NO = &basetype.Int32{Data:i}
		err = db.Insert("student", stu)

		if err != nil {
			fmt.Println("Insert: ", err)
		}
	}

	db.Commit()
}

func TestDBAccessRollback(t *testing.T) {
	stu := &test.Student{
		Name: &basetype.String{Data:"chenyirui"},
		Age: &basetype.Int32{Data:20},
		NO: &basetype.Int32{Data:0},
	}

	err := db.SetNotAutoCommit()

	if err != nil {
		fmt.Println("SetNotAutoCommit: ", err)
	}

	for i := int32(10); i < 20; i++ {
		stu.NO = &basetype.Int32{Data:i}
		err = db.Insert("student", stu)

		if err != nil {
			fmt.Println("Insert: ", err)
		}
	}

	// 查询
	revData, err := db.Query("student", []string{}, "", &test.Student{})

	if err == nil {
		for _, v := range revData {
			tmp := *v.(*test.Student)
			fmt.Println(tmp.GetName(), tmp.GetName(), tmp.GetAge(), tmp.GetNO())
		}
	}

	db.Rollback()

	// 回滚之后再次查询结果
	revData, err = db.Query("student", []string{}, "", &test.Student{})

	if err == nil {
		for _, v := range revData {
			tmp := *v.(*test.Student)
			fmt.Println(tmp.GetName(), tmp.GetName(), tmp.GetAge(), tmp.GetNO())
		}
	}
}