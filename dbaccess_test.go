package dbaccess

import (
	"fmt"
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

var db *DBAccess

func init() {
	db = NewDBAccess("mysql", "root:muchinfo@/test")
	db.ConnDB()
}

func TestDBAccessInsert(t *testing.T) {
	stu := &Student{
		Name: &String{Data:"zouqiang"},
		Age:  &Int32{Data:30},
		NO:   &Int32{Data:2},
	}

	err := db.Insert("student", stu)

	if err != nil {
		t.Error("Insert err ", err)
	}
}

func TestDBAccessQuery(t *testing.T) {
	data, err := db.Query("student", []string{}, "", &Student{})

	if err == nil {
		for _, v := range data {
			tmp := v.(*Student)
			t.Log(tmp.GetAge(), tmp.GetName(), tmp.GetNO())
		}

	} else {
		t.Error("Query err ", err)
	}
}

func TestDBAccessUpdate(t *testing.T) {
	stu := &Student{
		Name: &String{Data:"chenyirui"},
		Age:  &Int32{Data:20},
	}

	err := db.Update("student", stu, &Student{
		NO: &Int32{Data:2},
	})

	if err != nil {
		t.Error("Update err ", err)
	}
}

func TestDBAccessDelete(t *testing.T) {
	err := db.Delete("student", &Student{})

	if err != nil {
		t.Error("Delete err ", err)
	}
}

func TestDBAccessCommit(t *testing.T) {
	stu := &Student{
		Name: &String{Data:"chenyirui"},
		Age:  &Int32{Data:20},
		NO:  &Int32{Data:0},
	}

	err := db.SetNotAutoCommit()

	if err != nil {
		fmt.Println("SetNotAutoCommit: ", err)
	}

	for i := int32(0); i < 10; i++ {
		stu.NO = &Int32{Data:i}
		err = db.Insert("student", stu)

		if err != nil {
			fmt.Println("Insert: ", err)
		}
	}

	db.Commit()
}

func TestDBAccessRollback(t *testing.T) {
	stu := &Student{
		Name: &String{Data:"chenyirui"},
		Age: &Int32{Data:20},
		NO: &Int32{Data:0},
	}

	err := db.SetNotAutoCommit()

	if err != nil {
		fmt.Println("SetNotAutoCommit: ", err)
	}

	for i := int32(10); i < 20; i++ {
		stu.NO = &Int32{Data:i}
		err = db.Insert("student", stu)

		if err != nil {
			fmt.Println("Insert: ", err)
		}
	}

	// 查询
	revData, err := db.Query("student", []string{}, "", &Student{})

	if err == nil {
		for _, v := range revData {
			tmp := v.(*Student)
			fmt.Println(tmp.GetName(), tmp.GetName(), tmp.GetAge(), tmp.GetNO())
		}
	}

	db.Rollback()

	// 回滚之后再次查询结果
	revData, err = db.Query("student", []string{}, "", &Student{})

	if err == nil {
		for _, v := range revData {
			tmp := v.(*Student)
			fmt.Println(tmp.GetName(), tmp.GetName(), tmp.GetAge(), tmp.GetNO())
		}
	}
}