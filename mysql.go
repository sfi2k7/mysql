package mysql

import (
	"fmt"
	"reflect"

	"strconv"

	"errors"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

const (
	tagName string = "db"
)

type M map[string]interface{}

func (m M) ToMap() map[string]interface{} {
	mm := make(map[string]interface{})
	for k, v := range m {
		mm[k] = v
	}
	return mm
}

type MySQL struct {
	db           string
	table        string
	sel          string
	where        string
	whereParams  M
	limit        int
	skip         int
	isOpen       bool
	connection   *sqlx.DB
	cs           string
	updates      M
	inserts      M
	LastInsertId int64
	LastAffected int64
	SafetyCheck  bool
	sort         []string
}

func (m *MySQL) Conn() *sqlx.DB {
	m.open()
	return m.connection
}

func (m *MySQL) T(tableName string) *MySQL {
	m.table = tableName
	return m
}

func (m *MySQL) Sort(sorts ...string) *MySQL {
	m.sort = sorts
	return m
}

func (m *MySQL) Limit(n int) *MySQL {
	m.limit = n
	return m
}

func (m *MySQL) Select(columns string) *MySQL {
	m.sel = columns
	return m
}

func (m *MySQL) DisableSafety() *MySQL {
	m.SafetyCheck = false
	return m
}

func (m *MySQL) Update(updates M) error {
	if m.SafetyCheck && len(m.where) == 0 {
		return errors.New("Updates must accompny a WHERE clause. Set SafetyCheck to false to disable this behaviour ")
	}
	m.updates = updates
	s := m.prepare()
	m.open()
	defer m.reset()
	if len(m.whereParams) > 0 {
		for k, v := range m.whereParams {
			updates[k] = v
		}
	}
	r, err := m.connection.NamedExec(s, updates.ToMap())
	if err != nil {
		return err
	}
	affected, err := r.RowsAffected()
	if err != nil {
		return err
	}
	m.LastAffected = affected
	return nil
}

func (m *MySQL) Insert(inserts interface{}) error {
	i, ok := inserts.(M)
	if !ok {
		i = ToM(inserts, tagName)
	}
	m.inserts = i
	// if reflect.ValueOf(inserts).Kind() == reflect.Struct {
	// 	m.inserts = ToM(inserts, tagName)
	// } else {
	// 	m.inserts = inserts.(M)
	// }

	s := m.prepare()

	m.open()
	defer m.reset()
	r, err := m.connection.NamedExec(s, m.inserts.ToMap())
	if err != nil {
		return err
	}
	lastInsertId, err := r.LastInsertId()
	if err != nil {
		return err
	}
	m.LastInsertId = lastInsertId
	return nil
}

func (m *MySQL) One(target interface{}, args ...interface{}) error {
	m.open()
	defer m.reset()
	if len(m.inserts) == 0 && len(m.updates) == 0 && len(m.sel) == 0 {
		m.sel = "*"
	}

	s := m.prepare()
	//fmt.Println(s)
	var row *sqlx.Row
	if len(args) == 0 {
		row = m.connection.QueryRowx(s)
	} else {
		row = m.connection.QueryRowx(s, args...)
	}

	if row.Err() != nil {
		fmt.Println("Row Error", row.Err())
		return row.Err()
	}

	err := row.StructScan(target)

	return err
}

func (m *MySQL) AllRows(args ...interface{}) (*sqlx.Rows, error) {
	m.open()
	defer m.reset()
	if len(m.inserts) == 0 && len(m.updates) == 0 && len(m.sel) == 0 {
		m.sel = "*"
	}

	s := m.prepare()
	return m.connection.Queryx(s, args...)
}

// func (m *MySQL) All(out interface{}, args ...interface{}) error {
// 	m.open()
// 	defer m.reset()
// 	if len(m.inserts) == 0 && len(m.updates) == 0 && len(m.sel) == 0 {
// 		m.sel = "*"
// 	}

// 	s := m.prepare()
// 	res, err := m.connection.Queryx(s, args...)
// 	if err != nil {
// 		return err
// 	}
// 	defer res.Close()
// 	for res.Next() {
// 		v := reflect.ValueOf(out)
// 		if v.Kind() == reflect.Ptr {
// 			v = v.Elem()
// 		}
// 		T := v.Type().Elem()
// 		//newi := reflect.New(T.Elem())
// 		newi := reflect.Zero(T)
// 		// if newi.Kind() == reflect.Ptr {
// 		// 	newi = newi.Elem()
// 		// }
// 		i := newi.Interface()
// 		err := res.StructScan(&i)
// 		fmt.Println(err)
// 		fmt.Println(newi)
// 	}
// 	return nil

// }

func (m *MySQL) Where(where string, args M) *MySQL {
	m.where = where
	m.whereParams = args
	return m
}

func (m *MySQL) Close() {
	if !m.isOpen {
		return
	}
	if m.connection == nil {
		return
	}
	if m.connection.Ping() != nil {
		m.connection.Close()
	}
	m.connection = nil
}

func (m *MySQL) open() error {
	if m.isOpen {
		return nil
	}
	c, err := sqlx.Open("mysql", m.cs)
	if err != nil {
		m.isOpen = false
		fmt.Println(err)
		return err
	}
	m.connection = c
	m.isOpen = true
	return nil
}

func (m *MySQL) prepare() string {
	if len(m.sel) > 0 {
		var str = "SELECT " + m.sel + " FROM " + m.table
		if len(m.where) > 0 {
			str += " WHERE " + m.where
		}

		if len(m.sort) > 0 {
			str += " ORDER BY "
			for _, s := range m.sort {
				str += s + ","
			}
			str = str[0 : len(str)-1]
		}

		if m.limit > 0 {
			str += " LIMIT " + strconv.Itoa(m.limit)
		}

		return str
	}

	if len(m.inserts) > 0 {
		var cols []string
		for c := range m.inserts {
			cols = append(cols, c)
		}
		var str = "INSERT INTO " + m.table + " ("
		for _, c := range cols {
			str += c + ","
		}
		str = str[0 : len(str)-1]
		str += ") VALUES ("
		for _, c := range cols {
			str += ":" + c + ","
		}
		str = str[0 : len(str)-1]
		str += ")"
		return str
	}

	if len(m.updates) > 0 {
		var str = "UPDATE " + m.table + " "
		var cols []string
		for c := range m.updates {
			cols = append(cols, c)
		}

		for _, c := range cols {
			str += "SET " + c + "=:" + c + ","
		}

		str = str[0 : len(str)-1]
		if len(m.where) > 0 {
			str += " WHERE " + m.where
		}
		return str
	}
	return ""
}

func (m *MySQL) reset() {
	m.db = ""
	m.limit = 0
	m.sel = ""
	m.skip = 0
	m.table = ""
	m.where = ""
	m.inserts = nil
	m.updates = nil
	m.sort = []string{}
	m.whereParams = M{}
}

func New(cs string) *MySQL {
	return &MySQL{
		cs:          cs,
		SafetyCheck: true,
	}
}

func ToM(s interface{}, tagName string) M {
	m := make(M, 0)
	v := reflect.ValueOf(s)

	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil // , errors.New("Input must be of type struct")
	}

	t := v.Type()
	for x := 0; x < v.NumField(); x++ {
		f := t.Field(x)
		if len(tagName) > 0 {
			tag := f.Tag.Get(tagName)
			if tag == "id" {
				continue
			}
			if len(tag) > 0 {
				m[tag] = v.Field(x).Interface()
			}
			continue
		}
		m[f.Name] = v.Field(x).Interface()
	}
	return m //, nil
}
