package mysql

import (
	"fmt"

	"strconv"

	"errors"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
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
}

// func (m *MySQL) DB(db string) *MySQL {
// 	m.db = db
// 	return m
// }

func (m *MySQL) T(tableName string) *MySQL {
	m.table = tableName
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

func (m *MySQL) Insert(inserts M) error {
	m.inserts = inserts
	s := m.prepare()
	m.open()
	defer m.reset()
	//var mm = make(map[string]interface{})
	r, err := m.connection.NamedExec(s, inserts.ToMap())
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

func (m *MySQL) Where(where string) *MySQL {
	m.where = where
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

		if m.limit > 0 {
			str += " LIMIT " + strconv.Itoa(m.limit)
		}

		return str
	}

	if len(m.inserts) > 0 {
		var cols []string
		for c, _ := range m.inserts {
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
		for c, _ := range m.updates {
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
}

func New(cs string) *MySQL {
	return &MySQL{
		cs:          cs,
		SafetyCheck: true,
	}
}
