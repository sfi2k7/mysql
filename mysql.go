package mysql

import (
	"database/sql"
	"fmt"
	"reflect"

	"strconv"

	"errors"

	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

const (
	tagName string = "db"
)

var (
	NotFoundError = errors.New("Not Found")
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

	//fmt.Println("Updating with ", s, updates.ToMap())
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
		str += " SET "
		for _, c := range cols {
			str += " " + c + "=:" + c + ","
		}

		str = str[0 : len(str)-1]
		if len(m.where) > 0 {
			str += " WHERE " + m.where
		}
		return str
	}
	return ""
}

func (m *MySQL) simpleQuery(query string) error {
	rows, err := m.connection.Query(query)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return nil
	}
	return errors.New("No Response from DB")
}

func (m *MySQL) CreateDatabase(dbName string) {
	err := m.simpleQuery("CREATE DATABASE `" + dbName + "`  DEFAULT CHARACTER SET latin1")
	if err != nil {
		fmt.Println(err)
	}
}

func (m *MySQL) UserExists(userName string) bool {
	res, err := m.Conn().Query("SELECT User from mysql.user WHERE User = ? LIMIT 1", userName)
	if err != nil {
		fmt.Println(err)
		return false
	}
	defer res.Close()
	for res.Next() {
		var user string
		res.Scan(&user)
		return user == userName
	}
	return false
}

func (m *MySQL) createUser(userName, password string) error {
	if m.UserExists(userName) {
		return nil
	}
	sql := "CREATE USER '" + userName + "'@'%' IDENTIFIED BY '" + password + "'"
	_, err := m.Conn().Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func (m *MySQL) AssignPermissions(db, userName string) error {
	sql := fmt.Sprintf("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%' WITH GRANT OPTION;", db, userName)
	_, err := m.Conn().Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func (m *MySQL) DatabaseExists(db string) (bool, error) {
	sql := "show databases"
	res, err := m.Conn().Query(sql)
	if err != nil {
		return false, err
	}
	defer res.Close()
	for res.Next() {
		var dbName string
		res.Scan(&dbName)
		if dbName == db {
			return true, nil
		}
	}
	return false, nil
}

func (m *MySQL) ListTables(db string) ([]string, error) {
	sql := `SELECT TABLE_NAME from information_schema WHERE TABLE_SCHEMA = '` + db + "'"
	var tables []string
	res, err := m.Conn().Query(sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	for res.Next() {
		var tableName string
		res.Scan(&tableName)
		tables = append(tables, tableName)
	}
	return tables, nil
}

func (m *MySQL) DeleteColumn(tableName, columnName string) error {
	sql := fmt.Sprintf("ALTER TABLE '%s' DROP COLUMN %s", tableName, columnName)
	_, err := m.Conn().Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func (m *MySQL) AddColumn(column *ColumnDefinition, after, tableName string) error {
	def := `ADD COLUMN ` + ColumnDefinitionStringBasedOnType(column.ColumnName, column.ColumnType)
	def = def[0 : len(def)-1]
	if len(after) > 0 {
		def += " AFTER " + after
	}
	sql := fmt.Sprintf("ALTER TABLE `%s` %s", tableName, def)
	_, err := m.Conn().Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func (m *MySQL) TableExists(dbName, tableName string) (bool, error) {
	tables, _ := m.ListTables(dbName)
	if tables == nil {
		return false, nil
	}

	for _, t := range tables {
		if t == tableName {
			return true, nil
		}
	}
	return false, nil
}

func (m *MySQL) GetCurrentStructure(dbName, tableName string) (*TableStructure, error) {
	sql := fmt.Sprintf(`select column_name,ordinal_position,data_type,column_type from information_schema.COLUMNS where table_schema = '%s' and table_name = '%s'`, dbName, tableName)
	res, err := m.Conn().Queryx(sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	table := &TableStructure{}
	for res.Next() {
		var column ColumnDefinition
		res.StructScan(&column)
		table.Columns = append(table.Columns, &column)
	}
	table.Name = tableName
	return table, nil
}

func (m *MySQL) DropTable(tableName string) error {
	sql := `DROP TABLE ` + tableName
	_, err := m.Conn().Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func (m *MySQL) CreateTableFromHeader(tableName, header, keyField string) error {
	fields := strings.Split(header, ",")
	sql := " CREATE TABLE `" + tableName + "` ("
	for _, f := range fields {
		nameAndType := strings.Split(f, "?")
		sql += ColumnDefinitionStringBasedOnType(nameAndType[0], nameAndType[1])
	}

	sql += "`current_hash` BIGINT(20) DEFAULT NULL,"
	if len(keyField) > 0 {
		if strings.Contains(keyField, ",") {
			sql = sql[0 : len(sql)-1]
		} else {
			sql += "PRIMARY KEY (`" + keyField + "`"
		}
	} else {
		sql = sql[0 : len(sql)-1]
	}
	sql += ` ) ENGINE=InnoDB DEFAULT CHARSET=latin1;`

	_, err := m.Conn().Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func (m *MySQL) ListForKey(tableName, key string, val interface{}) ([][]interface{}, error) {
	sql := `SELECT * FROM ` + tableName + " WHERE `" + key + "` = ? LIMTI 1"
	list, err := m.List(sql, val)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (m *MySQL) MapForKey(tableName, key string, val interface{}) (*Record, error) {
	sql := `SELECT * FROM ` + tableName + " WHERE `" + key + "` = ? LIMTI 1"
	list, err := m.ListMap(sql, val)
	if err != nil {
		return nil, err
	}
	if len(list.Records) > 0 {
		return list.Records[0], nil
	}
	return nil, errors.New("Not Found")
}

func (m *MySQL) ListMap(statement string, args ...interface{}) (*Rows, error) {
	var res *sql.Rows
	var err error
	res, err = m.Conn().Query(statement, args...)
	// if len(args) == 0 {
	// 	res, err = m.Conn().Query(statement)
	// } else {
	// 	res, err = m.Conn().Query(statement, args)
	// }

	if err != nil {
		return nil, err
	}
	defer res.Close()
	columns, _ := res.Columns()
	records := &Rows{}
	for res.Next() {
		record := make([]interface{}, len(columns))
		for i := range record {
			record[i] = new(interface{})
		}

		res.Scan(record...)

		r := NewRecord()
		for i := range record {
			r.Fields[columns[i]] = *(record[i].(*interface{}))
		}
		records.Records = append(records.Records, r)
		record = nil
	}
	return records, nil
}

func (m *MySQL) List(statement string, args ...interface{}) ([][]interface{}, error) {
	res, err := m.Conn().Queryx(statement, args...)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	columns, _ := res.Columns()
	var records [][]interface{}
	for res.Next() {
		record := make([]interface{}, len(columns))
		for i := range record {
			record[i] = new(interface{})
		}

		res.Scan(record...)

		for i := range record {
			record[i] = *(record[i].(*interface{}))
		}
		records = append(records, record)
	}
	return records, nil
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

//Column Definition coming from C#. Data types of .NET
func ColumnDefinitionStringBasedOnType(columnName, columnType string) string {
	sql := "`" + columnName + "` "
	if columnType == "string" {
		sql += "`" + columnName + "` varchar(100) DEFAULT NULL,"
	}

	if columnType == "int32" {
		sql += "`" + columnName + "` int(11) DEFAULT NULL,"
	}
	if columnType == "int64" {
		sql += "`" + columnName + "` bigint(11) DEFAULT NULL,"
	}

	if columnType == "decimal" {
		sql += "`" + columnName + "` DECIMAL(11,5) DEFAULT NULL,"
	}
	if columnType == "boolean" {
		sql += "`" + columnName + "` BIT(1) DEFAULT NULL,"
	}

	if columnType == "datetime" {
		sql += "`" + columnName + "` DATETIME NULL DEFAULT NULL,"
	}
	return sql
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
