package mysql

type ColumnDefinition struct {
	ColumnName      string `db:"column_name"`
	OrdinalPosition string `db:"ordinal_position"`
	DataType        string `db:"data_type"`
	ColumnType      string `db:"column_type"`
}
