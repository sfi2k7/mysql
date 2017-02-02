package mysql

type TableStructure struct {
	Name    string
	Columns []*ColumnDefinition
}
