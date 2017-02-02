package mysql

type Record struct {
	Fields map[string]interface{}
}

func (r *Record) Count() int {
	return len(r.Fields)
}
func (r *Record) HasField(c string) bool {
	_, ok := r.Fields[c]
	return ok
}

func (r *Record) GetString(c string) string {
	if r.HasField(c) {
		return r.Fields[c].(string)
	}
	return ""
}

func (r *Record) GetInt(c string) int {
	if r.HasField(c) {
		return r.Fields[c].(int)
	}
	return 0
}

func NewRecord() *Record {
	return &Record{
		Fields: make(map[string]interface{}),
	}
}

type Rows struct {
	Records []*Record
}
