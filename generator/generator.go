package generator

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"text/template"
)

// DB represents the Database that we want to dump
type DB struct {
	// The name of the schema from the data base we care to dump
	Schema string
	*sql.DB
}

// Tables returns a mapping for the tables that are in the database
func (db *DB) Tables() (tables map[string][]SchemaObjectDescription) {
	if db == nil || db.DB == nil {
		panic("db is nil")
	}
	tables = make(map[string][]SchemaObjectDescription)
	schema := db.Schema
	if schema == "" {
		schema = DefaultSchema
	}
	selectSQL := fmt.Sprintf(SelectAllTableFromSchema, schema)
	rows, err := db.Query(selectSQL)
	if err != nil {
		log.Printf("error running SQL\n%s", selectSQL)
		panic("not expecting error")
	}
	defer rows.Close()
	for rows.Next() {
		var sod SchemaObjectDescription
		if err := sod.Scan(rows); err != nil {
			log.Fatal(err)
		}
		tables[sod.TableName] = append(tables[sod.TableName], sod)
	}
	for name := range tables {
		sort.Sort(byTableTypeAndSQL(tables[name]))
	}
	return tables
}

func (db *DB) InsertSQL(tableName string, skipFields ...string) []byte {

	// lets get all the data from the database
	rows, err := db.Query(fmt.Sprintf(`SELECT * FROM "%v";`, tableName))
	if err != nil {
		log.Fatal("got error getting data", err)
	}
	defer rows.Close()

	var columns insertColumns
	{
		dbColumns, err := rows.Columns()
		if err != nil {
			log.Fatal("got error getting columns", err)
		}
		columns = make(insertColumns, 0, len(dbColumns))
		for i := range dbColumns {
			name := strings.ToLower(dbColumns[i])
			skip := false
			for _, fld := range skipFields {
				if name == fld {
					skip = true
					break
				}
			}
			columns = append(columns, insertColumn{Name: name, Index: i, Skip: skip})
		}
	}
	var values [][]string

	for rows.Next() {
		vals := make([]interface{}, len(columns))
		// need to assign a *interface{} value to each entry
		for i := range vals {
			vals[i] = new(interface{})
		}
		if err := rows.Scan(vals...); err != nil {
			panic(err)
		}
		stringValues := interfaceValuesToString(vals)
		// run through the strings and adjust the max
		for i := range columns {
			l := len(stringValues[columns[i].Index])
			if l > columns[i].MaxLength {
				columns[i].MaxLength = l
			}
		}

		values = append(values, stringValues)
	}

	// Nothing to do, just return empty
	if len(values) == 0 {
		return nil
	}

	sort.Sort(columns)
	var tplContext = struct {
		TableName string
		Columns   string
		Values    []string
	}{
		Columns:   columns.String(),
		TableName: tableName,
	}

	for i := range values {
		spacer := ", "
		if i == 0 {
			spacer = "  "
		}
		tplContext.Values = append(tplContext.Values,
			spacer+valueRowString(columns, values[i]),
		)
	}
	var buff bytes.Buffer
	if err = insertTemplate.Execute(&buff, tplContext); err != nil {
		log.Fatalf("system error running insertSQL with %v : %v", tplContext, err)
	}
	return buff.Bytes()

}

// SelectAllTableFromSchema is a simple sql for getting table info
//
// REF: https://sqlite.org/schematab.html
const (
	SelectAllTableFromSchema = `
SELECT
    tbl_name as table_name
  , type
  , name as unique_name -- only really applicable to the auto-created indexes
  , sql -- will be null for auto-created indexes
FROM
  %s.sqlite_schema
;
`
	DefaultSchema = "main"
)

type DBScanner interface {
	Scan(...interface{}) error
}

type SchemaType uint8

const (
	SchemaTypeTable SchemaType = iota
	SchemaTypeView
	SchemaTypeTrigger
	SchemaTypeIndex

	// SchemaTypeUnknown should always be the last entry
	SchemaTypeUnknown
)

var schemaTypes = [...]string{"table", "index", "view", "trigger", "unknown"}

func (st SchemaType) String() string {
	idx := int(st)
	if idx > int(SchemaTypeUnknown) {
		idx = int(SchemaTypeUnknown)
	}
	return schemaTypes[idx]
}

func (st *SchemaType) Scan(src interface{}) error {
	s, ok := src.(string)
	if !ok {
		return fmt.Errorf("unsupported type %#v", src)
	}
	*st = SchemaTypeUnknown
	s = strings.ToLower(strings.TrimSpace(s))
	for i, t := range schemaTypes {
		if t == s {
			*st = SchemaType(i)
			return nil
		}
	}
	return nil
}

type SchemaColumn struct {
	ID           int
	Name         string
	Type         string
	NotNull      bool
	DefaultValue *interface{}
	PrimaryKey   bool
}

type SchemaObjectDescription struct {
	TableName string
	Type      SchemaType
	SQL       string
}

func (sod *SchemaObjectDescription) Scan(s DBScanner) error {
	var (
		unique   string
		tableSQL *string
	)

	err := s.Scan(&sod.TableName, &sod.Type, &unique, &tableSQL)
	if err != nil {
		return err
	}
	if tableSQL == nil {
		tableSQL = new(string)
	} else {
		*tableSQL = strings.TrimSpace(*tableSQL)
	}
	if *tableSQL != "" && !strings.HasSuffix(*tableSQL, ";") {
		*tableSQL += ";"
	}
	sod.SQL = *tableSQL
	return nil
}

var insertTemplate = template.Must(template.New("insertTemplate").Parse(`
INSERT INTO {{.TableName}}
  {{.Columns}}
VALUES
{{range .Values -}}
  {{.}}
{{end -}}
;
`))

// insertColumn represents a column in the database
type insertColumn struct {
	// Name is the name of the column
	Name string
	// MaxLength is the largest size of the value in this field, used to calculate the padding between fields
	// to line up values
	MaxLength int
	// Index of the position of the data element, this way we can sort the columns and still
	// reference the correct data element.
	Index int
	// Skip indicates weather or not we should skip the column for insert sql
	Skip bool
}

type insertColumns []insertColumn

// String is an SQL friendly representation of the list of columns
func (c insertColumns) String() string {
	if c.LenWithoutSkip() == 0 {
		// this should not be the case
		return ""
	}
	var str strings.Builder
	first := true

	str.WriteString("( ")
	for i := 0; i < len(c); i++ {
		if c[i].Skip {
			continue
		}
		if !first {
			str.WriteString(", ")
		}
		str.WriteString(c[i].Name)
		first = false
	}
	str.WriteString(" )")
	return str.String()
}

// Len returns total number of columns
func (c insertColumns) Len() int { return len(c) }

// Less will return if the column order for i is less then column order for j
func (c insertColumns) Less(i, j int) bool {
	// sort skipped values to the end
	if c[i].Skip != c[j].Skip {
		return c[j].Skip
	}
	if c[i].MaxLength != c[j].MaxLength {
		return c[i].MaxLength < c[j].MaxLength
	}
	if c[i].Name != c[j].Name {
		return c[i].Name < c[j].Name
	}
	return c[i].Index < c[j].Index
}

// Swap will swap the positions of the columns in the slice
func (c insertColumns) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

// LenWithoutSkip returns the number of columns that are not skipped
func (c insertColumns) LenWithoutSkip() int {
	count := 0
	for i := range c {
		if c[i].Skip {
			continue
		}
		count++
	}
	return count
}

// valueRowString will return a value row for an INSERT VALUES statement
func valueRowString(columns insertColumns, values []string) string {
	var str strings.Builder
	columnLen := columns.LenWithoutSkip()
	format := "( % *s" + strings.Repeat(", % *s", columnLen-1) + " )"
	str.WriteString("( ")

	stringValues := make([]interface{}, 0, columnLen*2)
	// assume there is at least one column that is not skipped
	for _, clm := range columns {
		if clm.Skip {
			continue
		}
		stringValues = append(stringValues,
			0-clm.MaxLength,
			values[clm.Index],
		)
	}
	return fmt.Sprintf(format, stringValues...)
}

// interfaceValToStr will return a string version of the file that is
// valid for sqlite Insert statement
func interfaceValToStr(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	value := reflect.ValueOf(val)

	switch value.Kind() {

	case reflect.Ptr:

		if value.IsNil() {
			return "NULL"
		}
		return interfaceValToStr(value.Elem().Interface())

	case reflect.String:
		str := val.(string)
		str = strings.Replace(str, `'`, `''`, -1)
		return fmt.Sprintf("'%s'", str)

	case reflect.Slice, reflect.Array:

		// only care about uint8 and int32 as these are []byte and []rune
		subKind := value.Type().Elem().Kind()
		if subKind == reflect.Uint8 || subKind == reflect.Int32 {
			return fmt.Sprintf(`X'%x'`, value.Bytes())
		}
		// otherwise lets just treat it like a string
		fallthrough
	default:
		return interfaceValToStr(fmt.Sprintf("%v", val))
	}
}

// interfaceValuesToString will return a string version of the value that would
// be acceptable to insert into an Insert statement.
//
// E.G. String values will the quoted with "'", "cat" will be "'cat'" and "it's" will be 'it''s'
// nil will be NULL
// etc...
func interfaceValuesToString(values []interface{}) []string {
	stringValues := make([]string, len(values))
	for i := range values {
		stringValues[i] = interfaceValToStr(values[i])
	}
	return stringValues
}

// byTableTypeAndSQL will sort the the SchemaObjectDescription by placing the objects with out SQL's at the end
// and then by the Type of the Object
type byTableTypeAndSQL []SchemaObjectDescription

func (by byTableTypeAndSQL) Len() int      { return len(by) }
func (by byTableTypeAndSQL) Swap(i, j int) { by[i], by[j] = by[j], by[i] }
func (by byTableTypeAndSQL) Less(i, j int) bool {
	if by[i].SQL == "" {
		return false
	}
	if by[j].SQL == "" {
		return true
	}
	// compare the type
	return by[i].Type < by[i].Type
}

func GetOrderedTablesNamesFromFile(filename string) (tableNames []string, fields [][]string, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, nil, nil
	}
	defer f.Close()
	return GetOrderedTableNames(f)
}

// GetOrderedTablesNames parses the io.Reader with table names in them, and returns the table
// names in the order they are present in the file. No checks are done to see if the names
// are unique.
func GetOrderedTableNames(r io.Reader) (tableNames []string, fields [][]string, err error) {
	scanner := bufio.NewScanner(r)
	var globalExcludedFields = make(map[string]struct{})
	for scanner.Scan() {
		tableName, excludedFields, _ := parseTableNameEntry(scanner.Text())
		if len(excludedFields) == 0 && (tableName == "" || tableName == "*") {
			// nothing to do here
			continue
		}
		if len(excludedFields) > 0 && (tableName == "" || tableName == "*") {
			for _, fld := range excludedFields {
				globalExcludedFields[fld] = struct{}{}
			}
			continue
		}

		tableNames = append(tableNames, tableName)
		fields = append(fields, excludedFields)
	}

	if err = scanner.Err(); err != nil {
		return nil, nil, err
	}

	// need to add the global excluded fields to all the tables
	// and sort and unique the fields
	for i := range fields {
		fmap := make(map[string]struct{})

		for key, v := range globalExcludedFields {
			fmap[key] = v
		}
		for j := range fields[i] {
			fmap[fields[i][j]] = struct{}{}
		}
		// reset the slice
		fields[i] = fields[i][:0]
		for key := range fmap {
			fields[i] = append(fields[i], key)
		}
		sort.Strings(fields[i])
	}

	return tableNames, fields, nil
}

var parseTableNameEntryRegexp = func() *regexp.Regexp {
	nameRexp := `(?P<tablename>[*]|[a-zA-Z][a-zA-Z0-9_]*|"[^"]+|")`
	fieldRexp := `([a-zA-Z][a-zA-Z0-9_]*|"[^"]+")`
	fieldsRexp := fmt.Sprintf(`(?P<exclude>%[1]s(?:\s*,\s*%[1]s)*)`, fieldRexp)
	commentRexp := `(?:\s*#(?P<comment>.*))?`
	fullRexp := fmt.Sprintf(`^\s*%[1]s?(?:\s+!\s+%[2]s)?%[3]s$`,
		nameRexp,
		fieldsRexp,
		commentRexp,
	)
	return regexp.MustCompile(fullRexp)
}()
var parseTableNameEntryFieldsRegexp = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9_]*|"[^"]+")`)

// parseTableNameEntry will parse the given line to find the table name, and any excluded files.
// The expected like should look like one the following:
// (${table_name})? (! ${field_name}(,%{filed_name})*)? (${comment})?
// where
//   a $table_name is ([a-zA-Z][a-zA-Z0-9_]*|"[^"]+")
//   a $field_name is ([a-zA-Z][a-zA-Z0-9_]*|"[^"]+")
func parseTableNameEntry(line string) (tableName string, excludeFields []string, comment string) {
	var lck sync.Mutex
	lck.Lock()
	defer lck.Unlock()
	matches := parseTableNameEntryRegexp.FindStringSubmatch(line)
	if len(matches) == 0 {
		log.Printf("line: %#v\n", line)
		return tableName, excludeFields, comment
	}
	tblIndex := parseTableNameEntryRegexp.SubexpIndex("tablename")
	if tblIndex != -1 {
		if len(matches) <= tblIndex {
			log.Printf("line: %#v\n", line)
			log.Printf("werid: matchs %#v\n%d\n", matches, tblIndex)
		}
		tableName = strings.Trim(matches[tblIndex], `"`)
	}

	excludeIndex := parseTableNameEntryRegexp.SubexpIndex("exclude")
	if excludeIndex != -1 {
		excludeFields = parseTableNameEntryFieldsRegexp.FindAllString(matches[excludeIndex], -1)
		for i := range excludeFields {
			excludeFields[i] = strings.Trim(excludeFields[i], `"`)
		}
	}
	commentIndex := parseTableNameEntryRegexp.SubexpIndex("comment")
	if commentIndex != -1 {
		comment = matches[commentIndex]
	}
	return tableName, excludeFields, comment
}

/* Compile time checks */

var (
	_ = schemaTypes[SchemaTypeUnknown]
)
