// Package sqlite provides the schema interface for an SQLite database.
package sqlite

import (
	"database/sql"
	"fmt"
	"github.com/gdey/sqlite-migration/schema"
	"strings"
	"sync"
	"time"
)

const (
	ObjectTypeTrigger = "trigger"
	ObjectTypeTable   = "table"
	ObjectTypeIndex   = "index"
	ObjectTypeView    = "view"
)

const (
	sqliteObjSQL         = `select type, name, tbl_name, sql from main.sqlite_master where type=?;`
	sqliteObjSQLForTable = `select type, name, tbl_name, sql from main.sqlite_master where type=? AND tbl_name=?`
)

type database = *sql.DB

func New(filename string) (*DB, error) {
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}
	me := &DB{
		DB:     db,
		DBName: filename,
	}
	return me, me.Init()
}

type DB struct {
	// We will start by using the sql.DB object, which is generic, and switch to the sqlite3.SQLiteConn object
	// if we need to.
	DB         database
	DBName     string
	schemaLock sync.Mutex
	*Schema
}

func (db *DB) Database() *sql.DB {
	if db == nil {
		return nil
	}
	return db.DB
}

func (db *DB) SchemaName() string {
	if db == nil || db.Schema == nil {
		return ""
	}

	return db.Schema.Name()
}

func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	err := db.Schema.objSQL.Close()
	if err != nil {
		return err
	}
	err = db.Schema.objSQLTable.Close()
	if err != nil {
		return err
	}
	return db.DB.Close()
}

func (db *DB) Init() error {
	// will setup up the defaultSchema and prepared value
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()
	if db.Schema != nil {
		return nil
	}
	stmt, err := db.DB.Prepare(sqliteObjSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare sql: %w", err)
	}
	stmtTable, err := db.DB.Prepare(sqliteObjSQLForTable)
	if err != nil {
		return fmt.Errorf("failed to prepare table sql: %w", err)
	}
	db.Schema = &Schema{
		db:          db.DB,
		name:        "main",
		objSQL:      stmt,
		objSQLTable: stmtTable,
	}
	return nil
}

func (db *DB) Schemata() ([]schema.Schema, error) {
	// TODO(gdey): should use pragma list_database to figure out all the available schemata
	// For now we fake it and assume there is only one schema, the main one
	if db.Schema == nil {
		if err := db.Init(); err != nil {
			return nil, err
		}
	}
	return []schema.Schema{db.Schema}, nil
}

type Schema struct {
	db          database
	name        string
	objSQL      *sql.Stmt
	objSQLTable *sql.Stmt
}

type RowScanner interface {
	Scan(dest ...interface{}) error
}

type Object struct {
	Name      string
	TableName string
	Type      string
	SQL       string
}

func (obj Object) AsTable(s *Schema) Table {
	return Table{
		schema: s,
		name:   obj.TableName,
		sql:    obj.SQL,
	}
}
func (obj Object) AsView(s *Schema) View {
	return View{
		schema: s,
		name:   obj.TableName,
		sql:    obj.SQL,
	}
}
func (obj Object) AsTrigger(s *Schema) Trigger {
	return Trigger{
		schema:    s,
		name:      obj.Name,
		tableName: obj.TableName,
		sql:       obj.SQL,
	}
}

func (obj Object) AsIndex(s *Schema) Index {
	return Index{
		schema:    s,
		name:      obj.Name,
		tableName: obj.TableName,
	}
}

func scanSqlObj(row RowScanner) (obj Object, err error) {
	var objSQL *string
	err = row.Scan(&obj.Type, &obj.Name, &obj.TableName, &objSQL)
	if objSQL != nil {
		*objSQL = strings.TrimSpace(*objSQL)
		if *objSQL != "" {
			if !strings.HasSuffix(*objSQL, ";") {
				*objSQL = *objSQL + ";"
			}
			obj.SQL = *objSQL
		}
	}
	return obj, err
}

func (s Schema) Name() string { return s.name }
func (s Schema) Tables() (tables []schema.Table, err error) {
	// need to get all the tables for the schema.
	rows, err := s.objSQL.Query(ObjectTypeTable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		obj, err := scanSqlObj(rows)
		if err != nil {
			return nil, err
		}
		tables = append(tables, obj.AsTable(&s))
	}
	return tables, nil
}
func (s Schema) Views() (views []schema.View, err error) {
	// need to get all the tables for the schema.
	rows, err := s.objSQL.Query(ObjectTypeView)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		obj, err := scanSqlObj(rows)
		if err != nil {
			return nil, err
		}
		views = append(views, obj.AsView(&s))
	}
	return views, nil
}
func (s Schema) Triggers() (triggers []schema.Trigger, err error) {
	// need to get all the tables for the schema.
	rows, err := s.objSQL.Query(ObjectTypeTrigger)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		obj, err := scanSqlObj(rows)
		if err != nil {
			return nil, err
		}
		triggers = append(triggers, obj.AsTrigger(&s))
	}
	return triggers, nil
}

func (s Schema) Indexes() (indexes []schema.Index, err error) {
	// need to get all the tables for the schema.
	rows, err := s.objSQL.Query(ObjectTypeIndex)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		obj, err := scanSqlObj(rows)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, obj.AsIndex(&s))
	}
	return indexes, nil
}

type Table struct {
	name   string
	sql    string
	schema *Schema
}

func (tbl Table) Temporary() bool { return false }
func (tbl Table) Name() string    { return tbl.name }
func (tbl Table) SQL() string     { return tbl.sql }
func (tbl Table) Triggers() (triggers []schema.Trigger, err error) {
	rows, err := tbl.schema.objSQLTable.Query(ObjectTypeTrigger, tbl.name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		obj, err := scanSqlObj(rows)
		if err != nil {
			return nil, err
		}
		triggers = append(triggers, obj.AsTrigger(tbl.schema))
	}
	return triggers, nil
}
func (tbl Table) Indexes() (indexes []schema.Index, err error) {
	rows, err := tbl.schema.objSQLTable.Query(ObjectTypeIndex, tbl.name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		obj, err := scanSqlObj(rows)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, obj.AsIndex(tbl.schema))
	}
	return indexes, nil
}
func (tbl Table) Columns() (cols []schema.Column, err error) {
	const columnsSQL = `pragma table_xinfo( %v );`
	db := tbl.schema.db
	rows, err := db.Query(fmt.Sprintf(columnsSQL, tbl.name))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid     int
			name    string
			typ     string
			notNull bool
			dValue  interface{}
			pk      int
			hidden  bool
		)
		if err = rows.Scan(&cid, &name, &typ, &notNull, &dValue, &pk, &hidden); err != nil {
			return nil, err
		}
		cols = append(cols, Column{
			index:        cid,
			name:         name,
			typ:          typ,
			primary:      pk,
			null:         !notNull,
			defaultValue: dValue,
			hidden:       hidden,
			schema:       tbl.schema,
		})
	}
	return cols, nil
}

func (tbl Table) ForeignKeys() (keys []schema.ForeignKey, err error) {
	const columnsSQL = `pragma foreign_key_list(%v);`
	db := tbl.schema.db
	rows, err := db.Query(fmt.Sprintf(columnsSQL, tbl.name))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id       int
			seq      int
			table    string
			from     string
			to       string
			onUpdate string
			onDelete string
			match    string
		)
		if err = rows.Scan(&id, &seq, &table, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return nil, err
		}
		keys = append(keys, ForeignKey{
			id:        id,
			seq:       seq,
			fromTable: tbl.name,
			from:      from,
			toTable:   table,
			to:        to,
			onUpdate:  onUpdate,
			onDelete:  onDelete,
			match:     match,
		})
	}
	return keys, nil
}

type Column struct {
	index        int
	name         string
	typ          string
	primary      int
	null         bool
	defaultValue interface{}
	hidden       bool
	schema       *Schema
}

func (col Column) Index() int   { return col.index }
func (col Column) Name() string { return col.name }
func (col Column) Type() string { return col.typ }
func (col Column) GoType() interface{} {
	switch strings.ToUpper(col.typ) {
	case "TEXT", "BLOB":
		return ""
	case "NULL":
		return nil
	case "REAL":
		return 1.0
	case "INTEGER":
		return 1
	case "BOOL", "BOOLEAN":
		return true
	case "DATETIME", "DATE":
		return time.Time{}
	default:
		return ""
	}
}
func (col Column) IsPrimary() (bool, int)        { return col.primary != 0, col.primary }
func (col Column) Nullable() bool                { return col.null }
func (col Column) Default() (interface{}, error) { return col.defaultValue, nil }
func (col Column) Hidden() bool                  { return col.hidden }

type Trigger struct {
	schema    *Schema
	name      string
	tableName string
	sql       string
}

func (t Trigger) Name() string  { return t.name }
func (t Trigger) Table() string { return t.tableName }
func (t Trigger) SQL() string   { return t.sql }

type ForeignKey struct {
	id        int
	seq       int
	fromTable string
	from      string
	toTable   string
	to        string
	onUpdate  string
	onDelete  string
	match     string
}

func (key ForeignKey) ID() int            { return key.id }
func (key ForeignKey) Seq() int           { return key.seq }
func (key ForeignKey) FromTable() string  { return key.fromTable }
func (key ForeignKey) FromColumn() string { return key.from }
func (key ForeignKey) ToTable() string    { return key.toTable }
func (key ForeignKey) ToColumn() string   { return key.to }
func (key ForeignKey) OnUpdate() string   { return key.onUpdate }
func (key ForeignKey) OnDelete() string   { return key.onDelete }
func (key ForeignKey) Match() string      { return key.match }

type View struct {
	name   string
	sql    string
	schema *Schema
}

func (view View) Name() string { return view.name }
func (view View) SQL() string  { return view.sql }
func (view View) Columns() (cols []schema.Column, err error) {
	const columnsSQL = `pragma table_xinfo( %v );`
	db := view.schema.db
	rows, err := db.Query(fmt.Sprintf(columnsSQL, view.name))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid     int
			name    string
			typ     string
			notNull bool
			dValue  interface{}
			pk      int
			hidden  bool
		)
		if err = rows.Scan(&cid, &name, &typ, &notNull, &dValue, &pk, &hidden); err != nil {
			return nil, err
		}
		cols = append(cols, Column{
			index:        cid,
			name:         name,
			typ:          typ,
			primary:      pk,
			null:         !notNull,
			defaultValue: dValue,
			hidden:       hidden,
			schema:       view.schema,
		})
	}
	return cols, nil
}

type Index struct {
	name      string
	tableName string
	schema    *Schema
}

func (index Index) Name() string  { return index.name }
func (index Index) Table() string { return index.tableName }
func (index Index) Columns() (cols []schema.Column, err error) {
	db := index.schema.db

	// first we need to get the list of columns that make up this index
	// TODO(gdey): should we use the index_xinfo to get more info about the index columns
	// REF: https://www.sqlite.org/pragma.html#pragma_index_xinfo
	var indexColsSQL = fmt.Sprintf(`pragma index_info( %v )`, index.name)
	rows, err := db.Query(indexColsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var colNums []int
	for rows.Next() {
		var (
			seq  int // ignored
			cid  int
			name string // ignored
		)
		if err = rows.Scan(&seq, &cid, &name); err != nil {
			return nil, err
		}
		if cid == -1 {
			continue
		}
		colNums = append(colNums, cid)
	}
	cols = make([]schema.Column, len(colNums))

	// we first have to get the table info;
	const columnsSQL = `pragma table_xinfo( %v );`
	rows, err = db.Query(fmt.Sprintf(columnsSQL, index.tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid     int
			name    string
			typ     string
			notNull bool
			dValue  interface{}
			pk      int
			hidden  bool
		)
		if err = rows.Scan(&cid, &name, &typ, &notNull, &dValue, &pk, &hidden); err != nil {
			return nil, err
		}
		// we need to find were we are going to place this column
		for i := range colNums {
			if colNums[i] != cid {
				continue
			}
			// found it
			cols[i] = Column{
				index:        cid,
				name:         name,
				typ:          typ,
				primary:      pk,
				null:         !notNull,
				defaultValue: dValue,
				hidden:       hidden,
				schema:       index.schema,
			}
		}
	}
	return cols, nil
}

// Compile Check
var _ = schema.Database(new(DB))
