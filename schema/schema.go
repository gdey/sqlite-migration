// Package schema provide a general interface to obtain schema information from a database
package schema

import "database/sql"

type Databaser interface {
	Database() *sql.DB
}

type Namer interface {
	Name() string
}

type SQLer interface {
	// SQL to create the object
	SQL() string
}

type NamedSQLer interface {
	Namer
	SQLer
}

// Database describes a generic database, an database provider could have additional functions
type Database interface {
	Databaser
	Namer

	// Schemata returns all the schema's in the database
	Schemata() ([]Schema, error)
	// SchemaName is the name of the default schema
	SchemaName() string
	// Schema interface functions other then Name should be for the default schema.
	Schema
}

// Schema contains the tables, views, triggers and indexes of the database
type Schema interface {
	Namer
	// Tables returns the tables in the schema
	Tables() ([]Table, error)
	// Views returns the views in the schema
	Views() ([]View, error)
	Indexes() ([]Index, error)
	Triggers() ([]Trigger, error)
}

// Table describes a table in the database
type Table interface {
	NamedSQLer
	// Columns in the table
	Columns() ([]Column, error)
	// Temporary table or not
	Temporary() bool
	ForeignKeys() ([]ForeignKey, error)
	// Triggers on this table
	Triggers() ([]Trigger, error)
	Indexes() ([]Index, error)
}

// Column describes a Column in a table
type Column interface {
	Namer
	Index() int
	Type() string
	// GoType should be the equivalent go type
	GoType() interface{}
	// IsPrimary return true if the column is part of the Primary Key, with int being the index. Otherwise, bool will be false, and int will be -1
	IsPrimary() (bool, int)
	Nullable() bool
	Default() (interface{}, error)
	Hidden() bool
}

type View interface {
	NamedSQLer
	Columns() ([]Column, error)
}

type Trigger interface {
	NamedSQLer
	Table() string
}

type Index interface {
	Namer
	Table() string
	Columns() ([]Column, error)
}

type ForeignKey interface {
	// ID is the ForeignKey ID
	ID() int
	// Seq is the seq id for multi-key references. Will always start at Zero
	Seq() int
	FromTable() string
	ToTable() string
	FromColumn() string
	ToColumn() string
	OnUpdate() string
	OnDelete() string
	Match() string
}
