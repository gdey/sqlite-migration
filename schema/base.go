package schema

// NullDatabase can be embedded into another Struct to get some basic functions to satisfy the Database interface
type NullDatabase struct {
}

func (db NullDatabase) Schemata() ([]Schema, error) { return nil, nil }
func (db NullDatabase) SchemaName() string          { return "" }

// NullSchema can be embedded into another Struct to get some basic functions to satisfy the Schema interface
type NullSchema struct{}

func (schema NullSchema) Tables() ([]Table, error)     { return nil, nil }
func (schema NullSchema) Views() ([]View, error)       { return nil, nil }
func (schema NullSchema) Indexes() ([]Index, error)    { return nil, nil }
func (schema NullSchema) Triggers() ([]Trigger, error) { return nil, nil }

type NullTable struct {
	NullColumns
	NullForeignKeys
}

type NullForeignKey struct{}

func (ForeignKey NullForeignKey) ID() int          { return 0 }
func (foreignKey NullForeignKey) OnUpdate() string { return "" }
func (foreignKey NullForeignKey) OnDelete() string { return "" }
func (foreignKey NullForeignKey) Match() string    { return "NONE" }

type NullColumns struct{}

func (col NullColumns) Columns() ([]Column, error) { return nil, nil }

type NullForeignKeys struct{}

func (foreignKeys NullForeignKeys) ForeignKeys() ([]ForeignKey, error) { return nil, nil }
