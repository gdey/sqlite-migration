package sqlite_test

import (
	"github.com/gdey/sqlite-migration/schema/internal/testdb"
	"github.com/gdey/sqlite-migration/schema/sqlite"
	"log"
	"testing"
)

func TestSqliteSchema(t *testing.T) {
	shouldCleanUp := true
	// for now the sqlite will only have one schema, the main schema.
	filename, cleanup, tdb := testdb.New(t, &shouldCleanUp, "schema", "initial")
	// we only care about the filename. So, we are going to close the db.
	tdb.Close()
	defer cleanup()
	db, err := sqlite.New(filename)
	if err != nil {
		shouldCleanUp = false
		t.Fatalf("new error, expecting nil, got '%v'", err)
		return
	}
	schemata, err := db.Schemata()
	if err != nil {
		shouldCleanUp = false
		t.Fatalf("schemata error, expecting nil got %v", err)
		return
	}
	if len(schemata) != 1 {
		t.Errorf("num, expected 1, got %v", len(schemata))
		shouldCleanUp = false
		return
	}
	if schemata[0].Name() != "main" {
		shouldCleanUp = false
		t.Errorf("schema name, expected 'main' got '%v'", schemata[0].Name())
		return
	}
	tables, err := schemata[0].Tables()
	if err != nil {
		shouldCleanUp = false
		t.Fatalf("schemata tables error, expecting nil got %v", err)
		return
	}
	if len(tables) != 4 {
		shouldCleanUp = false
		t.Fatalf("number of tables, expected 4, got %v", len(tables))
		return
	}
	for i, table := range tables {
		log.Printf("Table[%v]: %v", i, table.Name())
		cols, err := table.Columns()
		if err != nil {
			shouldCleanUp = false
			t.Fatalf("[%v] columns error, expected nil, got %v", i, err)
		}
		log.Printf("[%v] Columns(%v)", i, len(cols))
		for j, col := range cols {
			log.Printf("%v:%+v", j, col)
		}
		triggers, err := table.Triggers()
		if err != nil {
			shouldCleanUp = false
			t.Fatalf("[%v] triggers error, expected nil, got %v", i, err)
		}
		log.Printf("[%v] Triggers(%v)", i, len(triggers))
		for j, trigger := range triggers {
			log.Printf("%v: name:%v", j, trigger.Name())
		}

	}
	triggers, err := schemata[0].Triggers()
	if err != nil {
		t.Fatalf("schemata triggers error, expecting nil got %v", err)
		return
	}
	for i, trigger := range triggers {
		log.Printf("%v: name:%v, table: %v", i, trigger.Name(), trigger.Table())
	}
}
