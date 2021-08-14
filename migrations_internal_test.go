package migration

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	// we only work with sqlite
	_ "github.com/mattn/go-sqlite3"
)

func copyFile(sourceFile, destinationFile string) error {
	input, err := os.ReadFile(sourceFile)
	if err != nil {
		return err
	}

	err = os.WriteFile(destinationFile, input, 0644)
	if err != nil {
		return err
	}
	return nil
}
func openDBCopy(src, dest string) (*sql.DB, error) {
	_ = os.Remove(dest)
	err := copyFile(src, dest)
	if err != nil {
		return nil, fmt.Errorf("failed to create copy of %v to %v: %w", src, dest, err)
	}
	db, err := sql.Open("sqlite3", dest)
	if err != nil {
		return nil, fmt.Errorf("error opening %v : %w", dest, err)
	}

	// SQLite does not default to having foreign keys enabled.
	// this statement enables them
	_, err = db.Exec(`PRAGMA foreign_keys = ON;`)
	if err != nil {
		return nil, fmt.Errorf("failed to enabled foreign key support: %w", err)
	}
	return db, nil
}

func NewTestDBFilename(t *testing.T, cleanup *bool) (string, func()) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "tempDB-")
	if err != nil {
		log.Fatal("Cannot create temporary file", err)
	}
	if cleanup == nil {
		// always cleanup
		return tmpFile.Name(), func() { _ = os.Remove(tmpFile.Name()) }
	}

	return tmpFile.Name(), func() {
		if *cleanup {
			// Remember to clean up the file afterwards
			_ = os.Remove(tmpFile.Name())
		} else {
			t.Logf("%s : db file is: %v", t.Name(), tmpFile.Name())
		}
	}
}

var (
	//go:embed testdata/*
	testdataFS embed.FS
)

func TestMigration_Update(t *testing.T) {

	type tcase struct {
		author   string
		dbName   string
		testDir  string
		genTable string

		Versions []string
		VerErr   error
		Start    string
		End      string
		Err      error
	}
	fn := func(tc tcase) func(t2 *testing.T) {
		return func(t *testing.T) {
			genTable := "gen_migrations"
			testDBName := "test.db"
			if tc.dbName != "" {
				testDBName = tc.dbName
			}
			if tc.genTable != "" {
				genTable = tc.genTable
			}
			migrations := New(
				filepath.Join("testdata", tc.testDir, "migrations"),
				genTable,
				testdataFS,
			)
			versions, err := migrations.Versions()
			if tc.VerErr != nil {
				if !errors.Is(err, tc.VerErr) {
					t.Errorf("versions error, expected %v got %v", tc.VerErr, err)
				}
				return
			}
			if err != nil {
				t.Errorf("versions error, expected nil got %v", err)
				return
			}
			if !reflect.DeepEqual(tc.Versions, versions) {
				t.Errorf("versions,\n\texpected %+v\n\t     got %+v", tc.Versions, versions)
				return
			}
			dbFilename, cleanup := NewTestDBFilename(t, nil)
			defer cleanup()
			db, err := openDBCopy(
				filepath.Join("testdata", tc.testDir, testDBName),
				dbFilename,
			)
			if err != nil {
				t.Errorf("openDBCopy error expected nil, got error: %v", err)
				return
			}

			author := "test"
			if tc.author != "" {
				author = tc.author
			}
			// for this one we will test all values instead of exiting after the first bad one
			start, end, err := migrations.Upgrade(db, author)
			if !errors.Is(err, tc.Err) {
				t.Errorf("upgrade err, expected %v got %v", tc.Err, err)
			}
			if start != tc.Start {
				t.Errorf("upgrade start, expected %v got %v", tc.Start, start)
			}
			if end != tc.End {
				t.Errorf("upgrade end, expected %v got %v", tc.End, end)
			}
		}
	}
	tests := map[string]tcase{
		"upgrade_issue": {
			Versions: []string{"", "simpletable.sql", "simple_table2.sql"},
			Err:      ErrUnknownVersion("simpletable2.sql"),
			Start:    "simpletable2.sql",
			End:      "simpletable2.sql",
		},
		"upgrade_simple": {
			Versions: []string{"", "simpletable.sql", "simple_table2.sql"},
			Start:    "simpletable.sql",
			End:      "simple_table2.sql",
		},
		"upgrade_none": {
			Versions: []string{"", "simpletable.sql"},
			Start:    "simpletable.sql",
			End:      "simpletable.sql",
		},
	}
	for name, tc := range tests {
		if tc.testDir == "" {
			tc.testDir = name
		}
		t.Run(name, fn(tc))
	}

}
