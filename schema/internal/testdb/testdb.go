package testdb

import (
	"database/sql"
	"fmt"
	migration "github.com/gdey/sqlite-migration"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	// we only work with sqlite
	_ "github.com/mattn/go-sqlite3"
)

// --------------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------------

// NewFilename will generate a new file that can be used for the database, and provide a cleanup function to remove
// the file after it's no longer needed.
// cleanup can be passed; which will tell the cleanup functions weather or not to delete the file. This is handy for
// debugging. Where you want a failed to test to print out the file name, so that the developer can inspect it later.
func NewFilename(t *testing.T, cleanup *bool) (string, func()) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "testdb-")
	if err != nil {
		t.Fatalf("Cannot create tempory file: %v", err)
		return "", nil
	}

	return tmpFile.Name(), func() {
		// default is to cleanup; however one can pass in a variable that can be set
		// to prevent cleanup
		if cleanup == nil || *cleanup {
			// Remember to clean up the file afterwards
			_ = os.Remove(tmpFile.Name())
		} else {
			t.Logf("%s : db file is: %v", t.Name(), tmpFile.Name())
		}
	}
}

// ApplyMigrations will apply the migrations to the given db.
// If corpus does not start with a "/" then base will be prepended to it. Otherwise
// the corpus will be used as is.
func ApplyMigrations(db *sql.DB, base string, corpora ...string) (err error) {
	author := "test"
	for i, corpus := range corpora {
		if corpus == "" {
			continue
		}
		// Only add base to the the corpus if it does not start with "/"
		if base != "" && !strings.HasPrefix(corpus, "/") {
			corpus = filepath.Join(base, corpus)
		}
		corpus = filepath.Join("testdata", corpus)

		// we need to stage the db with testing data
		testDatum := migration.New(corpus, fmt.Sprintf("test_data_%03d", i), nil)
		_, _, err = testDatum.Init(db, author)
		if err != nil {
			return fmt.Errorf("db failed to load test data `%v` : %w", corpus, err)
		}
	}
	return nil
}

// InitFilename will init the sqlite3 database with the corpus migrations applied to it.
// If corpus does not start with a "/" then base will be prepended to it. Otherwise
// the corpus will be used as is.
func InitFilename(filename, base string, corpora ...string) (db *sql.DB, err error) {

	// First let's create a new db and apply stage to it.
	db, err = sql.Open("sqlite3", filename)
	if err != nil {
		return nil, fmt.Errorf("error opening %v : %w", filename, err)
	}

	// SQLite does not default to having foreign keys enabled.
	// this statement enables them
	_, err = db.Exec(`PRAGMA foreign_keys = ON;`)
	if err != nil {
		return nil, err
	}
	if err = ApplyMigrations(db, base, corpora...); err != nil {
		return nil, err
	}
	return db, nil
}

// New will create a new database with the corpus migrations applied to it.
// If corpus does not start with a "/" then base will be prepended to it. Otherwise
// the corpus will be used as is.
func New(t *testing.T, shouldCleanup *bool, base string, corpora ...string) (name string, cleanup func(), db *sql.DB) {
	t.Helper()

	name, cleanup = NewFilename(t, shouldCleanup)
	db, err := InitFilename(name, base, corpora...)
	if err != nil {
		t.Error(err.Error())
		cleanup()
		return "", nil, nil
	}
	return name, cleanup, db
}
