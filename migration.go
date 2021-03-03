package migration

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"
	"unicode/utf8"
)

const (
	// InitialVersion represents the value of the very first version, an empty database.
	InitialVersion = ""
)

type osFS struct{}

func (osFS) Open(name string) (fs.File, error) { return os.Open(name) }

type FSOpener = fs.FS

type Logger interface {
	Printf(format string, v ...interface{})
}

type nulLogger struct{}

func (nulLogger) Printf(_ string, _ ...interface{}) { return }

// Manager will manage a set of migration file and apply them to the database
type Manager struct {
	tblName string
	dir     string
	fs      FSOpener
	log     Logger
}

func (mng *Manager) FS() FSOpener {
	if mng == nil || mng.fs == nil {
		mng.fs = osFS{}
	}
	return mng.fs
}
func (mng *Manager) Log() Logger {
	if mng == nil || mng.log == nil {
		mng.log = nulLogger{}
	}
	return mng.log
}

func (mng *Manager) SetLog(l Logger) {
	if mng == nil {
		return
	}
	mng.log = l
}

func (mng *Manager) VersionFile() string {
	var dir = "migrations"
	if mng != nil {
		dir = mng.dir
	}
	return filepath.Join(dir, "sequence.txt")
}

func (mng *Manager) Versions() ([]string, error) {
	f, err := mng.FS().Open(mng.VersionFile())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return getVersionsFromFile(f), nil
}

func (mng *Manager) LatestVersion() (string, error) {
	v, err := mng.Versions()
	if err != nil {
		return "", err
	}
	return v[len(v)-1], nil
}

func (mng *Manager) TableName() string {
	if mng == nil || mng.tblName == "" {
		return "dbfile_migrations"
	}
	return mng.tblName
}

func (mng *Manager) HasTrackingTable(db *sql.DB) bool {
	const (
		CountMigrationTableSQL = `
SELECT COUNT(*)
FROM sqlite_master
WHERE tbl_name = '%[1]s';
`
	)
	count := 0

	// let's check to see if our tables are there
	sqlQuery := fmt.Sprintf(CountMigrationTableSQL, mng.TableName())
	err := db.QueryRow(sqlQuery).Scan(&count)
	if err != nil {
		mng.Log().Printf("error running SQL\n%v\n%v", sqlQuery, err)
		return false
	}
	return count != 0
}

// Init will insure that the initial tables for the file
// correctly initialized, it returns the current database version
func (mng *Manager) Init(db *sql.DB, author string) (ver string, didInit bool, err error) {

	const (
		// MigrationsTableCreateSQL is used to create the basic table used to manage sql migrations
		MigrationsTableCreateSQL = `
CREATE TABLE IF NOT EXISTS %[1]s (
	  file_path    TEXT NOT NULL
	, file_hash    TEXT NOT NULL
	, created_at   TEXT NOT NULL
	, author       TEXT NOT NULL
	, duration     INTEGER NOT NULL	-- in seconds
);
	`
	)

	if db == nil {
		panic("db is nil")
	}

	if mng.HasTrackingTable(db) {
		// Tracking table exists
		// get the current version of the db from the table
		ver, err := mng.DBVersion(db)
		return ver, false, err
	}

	// The tracking tables don't exist
	// We need to add them.
	sqlQuery := fmt.Sprintf(MigrationsTableCreateSQL, mng.TableName())
	if _, err = db.Exec(sqlQuery); err != nil {
		mng.Log().Printf("Error running sql:\n%s", sqlQuery)
		return "", false, fmt.Errorf("failed to create tables : %w", err)
	}
	dbVersion, err := mng.addTrackingEntry(db, author, InitialVersion)
	if err != nil {
		return "", false, err
	}
	return dbVersion, true, nil

}

// Upgrade will upgrade the db file to the latest schema
func (mng *Manager) Upgrade(db *sql.DB, author string) (startingVersion string, newVersion string, err error) {

	var didInit bool
	// insure the database is correctly initialized
	startingVersion, didInit, err = mng.Init(db, author)
	if err != nil {
		return startingVersion, "", err
	}
	// we init the db, which means it's a new database, let's return "" for starting version
	// Upgrade to the latest version
	newVersion, err = mng.addTrackingEntry(db, author, startingVersion)
	if didInit {
		return "", newVersion, err
	}
	return startingVersion, newVersion, err
}

// DBVersion returns the version of migration in the given db
func (mng *Manager) DBVersion(db *sql.DB) (string, error) {
	const (
		SelectLatestVersionSQL = `
	SELECT file_path AS file
	FROM %s
	ORDER by ROWID desc
	LIMIT 1;
	`
	)
	var (
		selectSQL = fmt.Sprintf(SelectLatestVersionSQL, mng.tblName)
		dbVersion string
		err       = db.QueryRow(selectSQL).Scan(&dbVersion)
	)
	if errors.Is(err, sql.ErrNoRows) {
		return InitialVersion, nil
	}
	return dbVersion, err
}

// addTrackingEntry will add the sql file management entries into the lis_migration table
func (mng *Manager) addTrackingEntry(db *sql.DB, author, initialVersion string) (string, error) {

	const (
		InsertMigrationSQL = `
	INSERT INTO %s (file_path,file_hash, author,duration,created_at)
	VALUES (?,?,?,?,datetime('now'));
	`
	)

	versions, err := mng.Versions()
	if err != nil {
		return "", err
	}

	var (
		i       int
		version string
	)

	for i, version = range versions {
		if version == initialVersion {
			break
		}
	}

	// Schema already up to date.
	if len(versions) == i+1 {
		// do nothing.
		return initialVersion, nil
	}

	i++ // move to the next version

	// get the max length of the versions
	var maxLength = utf8.RuneCountInString(versions[i])
	for j := i + 1; j < len(versions); j++ {
		if len(versions[j]) > maxLength {
			maxLength = utf8.RuneCountInString(versions[j])
		}
	}

	// Now we need to apply the remaining versions to the database
	for ; i < len(versions); i++ {
		startT := time.Now()
		migrationFilename := filepath.Join(mng.dir, versions[i])
		hash, err := mng.applySQLFile(db, migrationFilename)
		if err != nil {
			return InitialVersion, fmt.Errorf("error applying SQL file: %v : %w", migrationFilename, err)
		}
		duration := time.Now().Sub(startT).Seconds()
		sqlQuery := fmt.Sprintf(InsertMigrationSQL, mng.tblName)
		_, err = db.Exec(sqlQuery,
			versions[i],
			hash,
			author,
			duration,
		)
		if err != nil {
			mng.Log().Printf("Error running sqlQuery:\n%s", sqlQuery)
			return "", fmt.Errorf("error inserting tracking info: %w", err)
		}
		mng.Log().Printf("SQL file %-*s took %3.5fs to apply", maxLength, versions[i], duration)
	}

	return versions[i-1], nil

}

func renderSQLTPL(filename string, body []byte, tplFuncMap template.FuncMap) ([]byte, error) {

	tmpl, err := template.New(filename).
		Delims("--{{", "}}--").
		Funcs(tplFuncMap).
		Parse(string(body))
	if err != nil {
		return []byte{}, fmt.Errorf("error parsing template %v: %w", filename, err)
	}

	h := sha1.New()
	h.Write(body)
	sum := h.Sum(nil)
	sha1Hash := fmt.Sprintf("{sha1}%x", sum)

	var sqlBody bytes.Buffer

	err = tmpl.Execute(&sqlBody, struct {
		Filename string
		Sha1Hash string
	}{Filename: filename, Sha1Hash: sha1Hash})
	if err != nil {
		return []byte{}, fmt.Errorf("error executing template %v: %v", filename, err)
	}
	return sqlBody.Bytes(), nil
}

// loadPartial will load the partial from the "partial" directory in the
// mng.dir dir, and return the contents it.
func (mng *Manager) loadPartial(name string) (string, error) {
	filename := filepath.Join(mng.dir, "partial", name)
	body, err := mng.readAllFile(filename)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (mng *Manager) FuncMap() template.FuncMap {
	return template.FuncMap{
		// The name "title" is what the function will be called in the template text.
		"title":   strings.Title,
		"args":    func(args ...interface{}) []interface{} { return args },
		"partial": mng.loadPartial,
	}
}

func (mng *Manager) readAllFile(filename string) ([]byte, error) {
	f, err := mng.FS().Open(filename)
	if err != nil {
		return []byte{}, fmt.Errorf("error opening file %v : %w", filename, err)
	}
	defer f.Close()

	body, err := ioutil.ReadAll(f)
	if err != nil {
		return []byte{}, fmt.Errorf("error reading file %v : %w", filename, err)
	}
	return body, nil
}

// applySQLFile will apply the given sql file to the db file
func (mng *Manager) applySQLFile(db *sql.DB, filename string) (string, error) {

	body, err := mng.readAllFile(filename)
	if err != nil {
		return "", err
	}

	// check to see if the filename is a template
	if strings.HasSuffix(filename, "tpl") {
		// we are going to treat the body as a template.
		if body, err = renderSQLTPL(filename, body, mng.FuncMap()); err != nil {
			return "", err
		}
	}

	h := sha1.New()
	h.Write(body)
	sum := h.Sum(nil)
	sha1Hash := fmt.Sprintf("{sha1}%x", sum)

	_, err = db.Exec(string(body))
	if err != nil {
		mng.Log().Printf("Error running sql:\n%s", body)
		return "", fmt.Errorf("error running sql: %v : %w", filename, err)
	}

	return sha1Hash, nil

}

// New returns a new manager
func New(dir, tableName string, fs FSOpener) *Manager {
	return &Manager{
		tblName: tableName,
		dir:     dir,
		fs:      fs,
		log:     nulLogger{},
	}
}

// getVersionsFromFile will split the provided file by newlines, skipping empty lines, and
// lines that begin with Octothorpe(#) and returning the entries in order.
//
// **Note**: Extra spaces at the start or end of the line will be stripped before determining
// whether to keep the line or not.
func getVersionsFromFile(f io.Reader) []string {

	var (
		l = []string{InitialVersion}
	)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		l = append(l, line)
	}

	return l

}
