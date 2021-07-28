package cmd

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/gdey/sqlite-migration/cmd/migration/cmd/genschema"
	"github.com/gdey/sqlite-migration/schema"
	"github.com/gdey/sqlite-migration/schema/sqlite"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/gdey/sqlite-migration/generator"

	"github.com/spf13/cobra"
)

var (
	useSingleFile  bool
	dataOnly       bool
	tableNameFile  = "table_order.txt"
	databaseSchema = generator.DefaultSchema

	generateSchemaCmd = func() *cobra.Command {
		cmd := &cobra.Command{
			Use:   "generate-schema [dir|filename]",
			Short: "generate-schema will generate a set of sql files based on the db",
			Long: `generate-schema will generate a set of sql files based on the given db

Though by default it will generate a set of sql files to the given directory. If a file is 
provided (or "--single-file" is set), then a single composite sql file will be generated instead. All files will
be overwritten.

`,
			Run: runGenerateSchemaCmd,
		}

		cmd.Flags().BoolVarP(&dataOnly, "data-only", "d", false, "Do not generate the Create statements for tables, views, etc..")
		cmd.Flags().BoolVar(&useSingleFile, "single-file", false, "generate only one file containing all SQL statements")
		cmd.Flags().StringVar(&tableNameFile, "table-name", filepath.Join(migrationPath, "table_order.txt"), "the file to specify the order in which to write the tables out.")
		cmd.Flags().StringVar(&databaseSchema, "database-schema", generator.DefaultSchema, "The database schema to dump")

		rootCmd.AddCommand(cmd)
		return cmd
	}()

	_ = generateSchemaCmd
)

func generatorOutput(cmd *cobra.Command, f string) (filename string, isDir bool, err error) {
	theLogger := getLogger(cmd)
	fileInfo, err := os.Stat(f)

	switch {
	case err == nil:
		// File/Dir exists need to see which one it it.
		theLogger.Printf("%v exists ignoring single single-file argument", filename)
		return f, fileInfo.IsDir(), nil
	default: // err != nil
		var pathErr = new(os.PathError)
		if errors.As(err, &pathErr) {
			// file does not exists.
			theLogger.Printf("%v does not exists, using single-file argument", filename)
			return f, !useSingleFile, nil
		}
		return f, !useSingleFile, err
	}
}

type TableObjectDescriptor struct {
	writtenOut bool
	dependsOn  []string
	excluded   bool
	table      schema.Table
	view       schema.View
	order      uint32
}

func (obj *TableObjectDescriptor) writeSchemaSQL(log *log.Logger, db *sql.DB, file *os.File, writeHeader, dataOnly bool, skipFields []string) (result map[string]bool, err error) {
	var (
		name    string
		sqlText string
		objType string
		isTable bool
	)
	if obj == nil {
		return nil, nil
	}
	if obj.table != nil {
		name = obj.table.Name()
		sqlText = obj.table.SQL()
		objType = sqlite.ObjectTypeTable
		isTable = true
	} else if obj.view != nil {
		name = obj.view.Name()
		sqlText = obj.view.SQL()
		objType = sqlite.ObjectTypeView
	} else {
		return nil, nil
	}
	result = make(map[string]bool)
	log.Printf("Writing SQL for %v to %v", name, file.Name())
	if writeHeader {
		_, _ = file.WriteString(fmt.Sprintf(`

-- -------------------------------------------------------------------------- --
-- %74s --
-- -------------------------------------------------------------------------- --

`, name))

	}
	if !dataOnly {
		// write out the SQL's
		result[objType] = true
		if err = genschema.WriteTableSQL(file, sqlText); err != nil {
			return result, err
		}
		// if it is a table, we should write out the triggers of that table.
		if isTable {
			triggers, _ := obj.table.Triggers()
			if len(triggers) > 0 {
				result[sqlite.ObjectTypeTrigger] = true
			}
			for i := range triggers {
				if err = genschema.WriteTableSQL(file, triggers[i].SQL()); err != nil {
					return result, err
				}
			}
		}
	}
	insertSQL := generator.InsertSQL(db, name, skipFields...)
	if len(insertSQL) != 0 {
		result["data"] = true
		if _, err = file.Write(insertSQL); err != nil {
			return result, fmt.Errorf("failed to write insert sql for %v %v: %w", objType, name, err)
		}
	}
	obj.writtenOut = true
	return result, nil
}

type tableWriter struct {
	excludedFields    [][]string
	orderedTableNames []string
	isSingleFile      bool
	dataOnly          bool
	file              *os.File
	dMap              map[string]map[string]bool
	tableMap          map[string]TableObjectDescriptor
	order             uint32
}

func (w *tableWriter) OrderedTableNames() []string {
	// this will go through the table map finding the names in order
	var names = make([]string, 0, len(w.tableMap))
	for name := range w.tableMap {
		names = append(names, name)
	}
	sort.Sort(generator.Byer{
		LenFn:  func() int { return len(names) },
		SwapFn: func(i, j int) { names[i], names[j] = names[j], names[i] },
		LessFn: func(i, j int) bool {
			if w.tableMap[names[i]].order == w.tableMap[names[j]].order {
				return names[i] < names[j]
			}
			return w.tableMap[names[i]].order < w.tableMap[names[j]].order
		},
	})
	return names
}

func (w *tableWriter) WriteTable(log *log.Logger, db *sql.DB, outputPath, name string) (err error) {
	tblDesc, ok := w.tableMap[name]
	if !ok {
		return fmt.Errorf("unknown table/view `%v`\nknown tables/views: %v", name, strings.Join(w.KnownTables(), ","))
	}
	if tblDesc.writtenOut {
		return nil
	}
	for _, dependedOnTable := range tblDesc.dependsOn {
		if dependedOnTable == name {
			// we only want depends that are not us.
			continue
		}
		if err = w.WriteTable(log, db, outputPath, dependedOnTable); err != nil {
			return err
		}
	}
	file := w.file
	if !w.isSingleFile {
		file, err = genschema.FileFor(outputPath, name)
		if err != nil {
			return fmt.Errorf("failed to open file for table %v for write: %w", name, err)
		}
		defer file.Close()
	}
	w.dMap[name], err = tblDesc.writeSchemaSQL(log, db, file, w.isSingleFile, w.dataOnly, w.excludedFieldsFor(name))
	if err != nil {
		return fmt.Errorf("failed to write schema(%v): %w", name, err)
	}
	tblDesc.writtenOut = true
	tblDesc.order = atomic.AddUint32(&w.order, 1)
	w.tableMap[name] = tblDesc

	return nil
}
func (w *tableWriter) WriteReadMe(outputPath string) error {
	if w == nil || w.isSingleFile {
		return nil
	}
	return genschema.WriteReadME(outputPath, w.OrderedTableNames(), w.dMap)
}

func (w *tableWriter) excludedFieldsFor(name string) []string {
	if w.excludedFields == nil {
		return make([]string, 0)
	}
	for i, tName := range w.orderedTableNames {
		if name == tName {
			if w.excludedFields[i] != nil {
				return w.excludedFields[i]
			}
		}
	}
	return make([]string, 0)
}
func (w *tableWriter) KnownTables() (names []string) {
	if w == nil || w.tableMap == nil {
		return []string{}
	}
	for name := range w.tableMap {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func runGenerateSchemaCmd(cmd *cobra.Command, args []string) {

	// check to see if the db file exists.
	var (
		theLogger         = getLogger(cmd)
		singleFile        *os.File
		orderedTableNames []string
		excludedFields    [][]string
	)

	if len(args) == 0 {
		theLogger.Printf("output file/dir is required.")
		os.Exit(ExitCodeOutputPath)
	}
	outputPath, isOutputDir, err := generatorOutput(cmd, args[0])
	if err != nil {
		theLogger.Printf("output file/dir error: %v", err)
		os.Exit(ExitCodeOutputPath)
	}

	if dbFilename == "" {
		theLogger.Print("database file must be given")
		os.Exit(ExitCodeDatabase)
	}

	db, err := sqlite.New(dbFilename)
	if err != nil {
		theLogger.Printf("error opening db %v: %v", dbFilename, err)
		os.Exit(ExitCodeDatabase)
	}

	if isOutputDir {
		_ = os.RemoveAll(outputPath)
		if err = os.MkdirAll(outputPath, 0777); err != nil {
			theLogger.Printf("output dir error: %v", err)
			os.Exit(ExitCodeOutputPath)
		}
		singleFile = nil
	} else {
		singleFile, err = os.Create(outputPath)
		if err != nil {
			theLogger.Printf("failed to create file %v : %v", outputPath, err)
			os.Exit(ExitCodeOutputPath)
		}
		_, _ = singleFile.WriteString(genschema.FileHeader)
		defer singleFile.Close()
	}
	if tableNameFile != "" {
		orderedTableNames, excludedFields, err = generator.GetOrderedTablesNamesFromFile(tableNameFile)
		if err != nil {
			theLogger.Printf("failed to read table names file %v : %v", tableNameFile, err)
		}
	}

	tables, _ := db.Tables()
	views, _ := db.Views()

	knownTables := make([]string, 0, len(tables))
	knownViews := make([]string, 0, len(views))
	tableMap := make(map[string]TableObjectDescriptor)

	for i, tbl := range tables {
		name := tbl.Name()
		knownTables = append(knownTables, name)
		// figure out the depends on.
		fKeys, _ := tbl.ForeignKeys()
		var dependsOn []string
		if len(fKeys) != 0 {
		nextKey:
			for _, fKey := range fKeys {
				toTbl := fKey.ToTable()
				for _, tblName := range dependsOn {
					if toTbl == tblName {
						continue nextKey
					}
				}
				dependsOn = append(dependsOn, toTbl)
			}
			sort.Strings(dependsOn)
		}
		tableMap[name] = TableObjectDescriptor{
			table:     tables[i],
			dependsOn: dependsOn,
		}
	}
	sort.Strings(knownTables)
	// let's build out the dependency graph of the tables

	for i := range views {
		name := views[i].Name()
		knownViews = append(knownViews, name)
		tableMap[name] = TableObjectDescriptor{
			view: views[i],
		}
	}
	sort.Strings(knownViews)

	if len(orderedTableNames) == 0 {
		orderedTableNames = append(knownTables, knownViews...)
	}

	tabWriter := tableWriter{
		dMap:              make(map[string]map[string]bool, len(orderedTableNames)),
		file:              singleFile,
		isSingleFile:      singleFile != nil,
		orderedTableNames: orderedTableNames,
		tableMap:          tableMap,
		dataOnly:          dataOnly,
		excludedFields:    excludedFields,
	}

	for _, name := range orderedTableNames {
		if err := tabWriter.WriteTable(theLogger, db.DB, outputPath, name); err != nil {
			theLogger.Printf(err.Error())
			os.Exit(ExitCodeDatabase)
		}
	}
	_ = tabWriter.WriteReadMe(outputPath)
	return
}
