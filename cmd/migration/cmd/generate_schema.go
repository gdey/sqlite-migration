package cmd

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gdey/sqlite-migration/cmd/migration/cmd/genschema"

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
	log := getLogger(cmd)
	fileInfo, err := os.Stat(f)

	switch {
	case err == nil:
		// File/Dir exists need to see which one it it.
		log.Printf("%v exists ignoring single single-file argument", filename)
		return f, fileInfo.IsDir(), nil
	default: // err != nil
		var pathErr = new(os.PathError)
		if errors.As(err, &pathErr) {
			// file does not exists.
			log.Printf("%v does not exists, using single-file argument", filename)
			return f, !useSingleFile, nil
		}
		return f, !useSingleFile, err
	}
}

func runGenerateSchemaCmd(cmd *cobra.Command, args []string) {

	// check to see if the db file exists.
	var (
		log               = getLogger(cmd)
		singleFile        *os.File
		orderedTableNames []string
		excludedFields    [][]string
	)

	if len(args) == 0 {
		log.Printf("output file/dir is required.")
		os.Exit(ExitCodeOutputPath)
	}
	outputPath, isOutputDir, err := generatorOutput(cmd, args[0])
	if err != nil {
		log.Printf("output file/dir error: %v", err)
		os.Exit(ExitCodeOutputPath)
	}

	if dbFilename == "" {
		log.Print("database file must be given")
		os.Exit(ExitCodeDatabase)
	}

	db, err := sql.Open("sqlite3", dbFilename)
	if err != nil {
		log.Printf("error opening db %v: %v", dbFilename, err)
		os.Exit(ExitCodeDatabase)
	}
	if isOutputDir {
		_ = os.RemoveAll(outputPath)
		if err = os.MkdirAll(outputPath, 0777); err != nil {
			log.Printf("output dir error: %v", err)
			os.Exit(ExitCodeOutputPath)
		}
		singleFile = nil
	} else {
		singleFile, err = os.Create(outputPath)
		if err != nil {
			log.Printf("failed to create file %v : %v", outputPath, err)
			os.Exit(ExitCodeOutputPath)
		}
		_, _ = singleFile.WriteString(genschema.FileHeader)
		defer singleFile.Close()
	}
	if tableNameFile != "" {
		orderedTableNames, excludedFields, err = generator.GetOrderedTablesNamesFromFile(tableNameFile)
		if err != nil {
			log.Printf("failed to read table names file %v : %v", tableNameFile, err)
		}
	}

	gen := generator.DB{DB: db}
	tables := gen.Tables()
	knownTables := make([]string, 0, len(tables))
	for name := range tables {
		knownTables = append(knownTables, name)
	}
	sort.Strings(knownTables)

	if len(orderedTableNames) == 0 {
		orderedTableNames = knownTables
	}

	var (
		file         = singleFile
		dMap         = make(map[string]map[string]bool, len(orderedTableNames))
		isSingleFile = singleFile != nil
	)

	for i, name := range orderedTableNames {
		tableDescriptors, ok := tables[name]
		if !ok {
			log.Printf("Unknown table `%v`\nknown tables: %v", name, strings.Join(knownTables, ","))
			os.Exit(ExitCodeDatabase)
		}

		if isSingleFile {
			_, _ = file.WriteString(fmt.Sprintf(`

-- -------------------------------------------------------------------------- --
-- %74s --
-- -------------------------------------------------------------------------- --

`, name))
		} else {
			file, err = genschema.FileFor(outputPath, name)
			if err != nil {
				log.Printf("failed to open file for table %v for write: %v", name, err)
				os.Exit(ExitCodeDatabase)
			}
		}
		log.Printf("Writing SQL for %v to %v", name, file.Name())
		dMap[name] = map[string]bool{}
		if !dataOnly {
			for _, descriptor := range tableDescriptors {
				dMap[name][strings.ToLower(descriptor.Type.String())] = true
				if err = genschema.WriteTableSQL(file, descriptor.SQL); err != nil {
					file.Close()
					if isSingleFile {
						os.Exit(ExitCodeDatabase)
					}
				}
			}
		}
		skipFields := make([]string, 0)
		if excludedFields != nil && excludedFields[i] != nil {
			skipFields = excludedFields[i]
		}

		insertSQL := gen.InsertSQL(name, skipFields...)
		if len(insertSQL) != 0 {
			dMap[name]["data"] = true
			if _, err = file.Write(insertSQL); err != nil {
				log.Printf("Failed to write insert sql for %v: %v", name, err)
				if isSingleFile {
					file.Close()
					os.Exit(ExitCodeDatabase)
				}
			}
		}
		if !isSingleFile {
			file.Close()
		}
	}
	if !isSingleFile {
		_ = genschema.WriteReadME(outputPath, orderedTableNames, dMap)
	}

	return
}
