package cmd

import (
	"log"
	"os"
	"path/filepath"
	"strings"

	migration "github.com/gdey/sqlite-migration"

	"github.com/spf13/cobra"
)

var (
	author          string
	dbFilename      string
	migrationPath   string
	migrationPrefix string
)

var rootCmd = func() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "migrate ...",
		Long:  `migrate enables the one to manage their sqlite databases`,
	}

	cmd.PersistentFlags().StringVarP(&author, "author", "a", os.Getenv("USER"), "author to assign for db changes")
	cmd.PersistentFlags().StringVar(&dbFilename, "db", "", "database file to use")
	cmd.PersistentFlags().StringVar(&migrationPath, "path", "sql_files/migrations", "the path to the migrations files.")
	cmd.PersistentFlags().StringVar(&migrationPrefix, "prefix", "gen", "the table prefix to use for the migrations table")

	return cmd
}()

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
	os.Exit(ExitCodeSuccess)
}

const (
	ExitCodeSuccess               = 0
	ExitCodeDatabase              = 4
	ExitCodeDatabaseAlreadyExists = 5
	ExitCodeOutputPath            = 6
)

func tableName() string {
	// grab the name of the migrations directory and the last element.
	base := strings.Trim(filepath.Base(migrationPath), `./\ `)
	base = strings.ReplaceAll(base, ".", "_")
	if base == "" {
		base = "migration"
	}
	return migrationPrefix + "_" + base
}

var myLogger *log.Logger

func getLogger(cmd *cobra.Command) *log.Logger {
	if myLogger != nil {
		return myLogger
	}

	myLogger = new(log.Logger)
	myLogger.SetPrefix("migration ")
	myLogger.SetOutput(cmd.OutOrStderr())
	return myLogger
}

func migrationFor(cmd *cobra.Command, path, tablename string) *migration.Manager {
	migrations := migration.New(path, tablename, nil)
	migrations.SetLog(getLogger(cmd))
	return migrations
}
