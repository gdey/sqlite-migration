package cmd

import (
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	force bool

	initCmd = func() *cobra.Command {
		cmd := &cobra.Command{
			Use:   "init",
			Short: "initialize the given database using the migration files.",
			Long: fmt.Sprintf(`initialize the given database using the migration files

The database will be destroyed and recreated. If the database already exists
and the "-f" flag has not be been set. The application will exist without
doing anything and an exist code of %d.
`, ExitCodeDatabaseAlreadyExists),
			Run: runInitCmd,
		}
		cmd.Flags().BoolVarP(&force, "force", "f", false, "force action")

		rootCmd.AddCommand(cmd)
		return cmd
	}()

	_ = initCmd
)

func runInitCmd(cmd *cobra.Command, _ []string) {

	migrations := migrationFor(cmd, migrationPath, tableName())
	log := getLogger(cmd)

	// check to see if the db file exists.
	if dbFilename == "" {
		log.Print("database file must be given")
		os.Exit(ExitCodeDatabase)
	}
	_, err := os.Stat(dbFilename)
	switch {
	case err == nil && !force:
		log.Printf("database file already exists")
		os.Exit(ExitCodeDatabaseAlreadyExists)
	case err == nil && force:
		log.Printf("removing database file: %v", dbFilename)
		os.Remove(dbFilename)
	default: // err != nil
		var perr = new(os.PathError)
		if !errors.As(err, &perr) {
			log.Printf("invalid database filename: %v", err)
			os.Exit(ExitCodeDatabase)
		}
	}
	db, err := sql.Open("sqlite3", dbFilename)
	if err != nil {
		log.Printf("error opening db %v: %v", dbFilename, err)
		os.Exit(ExitCodeDatabase)
	}
	// Now need to setup migrations
	ver, ok, err := migrations.Init(db, author)
	if err != nil {
		log.Printf("error opening db %v: %v", dbFilename, err)
		os.Exit(ExitCodeDatabase)

	}
	if !ok {
		log.Printf("did not init %v", dbFilename)
	}
	log.Printf("database file %v is at db version %v", dbFilename, ver)
	return
}
