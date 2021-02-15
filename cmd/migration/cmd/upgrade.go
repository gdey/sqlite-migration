package cmd

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	upgradeCmd = func() *cobra.Command {
		cmd := &cobra.Command{
			Use:   "upgrade",
			Short: "upgrade the given database using the migration files.",
			Long:  fmt.Sprintf(`upgrade the given database using the migration files`),
			Run:   runUpgradeCmd,
		}
		rootCmd.AddCommand(cmd)
		return cmd
	}()

	_ = upgradeCmd
)

func runUpgradeCmd(cmd *cobra.Command, _ []string) {

	migrations := migrationFor(cmd, migrationPath, tableName())
	log := getLogger(cmd)

	// check to see if the db file exists.
	if dbFilename == "" {
		log.Print("database file must be given")
		os.Exit(ExitCodeDatabase)
	}

	db, err := sql.Open("sqlite3", dbFilename)
	if err != nil {
		log.Printf("error opening db %v: %v", dbFilename, err)
		os.Exit(ExitCodeDatabase)
	}
	// Now need to setup migrations
	startingVersion, newVersion, err := migrations.Upgrade(db, author)
	if err != nil {
		log.Printf("error upgrading db %v: %v", dbFilename, err)
		os.Exit(ExitCodeDatabase)

	}
	if startingVersion == newVersion {
		log.Printf("database file %v already at latest version `%v`", dbFilename, startingVersion)
		return
	}
	if startingVersion == "" {
		startingVersion = "a new database"
	} else {
		startingVersion = "`" + startingVersion + "`"
	}
	log.Printf("database file (%v) upgraded from %v to `%v`", dbFilename, startingVersion, newVersion)
	return
}
