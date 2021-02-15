package main

import (
	"github.com/gdey/sqlite-migration/cmd/migration/cmd"

	// we only work with sqlite
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	cmd.Execute()
}
