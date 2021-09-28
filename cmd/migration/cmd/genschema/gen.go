package genschema

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

func WriteTableSQL(file *os.File, tableSQL string) (err error) {
	if tableSQL == "" {
		return nil
	}

	if _, err = file.WriteString(tableSQL); err != nil {
		log.Printf("Failed to write sql for %v: %v -- [%v]", file.Name(), err, tableSQL)
		return err
	}
	if !strings.HasSuffix(tableSQL, ";") {
		if _, err = file.WriteString(";\n"); err != nil {
			log.Printf("Failed to write sql for %v: %v -- ;", file.Name(), err)
			return err
		}
	}
	_, _ = file.WriteString("\n\n")
	return nil
}

func FilenameFor(dir, tableName string) string {
	name := strings.Map(func(r rune) rune {
		switch {
		case unicode.IsLetter(r):
			return unicode.To(unicode.LowerCase, r)
		case unicode.IsNumber(r):
			return r
		default:
			return '_'
		}
	}, tableName)
	return filepath.Join(dir, name+".sql")
}

// FileFor will return a file handle with the header written already,
// it will the responsibility of the caller to close the file, if there isn't an error
func FileFor(dir, tableName string) (*os.File, error) {

	filename := FilenameFor(dir, tableName)
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	_, err = f.WriteString(FileHeader)
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

const FileHeader = `
--
--                                                 --
-- File auto generated using sqlite-migration tool --
--               DO NOT EDIT!                      --
--                                                 --

`

func WriteReadME(dir string, tableNames []string, descriptorMap map[string]map[string]bool) error {
	file, err := os.Create(filepath.Join(dir, "README.md"))
	if err != nil {
		return err
	}
	defer file.Close()
	_, _ = file.WriteString(`# **This directory is for reference only.**

# ** DO NOT MODIFY **

This directory and all the files in here are managed by the sqlite-migration tool.

The directory and files are deleted and recreated each time.

## List of Files:

`)

	for _, tableName := range tableNames {
		descriptors := descriptorMap[tableName]
		//* [foo](schema/foo.sql), table, view, trigger, index, data
		_, _ = file.WriteString(fmt.Sprintf("* [%s](%s)",
			tableName,
			FilenameFor(".", tableName),
		))
		first := true
		for _, dType := range []string{"table", "view", "trigger", "index", "data"} {
			if descriptors[dType] {
				if !first {
					_, _ = file.WriteString(",")
				}
				_, _ = file.WriteString(" ")
				_, _ = file.WriteString(dType)
				first = false
			}
		}
		_, _ = file.WriteString("\n")
	}
	_, _ = file.WriteString("\n\n")
	return nil
}
