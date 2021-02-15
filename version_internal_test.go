package migration

import (
	"os"
	"path/filepath"
	"testing"
)

func Test_ParseMigrationSequenceFile(t *testing.T) {
	type tcase struct {
		filename string
		versions []string
	}
	fn := func(tc tcase) func(*testing.T) {
		return func(t *testing.T) {

			f, err := os.Open(filepath.Join("testdata", "migration_files", tc.filename))
			if err != nil {
				t.Fatalf("Was not able to open test file: %v", tc.filename)
				return
			}
			defer f.Close()

			versions := getVersionsFromFile(f)

			if len(tc.versions) != len(versions) {
				t.Errorf("versions length, expected %v got %v", len(tc.versions), len(versions))
				maxLength := len(tc.versions)
				if len(tc.versions) < len(versions) {
					maxLength = len(versions)
				}
				for i := 0; i < maxLength; i++ {
					tcv, v := "---", "---"
					if i < len(tc.versions) {
						tcv = tc.versions[i]
					}
					if i < len(versions) {
						v = versions[i]
					}
					t.Logf("index %03d  expected '%s' got '%s'", i, tcv, v)
				}
				return
			}

		}
	}

	tests := map[string]tcase{
		"empty": {
			filename: "empty.txt",
			versions: []string{""},
		},
		"empty comment": {
			filename: "empty_w_comments.txt",
			versions: []string{""},
		},
		"one entry": {
			filename: "one_entry.txt",
			versions: []string{"", "2020/add_user.sql"},
		},
		"one entry comment": {
			filename: "one_entry_w_comments.txt",
			versions: []string{"", "2020/add_user.sql"},
		},
	}
	for name, tc := range tests {
		t.Run(name, fn(tc))
	}
}
