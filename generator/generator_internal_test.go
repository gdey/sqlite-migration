package generator

import (
	"reflect"
	"testing"
)

func TestParseTableNameEntry(t *testing.T) {
	type tcase struct {
		line           string
		tableName      string
		excludedFields []string
		comment        string
		not            bool
	}

	fn := func(tc tcase) func(*testing.T) {
		return func(t *testing.T) {
			tableName, excludedFields, comment, not := parseTableNameEntry(tc.line)
			if tableName != tc.tableName {
				t.Errorf("tableName, expected '%v' got '%v'", tc.tableName, tableName)
			}
			if !reflect.DeepEqual(excludedFields, tc.excludedFields) {
				t.Errorf("excludedFields, expected %+v got %+v", tc.excludedFields, excludedFields)
			}
			if comment != tc.comment {
				t.Errorf("comment, expected '%v' got '%v'", tc.comment, comment)
			}
			if not != tc.not {
				t.Errorf("not, expected %v got %v", tc.not, not)
			}
		}
	}
	tests := map[string]tcase{
		"empty line": {},
		"* no excluded fields": {
			line:      "*",
			tableName: "*",
		},
		"* excluded fields": {
			line:           "* ! foo , bar",
			tableName:      "*",
			excludedFields: []string{"foo", "bar"},
		},
		"* excluded fields space": {
			line:           "* ! foo bar",
			tableName:      "*",
			excludedFields: []string{"foo", "bar"},
		},
		"foo excluded fields": {
			line:           "foo ! foo, bar",
			tableName:      "foo",
			excludedFields: []string{"foo", "bar"},
		},
		"foo excluded fields with comment": {
			line:           "foo ! foo, bar # a comment",
			tableName:      "foo",
			excludedFields: []string{"foo", "bar"},
			comment:        " a comment",
		},
		"foo_1 excluded fields with comment": {
			line:           "foo_1 ! foo, bar # a comment",
			tableName:      "foo_1",
			excludedFields: []string{"foo", "bar"},
			comment:        " a comment",
		},
		"foo bar1 excluded fields with comment": {
			line:           `"foo bar1" ! foo, bar # a comment`,
			tableName:      `foo bar1`,
			excludedFields: []string{"foo", "bar"},
			comment:        " a comment",
		},
		" foo bar1 excluded fields with comment": {
			line:           ` "foo bar1" ! foo, bar # a comment`,
			tableName:      `foo bar1`,
			excludedFields: []string{"foo", "bar"},
			comment:        " a comment",
		},
		"not foo excluded fields": {
			line:           "!foo ! foo, bar",
			tableName:      "foo",
			excludedFields: []string{"foo", "bar"},
			not:            true,
		},
	}
	for name, tc := range tests {
		t.Run(name, fn(tc))
	}

}
