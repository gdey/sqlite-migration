package generator_test

import (
	"github.com/gdey/sqlite-migration/generator"
	"reflect"
	"strings"
	"testing"
)

func TestSortUniqueStrings(t *testing.T) {
	type tcase struct {
		values   []string
		expected []string
	}
	fn := func(tc tcase) func(*testing.T) {
		return func(t *testing.T) {
			generator.SortUniqueStrings(&tc.values)
			if !reflect.DeepEqual(tc.expected, tc.values) {
				t.Errorf("sort unique, expected %+v got %+v", tc.expected, tc.values)
			}
		}
	}
	tests := map[string]tcase{
		"empty": {},
		"simple out of order": {
			values:   strings.Split("GAUTAMDEYCODERASIFF", ""),
			expected: strings.Split("ACDEFGIMORSTUY", ""),
		},
	}
	for name, tc := range tests {
		t.Run(name, fn(tc))
	}
}
