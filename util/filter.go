package util

import (
	"strings"
)

type (
	//Filter representation of source filter
	Filter struct {
		FilterPolicy string                 `json:"filter_policy" mapstructure:"filter_policy"`
		Filter       map[string]interface{} `json:"filter"`
	}
)

// isAccept returns true if policy is accept
func (f *Filter) isAccept() bool {
	return f.FilterPolicy == "accept"
}

// IsFilteredDatabase check if database is filtered
func (f *Filter) IsFilteredDatabase(database string) bool {
	found := false
	if f.Filter == nil {
		found = true
	} else {
		if _, ok := f.Filter[strings.ToLower(database)]; ok {
			found = true
		}
	}
	if found {
		return !f.isAccept()
	}
	return f.isAccept()
}

// IsFilteredTable check if table is filtered
func (f *Filter) IsFilteredTable(database string, table string) bool {
	found := false
	if tableFilter, ok := f.Filter[strings.ToLower(database)]; ok {
		if tableFilter == nil {
			found = true
		}else {
			if _, ok := tableFilter.(map[string]interface{})[strings.ToLower(table)]; ok {
				found = true
			}
		}

	}
	if found {
		return !f.isAccept()
	}
	return f.isAccept()

}

// IsFilteredColumn check if column is filtered
func (f *Filter) IsFilteredColumn(database string, table string, column string) bool {
	found := false
	if tableFilter, ok := f.Filter[strings.ToLower(database)]; ok {
		if columnFilter, ok := tableFilter.(map[string]interface{})[strings.ToLower(table)] ; ok {
			if columnFilter != nil {
				found = contains(columnFilter.([]interface{}), column)
			} else {
				found = true
			}
		}
	}

	if found {
		return !f.isAccept()
	}
	return f.isAccept()
}

// contains check if e is contain in s
func contains(s []interface{}, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
