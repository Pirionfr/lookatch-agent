package util

import (
	"strings"
)

type (

	Filter struct {
		Filter_policy string                 `json:"filter_policy"`
		Filter        map[string]interface{} `json:"filter"`
	}
)

func (f *Filter) isAccept() bool {
	return f.Filter_policy == "accept"
}

func (f *Filter) IsFilteredDatabase(database string) bool {
	found := false
	if tableFilter, ok := f.Filter[strings.ToLower(database)]; ok {
		if tableFilter == nil {
			found = true
		} else {
			return false
		}
	}
	if found {
		return f.isAccept()
	}
	return !f.isAccept()
}

func (f *Filter) IsFilteredTable(database string, table string) bool {
	found := false
	if tableFilter, ok := f.Filter[strings.ToLower(database)].(map[string]interface{}); ok {
		if columnFilter, ok := tableFilter[strings.ToLower(table)]; ok {
			if columnFilter == nil {
				found = true
			} else {
				return false
			}
		}
	}
	if found {
		return f.isAccept()
	}
	return !f.isAccept()

}

func (f *Filter) IsFilteredColumn(database string, table string, column string) bool {
	found := false
	if tableFilter, ok := f.Filter[strings.ToLower(database)].(map[string]interface{}); ok {
		columnFilter := tableFilter[strings.ToLower(table)]
		if columnFilter != nil {
			found = contains(columnFilter.([]interface{}), column)
		}
	}
	if found {
		return f.isAccept()
	}
	return !f.isAccept()
}

func contains(s []interface{}, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
