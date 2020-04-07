package utils

import (
	"encoding/json"
	"testing"
)

func TestDeclareNewTask(t *testing.T) {
	type QueryAction struct {
		query string `name:"query" description:"SQL query to execute on agent" required:"true"` // nolint
	}

	validResult := "{\"description\":\"Query source\",\"params\":[{\"name\":\"query\",\"description\":\"SQL query to execute on agent\",\"type\":\"string\",\"required\":true}]}"

	actionDeclaration := DeclareNewTaskDescription(QueryAction{}, "Query source")

	result, _ := json.Marshal(actionDeclaration)

	if string(result) != validResult {
		t.Fail()
	}
}

func TestDeclareNewTaskNilClass(t *testing.T) {

	actionDeclaration := DeclareNewTaskDescription(nil, "Query source")

	if actionDeclaration.Parameters != nil {
		t.Fail()
	}
}
