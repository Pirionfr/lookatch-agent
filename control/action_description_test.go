package control

import (
	"encoding/json"
	"testing"
)

func TestDeclareNewAction(t *testing.T) {
	type QueryAction struct {
		query string `description:"SQL query to execute on agent" required:"true"`
	}

	validResult := "{\"description\":\"Query source\",\"parameters\":[{\"name\":\"query\",\"description\":\"SQL query to execute on agent\",\"type\":\"string\",\"required\":true}]}"

	actionDeclaration := DeclareNewAction(QueryAction{}, "Query source")

	result, _ := json.Marshal(actionDeclaration)

	if string(result) != validResult {
		t.Fail()
	}
}
