package sources

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	"github.com/spf13/viper"
)

var vPostgreSQLQuery *viper.Viper
var sPostgreSQLQuery *Source

func init() {
	vPostgreSQLQuery = viper.New()
	vPostgreSQLQuery.Set("agent.hostname", "test")
	vPostgreSQLQuery.Set("agent.tenant", "test")
	vPostgreSQLQuery.Set("agent.env", "test")
	vPostgreSQLQuery.Set("agent.uuid", "test")

	vPostgreSQLQuery.Set("sources.default.autostart", true)
	vPostgreSQLQuery.Set("sources.default.enabled", true)

	eventChan := make(chan *events.LookatchEvent, 1)

	agentInfo := &AgentHeader{
		tenant: events.LookatchTenantInfo{
			Id:  vPostgreSQLQuery.GetString("agent.tenant"),
			Env: vPostgreSQLQuery.GetString("agent.env"),
		},
		hostname: vPostgreSQLQuery.GetString("agent.hostname"),
		uuid:     vPostgreSQLQuery.GetString("agent.uuid"),
	}

	sPostgreSQLQuery = &Source{
		Name:          "default",
		OutputChannel: eventChan,
		AgentInfo:     agentInfo,
		Conf:          vPostgreSQLQuery,
	}

}

func TestPostgreSQLQueryGetMeta(t *testing.T) {
	PostgreSQLQuery, ok := newPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if len(PostgreSQLQuery.GetMeta()) != 0 {
		t.Fail()
	}
}

func TestPostgreSQLQueryGetSchema(t *testing.T) {
	PostgreSQLQuery, ok := newPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if len(PostgreSQLQuery.GetMeta()) != 0 {
		t.Fail()
	}
}

func TestPostgreSQLQueryInit(t *testing.T) {
	PostgreSQLQuery, ok := newPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	PostgreSQLQuery.Init()
}

func TestPostgreSQLQueryStop(t *testing.T) {
	PostgreSQLQuery, ok := newPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if PostgreSQLQuery.Stop() != nil {
		t.Fail()
	}
}

//TODO add standalone mode
//func TestPostgreSQLQueryStart(t *testing.T) {
//	PostgreSQLQuery, ok := newPostgreSQLQuery(sPostgreSQLQuery)
//	if ok != nil {
//		t.Fail()
//	}
//
//	if PostgreSQLQuery.Start() != nil {
//		t.Fail()
//	}
//}

func TestPostgreSQLQueryGetName(t *testing.T) {
	PostgreSQLQuery, ok := newPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if PostgreSQLQuery.GetName() != "default" {
		t.Fail()
	}
}

func TestPostgreSQLQueryGetStatus(t *testing.T) {
	PostgreSQLQuery, ok := newPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}
	fmt.Println(PostgreSQLQuery.GetStatus())
	if PostgreSQLQuery.GetStatus() != control.SourceStatusOnError {
		t.Fail()
	}
}

func TestPostgreSQLQueryIsEnable(t *testing.T) {
	PostgreSQLQuery, ok := newPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if !PostgreSQLQuery.IsEnable() {
		t.Fail()
	}
}

func TestPostgreSQLQueryHealtCheck(t *testing.T) {
	PostgreSQLQuery, ok := newPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if PostgreSQLQuery.HealthCheck() {
		t.Fail()
	}
}

func TestPostgreSQLQueryGetAvailableActions(t *testing.T) {
	PostgreSQLQuery, ok := newPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if PostgreSQLQuery.GetAvailableActions() == nil {
		t.Fail()
	}
}

func TestPostgreSQLQueryProcess(t *testing.T) {
	PostgreSQLQuery, ok := newPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if PostgreSQLQuery.Process("") != nil {
		t.Fail()
	}
}

func TestPostgreSQLQueryGetOutputChan(t *testing.T) {
	PostgreSQLQuery, ok := newPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if reflect.TypeOf(PostgreSQLQuery.GetOutputChan()).String() != "chan *events.LookatchEvent" {
		t.Fail()
	}
}
