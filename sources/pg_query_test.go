package sources

import (
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/spf13/viper"

	"github.com/Pirionfr/lookatch-agent/events"
)

var vPostgreSQLQuery *viper.Viper
var sPostgreSQLQuery *Source

func init() {
	vPostgreSQLQuery = viper.New()
	vPostgreSQLQuery.Set("agent.Hostname", "test")
	vPostgreSQLQuery.Set("agent.env", "test")
	vPostgreSQLQuery.Set("agent.UUID", "test")

	vPostgreSQLQuery.Set("sources.default.autostart", true)
	vPostgreSQLQuery.Set("sources.default.enabled", true)

	eventChan := make(chan events.LookatchEvent, 1)

	agentInfo := &AgentHeader{
		Tenant: events.LookatchTenantInfo{
			ID:  vPostgreSQLQuery.GetString("agent.UUID"),
			Env: vPostgreSQLQuery.GetString("agent.env"),
		},
		Hostname: vPostgreSQLQuery.GetString("agent.Hostname"),
		UUID:     vPostgreSQLQuery.GetString("agent.UUID"),
	}

	sPostgreSQLQuery = &Source{
		Name:          "default",
		OutputChannel: eventChan,
		AgentInfo:     agentInfo,
		Conf:          vPostgreSQLQuery,
	}

}

func TestPostgreSQLQueryGetMeta(t *testing.T) {
	PostgreSQLQuery, ok := NewPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if len(PostgreSQLQuery.GetMeta()) == 0 {
		t.Fail()
	}
}

func TestPostgreSQLQueryInit(t *testing.T) {
	PostgreSQLQuery, ok := NewPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	PostgreSQLQuery.Init()
}

func TestPostgreSQLQueryStop(t *testing.T) {
	PostgreSQLQuery, ok := NewPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if PostgreSQLQuery.Stop() != nil {
		t.Fail()
	}
}

//TODO add standalone mode
//func TestPostgreSQLQueryStart(t *testing.T) {
//	PostgreSQLQuery, ok := NewPostgreSQLQuery(sPostgreSQLQuery)
//	if ok != nil {
//		t.Fail()
//	}
//
//	if PostgreSQLQuery.Start() != nil {
//		t.Fail()
//	}
//}

func TestPostgreSQLQueryGetName(t *testing.T) {
	PostgreSQLQuery, ok := NewPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if PostgreSQLQuery.GetName() != "default" {
		t.Fail()
	}
}

func TestPostgreSQLQueryGetStatus(t *testing.T) {
	pgQuery, ok := NewPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	pQuery := pgQuery.(*PostgreSQLQuery)
	pQuery.db = db

	if pgQuery.GetStatus() != SourceStatusRunning {
		t.Fail()
	}
}

func TestPostgreSQLQueryIsEnable(t *testing.T) {
	PostgreSQLQuery, ok := NewPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if !PostgreSQLQuery.IsEnable() {
		t.Fail()
	}
}

func TestPostgreSQLQueryHealtCheck(t *testing.T) {
	pgQuery, ok := NewPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	pQuery := pgQuery.(*PostgreSQLQuery)
	pQuery.db = db

	if !pgQuery.HealthCheck() {
		t.Fail()
	}
}

func TestPostgreSQLQueryGetAvailableActions(t *testing.T) {
	PostgreSQLQuery, ok := NewPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if PostgreSQLQuery.GetCapabilities() == nil {
		t.Fail()
	}
}

func TestPostgreSQLQueryProcess(t *testing.T) {
	PostgreSQLQuery, ok := NewPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if PostgreSQLQuery.Process("") == nil {
		t.Fail()
	}
}

func TestPostgreSQLQueryGetOutputChan(t *testing.T) {
	PostgreSQLQuery, ok := NewPostgreSQLQuery(sPostgreSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if reflect.TypeOf(PostgreSQLQuery.GetOutputChan()).String() != "chan events.LookatchEvent" {
		t.Fail()
	}
}
