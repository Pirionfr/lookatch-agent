package sources

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/spf13/viper"

	"github.com/Pirionfr/lookatch-agent/events"
)

var vMSSQLQuery *viper.Viper
var sMSSQLQuery *Source

func init() {
	vMSSQLQuery = viper.New()
	vMSSQLQuery.Set("agent.hostname", "test")
	vMSSQLQuery.Set("agent.env", "test")
	vMSSQLQuery.Set("agent.uuid", "test")

	vMSSQLQuery.Set("sources.default.autostart", true)
	vMSSQLQuery.Set("sources.default.enabled", true)

	eventChan := make(chan events.LookatchEvent, 1)

	agentInfo := &AgentHeader{
		tenant: events.LookatchTenantInfo{
			ID:  vMSSQLQuery.GetString("agent.uuid"),
			Env: vMSSQLQuery.GetString("agent.env"),
		},
		hostname: vMSSQLQuery.GetString("agent.hostname"),
		uuid:     vMSSQLQuery.GetString("agent.uuid"),
	}

	sMSSQLQuery = &Source{
		Name:          "default",
		OutputChannel: eventChan,
		AgentInfo:     agentInfo,
		Conf:          vMSSQLQuery,
	}

}

func TestMSSQLQueryGetMeta(t *testing.T) {
	MSSQLQuery, ok := newSqlserverSQLQuery(sMSSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if len(MSSQLQuery.GetMeta()) == 0 {
		t.Fail()
	}
}

func TestMSSQLQueryInit(t *testing.T) {
	MSSQLQuery, ok := newSqlserverSQLQuery(sMSSQLQuery)
	if ok != nil {
		t.Fail()
	}

	MSSQLQuery.Init()
}

func TestMSSQLQueryStop(t *testing.T) {
	MSSQLQuery, ok := newSqlserverSQLQuery(sMSSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if MSSQLQuery.Stop() != nil {
		t.Fail()
	}
}

//TODO add standalone mode
//func TestMSSQLQueryStart(t *testing.T) {
//	MSSQLQuery, ok := newSqlserverSQLQuery(sMSSQLQuery)
//	if ok != nil {
//		t.Fail()
//	}
//
//	if MSSQLQuery.Start() != nil {
//		t.Fail()
//	}
//}

func TestMSSQLQueryGetName(t *testing.T) {
	MSSQLQuery, ok := newSqlserverSQLQuery(sMSSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if MSSQLQuery.GetName() != "default" {
		t.Fail()
	}
}

func TestMSSQLQueryGetStatus(t *testing.T) {
	MSSQLQuery, ok := newSqlserverSQLQuery(sMSSQLQuery)
	if ok != nil {
		t.Fail()
	}
	fmt.Println(MSSQLQuery.GetStatus())
	if MSSQLQuery.GetStatus() != SourceStatusOnError {
		t.Fail()
	}
}

func TestMSSQLQueryIsEnable(t *testing.T) {
	MSSQLQuery, ok := newSqlserverSQLQuery(sMSSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if !MSSQLQuery.IsEnable() {
		t.Fail()
	}
}

func TestMSSQLQueryHealtCheck(t *testing.T) {
	MSSQLQuery, ok := newSqlserverSQLQuery(sMSSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if MSSQLQuery.HealthCheck() {
		t.Fail()
	}
}

func TestMSSQLQueryGetAvailableActions(t *testing.T) {
	MSSQLQuery, ok := newSqlserverSQLQuery(sMSSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if MSSQLQuery.GetCapabilities() == nil {
		t.Fail()
	}
}

func TestMSSQLQueryProcess(t *testing.T) {
	MSSQLQuery, ok := newSqlserverSQLQuery(sMSSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if MSSQLQuery.Process("") == nil {
		t.Fail()
	}
}

func TestMSSQLQueryGetOutputChan(t *testing.T) {
	MSSQLQuery, ok := newSqlserverSQLQuery(sMSSQLQuery)
	if ok != nil {
		t.Fail()
	}

	if reflect.TypeOf(MSSQLQuery.GetOutputChan()).String() != "chan events.LookatchEvent" {
		t.Fail()
	}
}
