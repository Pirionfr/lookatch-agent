package sources

import (
	"fmt"
	"github.com/spf13/viper"
	"reflect"
	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	"testing"
)

var vMysqlQuery *viper.Viper
var sMysqlQuery *Source

func init() {
	vMysqlQuery = viper.New()
	vMysqlQuery.Set("agent.hostname", "test")
	vMysqlQuery.Set("agent.tenant", "test")
	vMysqlQuery.Set("agent.env", "test")
	vMysqlQuery.Set("agent.uuid", "test")

	vMysqlQuery.Set("sources.default.autostart", true)
	vMysqlQuery.Set("sources.default.enabled", true)

	eventChan := make(chan *events.LookatchEvent, 1)

	agentInfo := &AgentHeader{
		tenant: events.LookatchTenantInfo{
			Id:  vMysqlQuery.GetString("agent.tenant"),
			Env: vMysqlQuery.GetString("agent.env"),
		},
		hostname: vMysqlQuery.GetString("agent.hostname"),
		uuid:     vMysqlQuery.GetString("agent.uuid"),
	}

	sMysqlQuery = &Source{
		Name:          "default",
		OutputChannel: eventChan,
		AgentInfo:     agentInfo,
		Conf:          vMysqlQuery,
	}

}

func TestMysqlQueryGetMeta(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	if len(MysqlQuery.GetMeta()) != 0 {
		t.Fail()
	}
}

func TestMysqlQueryGetSchema(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	if len(MysqlQuery.GetMeta()) != 0 {
		t.Fail()
	}
}

func TestMysqlQueryInit(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	MysqlQuery.Init()
}

func TestMysqlQueryStop(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	if MysqlQuery.Stop() != nil {
		t.Fail()
	}
}

//TODO add standalone mode
//func TestMysqlQueryStart(t *testing.T) {
//	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
//	if ok != nil {
//		t.Fail()
//	}
//
//	if MysqlQuery.Start() != nil {
//		t.Fail()
//	}
//}

func TestMysqlQueryGetName(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	if MysqlQuery.GetName() != "default" {
		t.Fail()
	}
}

func TestMysqlQueryGetStatus(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}
	fmt.Println(MysqlQuery.GetStatus())
	if MysqlQuery.GetStatus() != control.SourceStatusOnError {
		t.Fail()
	}
}

func TestMysqlQueryIsEnable(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	if MysqlQuery.IsEnable() != true {
		t.Fail()
	}
}

func TestMysqlQueryHealtCheck(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	if MysqlQuery.HealtCheck() != false {
		t.Fail()
	}
}

func TestMysqlQueryGetAvailableActions(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	if MysqlQuery.GetAvailableActions() == nil {
		t.Fail()
	}
}

func TestMysqlQueryProcess(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	if MysqlQuery.Process("") != nil {
		t.Fail()
	}
}

func TestMysqlQueryGetOutputChan(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	if reflect.TypeOf(MysqlQuery.GetOutputChan()).String() != "chan *events.LookatchEvent" {
		t.Fail()
	}
}

func TestExtractDatabaseTable1(t *testing.T) {
	m := &MySQLQuery{}
	database, table := m.ExtractDatabaseTable("select * From database.table")
	if database != "database" || table != "table" {
		t.Fail()
	}
}

func TestExtractDatabaseTable2(t *testing.T) {
	m := &MySQLQuery{}
	database, table := m.ExtractDatabaseTable("select * from database.table Where c1=10")
	if database != "database" || table != "table" {
		t.Fail()
	}
}

func TestExtractDatabaseTable3(t *testing.T) {
	m := &MySQLQuery{}
	database, table := m.ExtractDatabaseTable("select 1 from table")
	if database != "" || table != "table" {
		t.Fail()
	}
}

func TestExtractDatabaseTable4(t *testing.T) {
	m := &MySQLQuery{}
	database, table := m.ExtractDatabaseTable("select * from 异体字")
	if database != "" || table != "异体字" {
		t.Fail()
	}
}

func TestExtractDatabaseTable5(t *testing.T) {
	m := &MySQLQuery{}
	database, table := m.ExtractDatabaseTable("select 1")
	if database != "" || table != "" {
		t.Fail()
	}
}

func TestExtractDatabaseTable6(t *testing.T) {
	m := &MySQLQuery{}
	database, table := m.ExtractDatabaseTable("select * from database.tableWhere Where c1=10")
	if database != "database" || table != "tableWhere" {
		t.Fail()
	}
}
