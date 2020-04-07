package sources

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/spf13/viper"

	"github.com/Pirionfr/lookatch-agent/events"
)

var vMysqlQuery *viper.Viper
var sMysqlQuery *Source

func init() {
	vMysqlQuery = viper.New()
	vMysqlQuery.Set("agent.hostname", "test")
	vMysqlQuery.Set("agent.env", "test")
	vMysqlQuery.Set("agent.uuid", "test")

	vMysqlQuery.Set("sources.default.autostart", true)
	vMysqlQuery.Set("sources.default.enabled", true)

	eventChan := make(chan events.LookatchEvent, 1)

	agentInfo := &AgentHeader{
		tenant: events.LookatchTenantInfo{
			ID:  vMysqlQuery.GetString("agent.uuid"),
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

	if len(MysqlQuery.GetMeta()) == 0 {
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
	mysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mQuery := mysqlQuery.(*MySQLQuery)
	mQuery.db = db

	fmt.Println(mysqlQuery.GetStatus())
	if mysqlQuery.GetStatus() != SourceStatusRunning {
		t.Fail()
	}
}

func TestMysqlQueryIsEnable(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	if !MysqlQuery.IsEnable() {
		t.Fail()
	}
}

func TestMysqlQueryHealtCheck(t *testing.T) {
	mysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mQuery := mysqlQuery.(*MySQLQuery)
	mQuery.db = db

	if !mysqlQuery.HealthCheck() {
		t.Fail()
	}
}

func TestMysqlQueryGetAvailableActions(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	if MysqlQuery.GetCapabilities() == nil {
		t.Fail()
	}
}

func TestMysqlQueryProcess(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	if MysqlQuery.Process("") == nil {
		t.Fail()
	}
}

func TestMysqlQueryGetOutputChan(t *testing.T) {
	MysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	if reflect.TypeOf(MysqlQuery.GetOutputChan()).String() != "chan events.LookatchEvent" {
		t.Fail()
	}
}

func TestQuerySchema(t *testing.T) {
	mysqlQuery, ok := newMysqlQuery(sMysqlQuery)
	if ok != nil {
		t.Fail()
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("Select 1").WillReturnRows(
		sqlmock.NewRows([]string{"1"}).
			AddRow(1))

	mQuery := mysqlQuery.(*MySQLQuery)
	mQuery.db = db

	res, err := mQuery.QueryMeta("Select 1")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when QueryMeta", err)
	}

	if res[0]["1"] == 1 {
		t.Fail()
	}
}
