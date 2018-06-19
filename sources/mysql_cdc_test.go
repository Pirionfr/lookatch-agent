package sources

import (
	"github.com/spf13/viper"
	"reflect"
	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	"testing"
)

var vMysqlcdc *viper.Viper
var sMysqlcdc *Source

func init() {
	vMysqlcdc = viper.New()
	vMysqlcdc.Set("agent.hostname", "test")
	vMysqlcdc.Set("agent.tenant", "test")
	vMysqlcdc.Set("agent.env", "test")
	vMysqlcdc.Set("agent.uuid", "test")

	vMysqlcdc.Set("sources.default.autostart", true)
	vMysqlcdc.Set("sources.default.enabled", true)

	eventChan := make(chan *events.LookatchEvent, 1)

	agentInfo := &AgentHeader{
		tenant: events.LookatchTenantInfo{
			Id:  vMysqlcdc.GetString("agent.tenant"),
			Env: vMysqlcdc.GetString("agent.env"),
		},
		hostname: vMysqlcdc.GetString("agent.hostname"),
		uuid:     vMysqlcdc.GetString("agent.uuid"),
	}

	sMysqlcdc = &Source{
		Name:          "default",
		OutputChannel: eventChan,
		AgentInfo:     agentInfo,
		Conf:          vMysqlcdc,
	}

}

func TestMysqlcdcGetMeta(t *testing.T) {
	Mysqlcdc, ok := newMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if len(Mysqlcdc.GetMeta()) != 0 {
		t.Fail()
	}
}

func TestMysqlcdcGetMeta2(t *testing.T) {
	Mysqlcdc, ok := newMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if len(Mysqlcdc.GetMeta()) != 0 {
		t.Fail()
	}
}

func TestMysqlcdcGetSchema(t *testing.T) {
	Mysqlcdc, ok := newMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if len(Mysqlcdc.GetMeta()) != 0 {
		t.Fail()
	}
}

func TestMysqlcdcInit(t *testing.T) {
	Mysqlcdc, ok := newMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	Mysqlcdc.Init()
}

func TestMysqlcdcStop(t *testing.T) {
	Mysqlcdc, ok := newMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if Mysqlcdc.Stop() != nil {
		t.Fail()
	}
}

//TODO add standalone mode
//func TestMysqlcdcStart(t *testing.T) {
//	Mysqlcdc, ok := newMysqlCdc(sMysqlcdc)
//	if ok != nil {
//		t.Fail()
//	}
//
//	if Mysqlcdc.Start() != nil {
//		t.Fail()
//	}
//}

func TestMysqlcdcGetName(t *testing.T) {
	Mysqlcdc, ok := newMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if Mysqlcdc.GetName() != "default" {
		t.Fail()
	}
}

func TestMysqlcdcGetStatus(t *testing.T) {
	Mysqlcdc, ok := newMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if Mysqlcdc.GetStatus() != control.SourceStatusWaitingForMETA {
		t.Fail()
	}
}

func TestMysqlcdcIsEnable(t *testing.T) {
	Mysqlcdc, ok := newMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if Mysqlcdc.IsEnable() != true {
		t.Fail()
	}
}

func TestMysqlcdcHealtCheck(t *testing.T) {
	Mysqlcdc, ok := newMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if Mysqlcdc.HealtCheck() != false {
		t.Fail()
	}
}

func TestMysqlcdcGetAvailableActions(t *testing.T) {
	Mysqlcdc, ok := newMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if Mysqlcdc.GetAvailableActions() == nil {
		t.Fail()
	}
}

func TestMysqlcdcProcess(t *testing.T) {
	Mysqlcdc, ok := newMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if Mysqlcdc.Process("") != nil {
		t.Fail()
	}
}

func TestMysqlcdcGetOutputChan(t *testing.T) {
	Mysqlcdc, ok := newMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if reflect.TypeOf(Mysqlcdc.GetOutputChan()).String() != "chan *events.LookatchEvent" {
		t.Fail()
	}
}
