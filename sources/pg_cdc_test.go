package sources

import (
	"reflect"
	"testing"

	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	"github.com/spf13/viper"
)

var vPgcdc *viper.Viper
var sPgcdc *Source

func init() {
	vPgcdc = viper.New()
	vPgcdc.Set("agent.hostname", "test")
	vPgcdc.Set("agent.tenant", "test")
	vPgcdc.Set("agent.env", "test")
	vPgcdc.Set("agent.uuid", "test")

	vPgcdc.Set("sources.default.autostart", true)
	vPgcdc.Set("sources.default.enabled", true)

	eventChan := make(chan *events.LookatchEvent, 1)

	agentInfo := &AgentHeader{
		tenant: events.LookatchTenantInfo{
			Id:  vPgcdc.GetString("agent.tenant"),
			Env: vPgcdc.GetString("agent.env"),
		},
		hostname: vPgcdc.GetString("agent.hostname"),
		uuid:     vPgcdc.GetString("agent.uuid"),
	}

	sPgcdc = &Source{
		Name:          "default",
		OutputChannel: eventChan,
		AgentInfo:     agentInfo,
		Conf:          vPgcdc,
	}

}

func TestPgcdcGetMeta(t *testing.T) {
	Pgcdc, ok := newPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if len(Pgcdc.GetMeta()) != 0 {
		t.Fail()
	}
}

func TestPgcdcGetMeta2(t *testing.T) {
	Pgcdc, ok := newPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if len(Pgcdc.GetMeta()) != 0 {
		t.Fail()
	}
}

func TestPgcdcGetSchema(t *testing.T) {
	Pgcdc, ok := newPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if len(Pgcdc.GetMeta()) != 0 {
		t.Fail()
	}
}

func TestPgcdcInit(t *testing.T) {
	Pgcdc, ok := newPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	Pgcdc.Init()
}

func TestPgcdcStop(t *testing.T) {
	Pgcdc, ok := newPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if Pgcdc.Stop() != nil {
		t.Fail()
	}
}

//TODO add standalone mode
//func TestPgcdcStart(t *testing.T) {
//	Pgcdc, ok := newPostgreSQLCdc(sPgcdc)
//	if ok != nil {
//		t.Fail()
//	}
//
//	if Pgcdc.Start() != nil {
//		t.Fail()
//	}
//}

func TestPgcdcGetName(t *testing.T) {
	Pgcdc, ok := newPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if Pgcdc.GetName() != "default" {
		t.Fail()
	}
}

func TestPgcdcGetStatus(t *testing.T) {
	Pgcdc, ok := newPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if Pgcdc.GetStatus() != control.SourceStatusWaitingForMETA {
		t.Fail()
	}
}

func TestPgcdcIsEnable(t *testing.T) {
	Pgcdc, ok := newPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if !Pgcdc.IsEnable() {
		t.Fail()
	}
}

func TestPgcdcHealtCheck(t *testing.T) {
	Pgcdc, ok := newPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if Pgcdc.HealthCheck() {
		t.Fail()
	}
}

func TestPgcdcGetAvailableActions(t *testing.T) {
	Pgcdc, ok := newPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if Pgcdc.GetAvailableActions() == nil {
		t.Fail()
	}
}

func TestPgcdcProcess(t *testing.T) {
	Pgcdc, ok := newPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if Pgcdc.Process("") != nil {
		t.Fail()
	}
}

func TestPgcdcGetOutputChan(t *testing.T) {
	Pgcdc, ok := newPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if reflect.TypeOf(Pgcdc.GetOutputChan()).String() != "chan *events.LookatchEvent" {
		t.Fail()
	}
}
