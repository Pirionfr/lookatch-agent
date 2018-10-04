package sources

import (
	"reflect"
	"testing"

	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	"github.com/spf13/viper"
)

var vDummy *viper.Viper
var sDummy *Source

func init() {
	vDummy = viper.New()
	vDummy.Set("agent.hostname", "test")
	vDummy.Set("agent.tenant", "test")
	vDummy.Set("agent.env", "test")
	vDummy.Set("agent.uuid", "test")

	eventChan := make(chan *events.LookatchEvent, 1)

	agentInfo := &AgentHeader{
		tenant: events.LookatchTenantInfo{
			Id:  vDummy.GetString("agent.tenant"),
			Env: vDummy.GetString("agent.env"),
		},
		hostname: vDummy.GetString("agent.hostname"),
		uuid:     vDummy.GetString("agent.uuid"),
	}

	sDummy = &Source{
		Name:          "Test",
		OutputChannel: eventChan,
		AgentInfo:     agentInfo,
		Conf:          vDummy,
	}

}

func TestDummyGetMeta(t *testing.T) {
	dummy, ok := newDummy(sDummy)
	if ok != nil {
		t.Fail()
	}

	if dummy.GetMeta() != nil {
		t.Fail()
	}
}

func TestDummyGetSchema(t *testing.T) {
	dummy, ok := newDummy(sDummy)
	if ok != nil {
		t.Fail()
	}

	if dummy.GetSchema() != nil {
		t.Fail()
	}
}

func TestDummyInit(t *testing.T) {
	dummy, ok := newDummy(sDummy)
	if ok != nil {
		t.Fail()
	}

	dummy.Init()
}

func TestDummyStop(t *testing.T) {
	dummy, ok := newDummy(sDummy)
	if ok != nil {
		t.Fail()
	}

	if dummy.Stop() != nil {
		t.Fail()
	}
}

func TestDummyStart(t *testing.T) {
	dummy, ok := newDummy(sDummy)
	if ok != nil {
		t.Fail()
	}

	if dummy.Start() != nil {
		t.Fail()
	}
}

func TestDummyGetName(t *testing.T) {
	dummy, ok := newDummy(sDummy)
	if ok != nil {
		t.Fail()
	}

	if dummy.GetName() != "Test" {
		t.Fail()
	}
}

func TestDummyGetStatus(t *testing.T) {
	dummy, ok := newDummy(sDummy)
	if ok != nil {
		t.Fail()
	}

	if dummy.GetStatus() != control.SourceStatusRunning {
		t.Fail()
	}
}

func TestDummyIsEnable(t *testing.T) {
	dummy, ok := newDummy(sDummy)
	if ok != nil {
		t.Fail()
	}

	if !dummy.IsEnable() {
		t.Fail()
	}
}

func TestDummyHealtCheck(t *testing.T) {
	dummy, ok := newDummy(sDummy)
	if ok != nil {
		t.Fail()
	}

	if !dummy.HealthCheck() {
		t.Fail()
	}
}

func TestDummyGetAvailableActions(t *testing.T) {
	dummy, ok := newDummy(sDummy)
	if ok != nil {
		t.Fail()
	}

	if dummy.GetAvailableActions() != nil {
		t.Fail()
	}
}

func TestDummyProcess(t *testing.T) {
	dummy, ok := newDummy(sDummy)
	if ok != nil {
		t.Fail()
	}

	if dummy.Process("") != nil {
		t.Fail()
	}
}

func TestDummyGetOutputChan(t *testing.T) {
	dummy, ok := newDummy(sDummy)
	if ok != nil {
		t.Fail()
	}

	if reflect.TypeOf(dummy.GetOutputChan()).String() != "chan *events.LookatchEvent" {
		t.Fail()
	}
}
