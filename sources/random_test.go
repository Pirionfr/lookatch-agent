package sources

import (
	"reflect"
	"testing"

	"github.com/spf13/viper"

	"github.com/Pirionfr/lookatch-agent/events"
)

var vRandom *viper.Viper
var sRandom *Source

func init() {
	vRandom = viper.New()
	vRandom.Set("agent.hostname", "test")
	vRandom.Set("agent.env", "test")
	vRandom.Set("agent.uuid", "test")

	eventChan := make(chan events.LookatchEvent, 1)

	agentInfo := &AgentHeader{
		tenant: events.LookatchTenantInfo{
			ID:  vRandom.GetString("agent.uuid"),
			Env: vRandom.GetString("agent.env"),
		},
		hostname: vRandom.GetString("agent.hostname"),
		uuid:     vRandom.GetString("agent.uuid"),
	}

	sRandom = &Source{
		Name:          "Test",
		OutputChannel: eventChan,
		AgentInfo:     agentInfo,
		Conf:          vRandom,
	}

}

func TestRandomGetMeta(t *testing.T) {
	Random, ok := newRandom(sRandom)
	if ok != nil {
		t.Fail()
	}

	if _, ok := Random.GetMeta()["nbMessages"]; !ok {
		t.Fail()
	}
}

func TestRandomGetSchema(t *testing.T) {
	Random, ok := newRandom(sRandom)
	if ok != nil {
		t.Fail()
	}

	if Random.GetSchema()["ramdom"]["line"].DataType != "string" {
		t.Fail()
	}
}

func TestRandomInit(t *testing.T) {
	Random, ok := newRandom(sRandom)
	if ok != nil {
		t.Fail()
	}

	Random.Init()
}

func TestRandomStop(t *testing.T) {
	Random, ok := newRandom(sRandom)
	if ok != nil {
		t.Fail()
	}

	if Random.Stop() != nil {
		t.Fail()
	}
}

func TestRandomStart(t *testing.T) {
	Random, ok := newRandom(sRandom)
	if ok != nil {
		t.Fail()
	}

	if Random.Start() != nil {
		t.Fail()
	}
}

func TestRandomGetName(t *testing.T) {
	Random, ok := newRandom(sRandom)
	if ok != nil {
		t.Fail()
	}

	if Random.GetName() != "Test" {
		t.Fail()
	}
}

func TestRandomGetStatus(t *testing.T) {
	Random, ok := newRandom(sRandom)
	if ok != nil {
		t.Fail()
	}

	if Random.GetStatus() != SourceStatusRunning {
		t.Fail()
	}
}

func TestRandomIsEnable(t *testing.T) {
	Random, ok := newRandom(sRandom)
	if ok != nil {
		t.Fail()
	}

	if !Random.IsEnable() {
		t.Fail()
	}
}

func TestRandomHealtCheck(t *testing.T) {
	Random, ok := newRandom(sRandom)
	if ok != nil {
		t.Fail()
	}

	if !Random.HealthCheck() {
		t.Fail()
	}
}

func TestRandomGetAvailableActions(t *testing.T) {
	Random, ok := newRandom(sRandom)
	if ok != nil {
		t.Fail()
	}

	if Random.GetCapabilities() == nil {
		t.Fail()
	}
}

func TestRandomProcess(t *testing.T) {
	Random, ok := newRandom(sRandom)
	if ok != nil {
		t.Fail()
	}

	if Random.Process("") == nil {
		t.Fail()
	}
}

func TestRandomGetOutputChan(t *testing.T) {
	Random, ok := newRandom(sRandom)
	if ok != nil {
		t.Fail()
	}

	if reflect.TypeOf(Random.GetOutputChan()).String() != "chan events.LookatchEvent" {
		t.Fail()
	}
}
