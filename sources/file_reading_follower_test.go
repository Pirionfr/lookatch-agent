package sources

import (
	"reflect"
	"testing"

	"github.com/Pirionfr/lookatch-agent/control"
	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/spf13/viper"
)

var vFileReadingFollower *viper.Viper
var sFileReadingFollower *Source

func init() {
	vFileReadingFollower = viper.New()
	vFileReadingFollower.Set("agent.hostname", "test")
	vFileReadingFollower.Set("agent.tenant", "test")
	vFileReadingFollower.Set("agent.env", "test")
	vFileReadingFollower.Set("agent.uuid", "test")

	vFileReadingFollower.Set("sources.default.autostart", true)
	vFileReadingFollower.Set("sources.default.enabled", true)
	vFileReadingFollower.Set("sources.default.path", "../config-test.json")

	eventChan := make(chan *events.LookatchEvent, 1)

	agentInfo := &AgentHeader{
		tenant: events.LookatchTenantInfo{
			ID:  vFileReadingFollower.GetString("agent.tenant"),
			Env: vFileReadingFollower.GetString("agent.env"),
		},
		hostname: vFileReadingFollower.GetString("agent.hostname"),
		uuid:     vFileReadingFollower.GetString("agent.uuid"),
	}

	sFileReadingFollower = &Source{
		Name:          "Test",
		OutputChannel: eventChan,
		AgentInfo:     agentInfo,
		Conf:          vFileReadingFollower,
	}

}

func TestFileReadingFollowerGetMeta(t *testing.T) {
	fileReadingFollower, ok := newFileReadingFollower(sFileReadingFollower)
	if ok != nil {
		t.Fail()
	}
	fileReadingFollower.Init()

	if fileReadingFollower.GetMeta()["offset"].(int64) != 0 {
		t.Fail()
	}
}

func TestFileReadingFollowerGetSchema(t *testing.T) {
	fileReadingFollower, ok := newFileReadingFollower(sFileReadingFollower)
	if ok != nil {
		t.Fail()
	}
	fileReadingFollower.Init()
	if fileReadingFollower.GetSchema() != "String" {
		t.Fail()
	}
}

func TestFileReadingFollowerInit(t *testing.T) {
	fileReadingFollower, ok := newFileReadingFollower(sFileReadingFollower)
	if ok != nil {
		t.Fail()
	}

	fileReadingFollower.Init()
}

func TestFileReadingFollowerStop(t *testing.T) {
	fileReadingFollower, ok := newFileReadingFollower(sFileReadingFollower)
	if ok != nil {
		t.Fail()
	}

	fileReadingFollower.Init()

	if fileReadingFollower.Stop() != nil {
		t.Fail()
	}
}

func TestFileReadingFollowerGetName(t *testing.T) {
	fileReadingFollower, ok := newFileReadingFollower(sFileReadingFollower)
	if ok != nil {
		t.Fail()
	}
	fileReadingFollower.Init()
	if fileReadingFollower.GetName() != "Test" {
		t.Fail()
	}
}

func TestFileReadingFollowerGetStatus(t *testing.T) {
	fileReadingFollower, ok := newFileReadingFollower(sFileReadingFollower)
	if ok != nil {
		t.Fail()
	}
	fileReadingFollower.Init()
	if fileReadingFollower.GetStatus() != control.SourceStatusRunning {
		t.Fail()
	}
}

func TestFileReadingFollowerIsEnable(t *testing.T) {
	fileReadingFollower, ok := newFileReadingFollower(sFileReadingFollower)
	if ok != nil {
		t.Fail()
	}
	fileReadingFollower.Init()
	if !fileReadingFollower.IsEnable() {
		t.Fail()
	}
}

func TestFileReadingFollowerHealtCheck(t *testing.T) {
	fileReadingFollower, ok := newFileReadingFollower(sFileReadingFollower)
	if ok != nil {
		t.Fail()
	}
	fileReadingFollower.Init()
	if !fileReadingFollower.HealthCheck() {
		t.Fail()
	}
}

func TestFileReadingFollowerGetAvailableActions(t *testing.T) {
	fileReadingFollower, ok := newFileReadingFollower(sFileReadingFollower)
	if ok != nil {
		t.Fail()
	}
	fileReadingFollower.Init()
	test := fileReadingFollower.GetAvailableActions()
	if test == nil {
		t.Fail()
	}
}

func TestFileReadingFollowerProcess(t *testing.T) {
	fileReadingFollower, ok := newFileReadingFollower(sFileReadingFollower)
	if ok != nil {
		t.Fail()
	}
	fileReadingFollower.Init()
	if fileReadingFollower.Process("") != nil {
		t.Fail()
	}
}

func TestFileReadingFollowerGetOutputChan(t *testing.T) {
	fileReadingFollower, ok := newFileReadingFollower(sFileReadingFollower)
	if ok != nil {
		t.Fail()
	}
	fileReadingFollower.Init()
	if reflect.TypeOf(fileReadingFollower.GetOutputChan()).String() != "chan *events.LookatchEvent" {
		t.Fail()
	}
}

func TestFileReadingFollowerStart(t *testing.T) {
	fileReadingFollower, ok := newFileReadingFollower(sFileReadingFollower)
	if ok != nil {
		t.Fail()
	}
	fileReadingFollower.Init()
	if fileReadingFollower.Start() != nil {
		t.Fail()
	}
}
