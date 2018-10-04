package core

import (
	"encoding/json"
	"testing"

	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var v *viper.Viper

func init() {
	v = viper.New()
	log.SetLevel(log.DebugLevel)
	v.SetConfigFile("../config-test.json")
	v.ReadInConfig()

}

func NewTestAgent() *Agent {
	s := make(chan error)
	a := newAgent(v, s)
	return a
}

func TestUpdateConf(t *testing.T) {
	agent := NewTestAgent()

	err := agent.updateConfig([]byte(`{ "sources": {"default": {"dummy": "modified"}}}`))
	if err != nil {
		t.Error(err)
	}

	if agent.config.GetString("sources.default.dummy") != "modified" {
		t.Fail()
	}
}

func TestCreateNewAgent(t *testing.T) {
	agent := NewTestAgent()

	if agent.hostname != "test" {
		t.Fail()
	}

}

func TestInitConfig(t *testing.T) {
	agent := NewTestAgent()
	err := agent.InitConfig()
	if err != nil {
		t.Error(err)
	}
}

func TestLoadSource(t *testing.T) {
	eventChan := make(chan *events.LookatchEvent)
	agent := NewTestAgent()

	err := agent.LoadSource("default", "dummy", eventChan)
	if err != nil {
		t.Error(err)
	}

}

func TestLoadSources(t *testing.T) {
	agent := NewTestAgent()

	multiplexer := make(map[string][]string)
	err := agent.LoadSources(&multiplexer)
	if err != nil {
		t.Error(err)
	}
}

func TestLoadSink(t *testing.T) {
	agent := NewTestAgent()

	eventChan := make(chan *events.LookatchEvent)

	err := agent.LoadSink("default", "stdout", eventChan)
	if err != nil {
		t.Error(err)
	}

}

func TestLoadSinks(t *testing.T) {
	agent := NewTestAgent()

	err := agent.LoadSinks()
	if err != nil {
		t.Error(err)
	}
}

func TestLoadMultiplexer(t *testing.T) {
	agent := NewTestAgent()

	multiplexer := make(map[string][]string)
	err := agent.LoadMultiplexer(&multiplexer)
	if err != nil {
		t.Error(err)
	}
}

func TestGetSource(t *testing.T) {
	agent := NewTestAgent()

	agent.InitConfig()

	source, ok := agent.getSource("default")
	if !ok {
		t.Fail()
	}

	if source.GetName() != "default" {
		t.Fail()
	}
}

func TestGetSources(t *testing.T) {
	agent := NewTestAgent()

	agent.InitConfig()

	source := agent.getSources()
	if len(source) != 1 {
		t.Fail()
	}
}

func TestGetSink(t *testing.T) {
	agent := NewTestAgent()

	agent.InitConfig()

	sink, ok := agent.getSink("default")
	if !ok {
		t.Fail()
	}

	if sink == nil {
		t.Fail()
	}
}

func TestGetSinks(t *testing.T) {
	agent := NewTestAgent()

	agent.InitConfig()

	source := agent.getSinks()
	if len(source) != 1 {
		t.Fail()
	}
}

func TestGetSourceAvailableAction(t *testing.T) {
	agent := NewTestAgent()

	agent.InitConfig()

	ctrlAgent := agent.getSourceAvailableAction()

	var sourceAction = make(map[string]map[string]*control.ActionDescription)

	json.Unmarshal(ctrlAgent.Payload, &sourceAction)

	if sourceAction["default"] != nil {
		t.Fail()
	}
}

func TestGetAvailableAction(t *testing.T) {
	agent := NewTestAgent()

	agent.InitConfig()

	ctrlAgent := agent.getAvailableAction()

	var sourceAction = make(map[string]*control.ActionDescription)

	json.Unmarshal(ctrlAgent.Payload, &sourceAction)

	if sourceAction[control.AgentStart] == nil {
		t.Fail()
	}

	if sourceAction[control.AgentStop] == nil {
		t.Fail()
	}

	if sourceAction[control.AgentRestart] == nil {
		t.Fail()
	}
}

func TestGetSourceMeta(t *testing.T) {
	agent := NewTestAgent()

	agent.InitConfig()

	ctrlAgent := agent.getSourceMeta()

	var sourceMeta = make(map[string]*control.Meta)

	json.Unmarshal(ctrlAgent.Payload, &sourceMeta)

	if len(sourceMeta["default"].Data) == 0 {
		t.Fail()
	}

}

func TestGetSourceStatus(t *testing.T) {
	agent := NewTestAgent()

	agent.InitConfig()

	ctrlAgent := agent.getSourceStatus()

	var sourceStatus = make(map[string]*control.Status)

	json.Unmarshal(ctrlAgent.Payload, &sourceStatus)

	if sourceStatus["default"].Code != control.SourceStatusRunning {
		t.Fail()
	}

}

func TestGetSourceSchema(t *testing.T) {
	agent := NewTestAgent()

	agent.InitConfig()

	ctrlAgent := agent.GetSchemas()

	var sourceSchema = make(map[string]*control.Schema)

	json.Unmarshal(ctrlAgent.Payload, &sourceSchema)

	log.Debug(sourceSchema["default"].Raw)

	if sourceSchema["default"].Raw == nil {
		t.Fail()
	}

}
