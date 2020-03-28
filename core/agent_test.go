package core

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/Pirionfr/lookatch-agent/sources"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/Pirionfr/lookatch-agent/utils"
)

const (
	ConfJSON = `{"sinks":{"default":{"enabled":true,"type":"Stdout"}},"sources":{"default":{"autostart":true,"enabled":true,"type":"Random","linked_sinks":["default"],"wait":"1s"}}}`
	MetaJSON = `{"agent":{"status":{"name":"status","timestamp":1548686030755,"value":"ONLINE"}},"sources":{"default":{"offset":{"name":"offset","timestamp":1548686030755,"value":"toto"}}},"sinks":{"default":{"offset":{"name":"offset","timestamp":1548686030755,"value":"toto"}}}}`
	TaskJSON = `[{"end_date":null,"taskType":"QuerySource","created_at":1548686030754,"description":null,"id":"a0bebf3b-2db1-46d2-9af2-5991a47dca45","params":{"query":"SELECT * FROM truc"},"steps":[],"start_date":null,"status":"TODO","target":"sources::default"}]`
)

var (
	v            *viper.Viper
	server       *httptest.Server
	TestUUID     = "9eb6adae-a46e-4d2b-be97-a70a4cbe6f0d"
	TestTenant   = "89db1d7e-9d07-4b25-ad24-c673f7e4e90d"
	TestPassword = "test"
)

func init() {

	handler := func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case AuthPath:
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("token"))

		case strings.Replace(configurationPath, agentIDParamPath, TestUUID, 1):
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(ConfJSON))

		case strings.Replace(metaPath, sourceIDParamPath, TestUUID, 1):
			if r.Method == http.MethodPost {
				w.Header().Set("X-DCC-TASKS", strconv.Itoa(1))
				w.WriteHeader(http.StatusNoContent)
			} else if r.Method == http.MethodGet {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(MetaJSON))
			}
		case strings.Replace(tasksPath, agentIDParamPath, TestUUID, 1):
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(TaskJSON))

		case strings.Replace(strings.Replace(taskPath, agentIDParamPath, TestUUID, 1), taskIDParamPath, "a0bebf3b-2db1-46d2-9af2-5991a47dca45", 1):
			w.WriteHeader(http.StatusOK)

		case strings.Replace(strings.Replace(schemaPath, agentIDParamPath, TestUUID, 1), sourceIDParamPath, "default", 1):
			w.WriteHeader(http.StatusOK)

		case strings.Replace(capabilitiesPath, agentIDParamPath, TestUUID, 1):
			w.WriteHeader(http.StatusNoContent)
		case strings.Replace(strings.Replace(sourceCapabilitiesPath, agentIDParamPath, TestUUID, 1), sourceIDParamPath, "default", 1):
			w.WriteHeader(http.StatusNoContent)
		}

	}

	server = httptest.NewServer(http.HandlerFunc(handler))

	v = viper.New()
	log.SetLevel(log.DebugLevel)
	v.SetConfigFile("../config-test.json")

	v.ReadInConfig()
	v.Set("controller.base_url", server.URL)
	v.Set("controller.poller_ticker", "10s")
	v.Set("agent.uuid", TestUUID)
	v.Set("agent.password", TestPassword)
	v.Set("agent.healthport", 8080)

}

func NewTestAgent() *Agent {
	s := make(chan error)
	a := newAgent(v, s)
	return a
}

func TestUpdateConf(t *testing.T) {
	agent := NewTestAgent()

	err := agent.updateConfig([]byte(ConfJSON))
	if err != nil {
		t.Error(err)
	}

	if agent.config.GetString("sources.default.type") != "Random" {
		t.Fail()
	}
}

func TestCreateNewAgent(t *testing.T) {
	agent := NewTestAgent()

	if agent.hostname != "test" {
		t.Fail()
	}

}

func TestInitAgent(t *testing.T) {
	agent := NewTestAgent()
	err := agent.InitAgent()
	if err != nil {
		t.Error(err)
	}
}

func TestLoadSource(t *testing.T) {
	agent := NewTestAgent()

	err := agent.LoadSource("default", "Random")
	if err != nil {
		t.Error(err)
	}

}

func TestLoadSources(t *testing.T) {
	agent := NewTestAgent()

	multiplexer := make(map[string][]string)
	demux := make(map[string][]string)
	err := agent.LoadSources(&multiplexer, &demux)
	if err != nil {
		t.Error(err)
	}
}

func TestLoadSink(t *testing.T) {
	agent := NewTestAgent()

	err := agent.LoadSink("default", "Stdout")
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

	err := agent.InitAgent()
	if err != nil {
		t.Error(err)
	}

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

	agent.InitAgent()

	source := agent.getSources()
	if len(source) != 1 {
		t.Fail()
	}
}

func TestGetSink(t *testing.T) {
	agent := NewTestAgent()

	agent.InitAgent()

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

	agent.InitAgent()

	source := agent.getSinks()
	if len(source) != 1 {
		t.Fail()
	}
}

func TestGetSourceCapabilities(t *testing.T) {
	agent := NewTestAgent()

	agent.InitAgent()

	taskdesc := agent.getSourceCapabilities()

	if taskdesc["default"] == nil {
		t.Fail()
	}
}

func TestGetCapabilities(t *testing.T) {
	agent := NewTestAgent()

	agent.InitAgent()

	ctrlAgent := agent.getCapabilities()

	if ctrlAgent[utils.AgentStart] == nil {
		t.Fail()
	}

	if ctrlAgent[utils.AgentStop] == nil {
		t.Fail()
	}

	if ctrlAgent[utils.AgentRestart] == nil {
		t.Fail()
	}
}

func TestGetSourceMeta(t *testing.T) {
	agent := NewTestAgent()

	agent.InitAgent()

	sourceMeta := agent.getSourceMeta()

	if len(sourceMeta["default"]) == 0 {
		t.Fail()
	}

}

func TestGetSourceStatus(t *testing.T) {
	agent := NewTestAgent()

	agent.InitAgent()

	sourceStatus := agent.getSourceStatus()

	if sourceStatus["default"].Value != sources.SourceStatusRunning {
		t.Fail()
	}

}

func TestGetSourceSchema(t *testing.T) {
	agent := NewTestAgent()

	agent.InitAgent()

	srcSchema := agent.GetSchemas()

	if srcSchema["default"] == nil {
		t.Fail()
	}

}

func TestProcessTask(t *testing.T) {
	agent := NewTestAgent()

	err := agent.updateConfig([]byte(ConfJSON))
	if err != nil {
		t.Error(err)
	}

	aTask := []utils.Task{}

	if json.Unmarshal([]byte(TaskJSON), &aTask) != nil {
		t.Fail()
	}

	err = agent.ProcessTask(aTask[0])
	if err != nil {
		t.Fail()
	}

}

func TestProcessTask2(t *testing.T) {
	agent := NewTestAgent()

	err := agent.updateConfig([]byte(ConfJSON))
	if err != nil {
		t.Error(err)
	}

	aTask := []utils.Task{}

	if json.Unmarshal([]byte(TaskJSON), &aTask) != nil {
		t.Fail()
	}
	aTask[0].TaskType = utils.SourceStop

	err = agent.ProcessTask(aTask[0])
	if err != nil {
		t.Fail()
	}

}

func TestProcessTask3(t *testing.T) {
	agent := NewTestAgent()

	err := agent.updateConfig([]byte(ConfJSON))
	if err != nil {
		t.Error(err)
	}

	aTask := []utils.Task{}

	if json.Unmarshal([]byte(TaskJSON), &aTask) != nil {
		t.Fail()
	}
	aTask[0].TaskType = utils.SourceRestart

	err = agent.ProcessTask(aTask[0])
	if err != nil {
		t.Fail()
	}

}

func TestProcessTask4(t *testing.T) {
	agent := NewTestAgent()

	err := agent.updateConfig([]byte(ConfJSON))
	if err != nil {
		t.Error(err)
	}

	aTask := []utils.Task{}

	if json.Unmarshal([]byte(TaskJSON), &aTask) != nil {
		t.Fail()
	}
	aTask[0].TaskType = utils.SourceStart

	err = agent.ProcessTask(aTask[0])
	if err != nil {
		t.Fail()
	}

}

func TestProcessTaskError(t *testing.T) {
	agent := NewTestAgent()

	err := agent.updateConfig([]byte(ConfJSON))
	if err != nil {
		t.Error(err)
	}

	aTask := []utils.Task{}

	if json.Unmarshal([]byte(TaskJSON), &aTask) != nil {
		t.Fail()
	}
	aTask[0].TaskType = "toto"

	err = agent.ProcessTask(aTask[0])
	if err != nil {
		t.Fail()
	}

}

func TestAgentSendCapabilities(t *testing.T) {

	agent := NewTestAgent()

	err := agent.updateConfig([]byte(ConfJSON))
	if err != nil {
		t.Error(err)
	}

	err = agent.SendCapabilities()
	if err != nil {
		t.Fail()
	}

}

func TestRemoteInit(t *testing.T) {

	agent := NewTestAgent()

	err := agent.RemoteInit()
	if err != nil {
		t.Fail()
	}

}

func TestHealthCheckChecker(t *testing.T) {
	agent := NewTestAgent()
	agent.healthCheckChecker()

	if !agent.HealthCheck() {
		t.Fail()
	}
}

func TestSendMetaAndGetProcessTask(t *testing.T) {
	agent := NewTestAgent()
	err := agent.sendMetaAndGetProcessTask()
	if err != nil {
		t.Fail()
	}
}
