package sources

import (
	"reflect"
	"testing"

	"github.com/Pirionfr/lookatch-agent/control"
	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/spf13/viper"
)

var vSyslog *viper.Viper
var sSyslog *Source

func init() {
	vSyslog = viper.New()
	vSyslog.Set("agent.hostname", "test")
	vSyslog.Set("agent.tenant", "test")
	vSyslog.Set("agent.env", "test")
	vSyslog.Set("agent.uuid", "test")

	vSyslog.Set("sources.default.autostart", true)
	vSyslog.Set("sources.default.enabled", true)
	vSyslog.Set("sources.default.port", 5142)

	eventChan := make(chan *events.LookatchEvent, 1)

	agentInfo := &AgentHeader{
		tenant: events.LookatchTenantInfo{
			ID:  vSyslog.GetString("agent.tenant"),
			Env: vSyslog.GetString("agent.env"),
		},
		hostname: vSyslog.GetString("agent.hostname"),
		uuid:     vSyslog.GetString("agent.uuid"),
	}

	sSyslog = &Source{
		Name:          "Test",
		OutputChannel: eventChan,
		AgentInfo:     agentInfo,
		Conf:          vSyslog,
	}

}

func TestSyslogGetMeta(t *testing.T) {
	Syslog, ok := newSyslog(sSyslog)
	if ok != nil {
		t.Fail()
	}

	if Syslog.GetMeta()["nbMessages"] != 0 {
		t.Fail()
	}
}

func TestSyslogGetSchema(t *testing.T) {
	Syslog, ok := newSyslog(sSyslog)
	if ok != nil {
		t.Fail()
	}

	if Syslog.GetSchema() != nil {
		t.Fail()
	}
}

func TestSyslogInit(t *testing.T) {
	Syslog, ok := newSyslog(sSyslog)
	if ok != nil {
		t.Fail()
	}

	Syslog.Init()
}

func TestSyslogStop(t *testing.T) {
	Syslog, ok := newSyslog(sSyslog)
	if ok != nil {
		t.Fail()
	}

	Syslog.Init()

	if Syslog.Stop() != nil {
		t.Fail()
	}
}

func TestSyslogStart(t *testing.T) {
	Syslog, ok := newSyslog(sSyslog)
	if ok != nil {
		t.Fail()
	}

	Syslog.Init()

	if Syslog.Start() != nil {
		t.Fail()
	}
}

func TestSyslogGetName(t *testing.T) {
	Syslog, ok := newSyslog(sSyslog)
	if ok != nil {
		t.Fail()
	}

	if Syslog.GetName() != "Test" {
		t.Fail()
	}
}

func TestSyslogGetStatus(t *testing.T) {
	Syslog, ok := newSyslog(sSyslog)
	if ok != nil {
		t.Fail()
	}

	if Syslog.GetStatus() != control.SourceStatusRunning {
		t.Fail()
	}
}

func TestSyslogIsEnable(t *testing.T) {
	Syslog, ok := newSyslog(sSyslog)
	if ok != nil {
		t.Fail()
	}

	if !Syslog.IsEnable() {
		t.Fail()
	}
}

func TestSyslogHealtCheck(t *testing.T) {
	Syslog, ok := newSyslog(sSyslog)
	if ok != nil {
		t.Fail()
	}

	if !Syslog.HealthCheck() {
		t.Fail()
	}
}

func TestSyslogGetAvailableActions(t *testing.T) {
	Syslog, ok := newSyslog(sSyslog)
	if ok != nil {
		t.Fail()
	}

	if Syslog.GetAvailableActions() != nil {
		t.Fail()
	}
}

func TestSyslogProcess(t *testing.T) {
	Syslog, ok := newSyslog(sSyslog)
	if ok != nil {
		t.Fail()
	}

	if Syslog.Process("") != nil {
		t.Fail()
	}
}

func TestSyslogGetOutputChan(t *testing.T) {
	Syslog, ok := newSyslog(sSyslog)
	if ok != nil {
		t.Fail()
	}
	Syslog.Init()

	if reflect.TypeOf(Syslog.GetOutputChan()).String() != "chan *events.LookatchEvent" {
		t.Fail()
	}
}
