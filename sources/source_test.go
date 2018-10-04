package sources

import (
	"testing"

	"github.com/Pirionfr/lookatch-common/events"
	"github.com/spf13/viper"
)

var vSource *viper.Viper

func init() {
	vSource = viper.New()
	vSource.Set("sources.default.autostart", true)
	vSource.Set("sources.default.enabled", true)
	vSource.Set("agent.hostname", "test")
	vSource.Set("agent.tenant", "test")
	vSource.Set("agent.env", "test")
	vSource.Set("agent.uuid", "test")

}

func TestSourcesNew(t *testing.T) {

	eventChan := make(chan *events.LookatchEvent, 1)

	source, ok := New("default", RandomType, vSource, eventChan)
	if ok != nil {
		t.Fail()
	}

	if source.GetName() != "default" {
		t.Fail()
	}

}
