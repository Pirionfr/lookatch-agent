package sinks

import (
	"github.com/spf13/viper"
	"reflect"
	"github.com/Pirionfr/lookatch-common/events"
	"testing"
)

var (
	vSink *viper.Viper
	in    chan *events.LookatchEvent
)

func init() {
	in = make(chan *events.LookatchEvent, 1)
	vSink = viper.New()
	vSink.Set("sinks.default.autostart", true)
	vSink.Set("sinks.default.enabled", true)
}

func TestCreateSink(t *testing.T) {
	rch := make(chan error)
	r, err := New("default", "stdout", vSink, rch, in)
	if err != nil {
		t.Error(err)
	}
	if reflect.TypeOf(r).String() != "*sinks.Stdout" {
		t.Error("mistmatch")
	}
}
