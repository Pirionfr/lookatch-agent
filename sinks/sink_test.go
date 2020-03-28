package sinks

import (
	"reflect"
	"testing"

	"github.com/spf13/viper"
)

var (
	vSink *viper.Viper
)

func init() {
	vSink = viper.New()
	vSink.Set("sinks.default.autostart", true)
	vSink.Set("sinks.default.enabled", true)
}

func TestCreateSink(t *testing.T) {
	rch := make(chan error)
	r, err := New("default", "Stdout", vSink, rch)
	if err != nil {
		t.Error(err)
	}
	if reflect.TypeOf(r).String() != "*sinks.Stdout" {
		t.Error("mistmatch")
	}
}
