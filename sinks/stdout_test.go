package sinks

import (
	"github.com/spf13/viper"
	"reflect"
	"github.com/Pirionfr/lookatch-common/events"
	"strconv"
	"testing"
	"time"
)

var (
	sink    *Sink
	vStdout *viper.Viper
)

func init() {
	eventChan := make(chan *events.LookatchEvent, 1)
	stop := make(chan error)

	vStdout = viper.New()
	vStdout.Set("sinks.default.autostart", true)
	vStdout.Set("sinks.default.enabled", true)

	sink = &Sink{eventChan, stop, "stdout", vStdout.Sub("sinks.default")}
}

func TestNewStdout(t *testing.T) {

	r, err := newStdout(sink)
	if err != nil {
		t.Error(err)
	}
	if reflect.TypeOf(r).String() != "*sinks.Stdout" {
		t.Fail()
	}
}

func TestGetInputChan(t *testing.T) {
	r, err := newStdout(sink)
	if err != nil {
		t.Error(err)
	}

	if reflect.TypeOf(r.GetInputChan()).String() != "chan *events.LookatchEvent" {
		t.Fail()
	}
}

func TestStart(t *testing.T) {
	r, err := newStdout(sink)
	if err != nil {
		t.Error(err)
	}

	err = r.Start()
	if err != nil {
		t.Error(err)
	}
}

func TestStart2(t *testing.T) {
	r, err := newStdout(sink)
	if err != nil {
		t.Error(err)
	}

	err = r.Start()
	if err != nil {
		t.Error(err)
	}

	r.GetInputChan() <- &events.LookatchEvent{
		Header: &events.LookatchHeader{
			EventType: "test",
		},
		Payload: &events.GenericEvent{
			Tenant:      "test",
			AgentId:     "test",
			Timestamp:   strconv.Itoa(int(time.Now().Unix())),
			Environment: "test",
			Value:       "test",
		},
	}
}

func TestStartNil(t *testing.T) {
	r, err := newStdout(sink)
	if err != nil {
		t.Error(err)
	}

	err = r.Start()
	if err != nil {
		t.Error(err)
	}

	r.GetInputChan() <- nil
}

func TestStartBadEvent(t *testing.T) {
	r, err := newStdout(sink)
	if err != nil {
		t.Error(err)
	}

	err = r.Start()
	if err != nil {
		t.Error(err)
	}

	r.GetInputChan() <- &events.LookatchEvent{
		Header: &events.LookatchHeader{
			EventType: "test",
		},
		Payload: "test",
	}
}
