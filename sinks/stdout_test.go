package sinks

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/spf13/viper"
)

var (
	sink    *Sink
	vStdout *viper.Viper
)

func init() {
	eventChan := make(chan events.LookatchEvent, 1)
	stop := make(chan error)

	vStdout = viper.New()
	vStdout.Set("sinks.default.autostart", true)
	vStdout.Set("sinks.default.enabled", true)

	sink = &Sink{eventChan, stop, commitChan, "Stdout", "", vStdout.Sub("sinks.default")}
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

	if reflect.TypeOf(r.GetInputChan()).String() != "chan events.LookatchEvent" {
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

	r.GetInputChan() <- events.LookatchEvent{
		Header: events.LookatchHeader{
			EventType: "test",
		},
		Payload: events.GenericEvent{
			Timestamp:   strconv.Itoa(int(time.Now().Unix())),
			Environment: "test",
			Value:       "test",
			Offset: &events.Offset{
				Source: "1",
				Agent:  "1",
			},
		},
	}
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

	r.GetInputChan() <- events.LookatchEvent{
		Header: events.LookatchHeader{
			EventType: "test",
		},
		Payload: "test",
	}
}
