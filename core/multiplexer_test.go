package core

import (
	"reflect"
	"testing"

	"github.com/Pirionfr/lookatch-agent/events"
)

var (
	sinksChan []chan *events.LookatchEvent
	in        chan *events.LookatchEvent
)

func TestNewMultiplexer(t *testing.T) {
	in = make(chan *events.LookatchEvent, 1)
	sinksChan = append(sinksChan, make(chan *events.LookatchEvent, 1))
	multiplexer := NewMultiplexer(in, sinksChan)
	if reflect.TypeOf(multiplexer).String() != "*core.Multiplexer" {
		t.Error("mistmatch")
	}
}
