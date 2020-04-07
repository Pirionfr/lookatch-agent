package core

import (
	"reflect"
	"testing"
)

var (
	commitIn  []chan interface{}
	commitOut chan interface{}
)

func TestNewDeMultiplexer(t *testing.T) {
	commitOut = make(chan interface{}, 1)
	commitIn = append(commitIn, make(chan interface{}, 1))
	multiplexer := NewDemultiplexer(commitIn, commitOut)
	if reflect.TypeOf(multiplexer).String() != "*core.DeMultiplexer" {
		t.Error("mistmatch")
	}
}
