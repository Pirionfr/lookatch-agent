package core

import "github.com/Pirionfr/lookatch-agent/events"

// Multiplexer represent the Multiplexer of collector
type Multiplexer struct {
	in   chan events.LookatchEvent
	outs []chan events.LookatchEvent
}

// NewMultiplexer create a new multiplexer
func NewMultiplexer(in chan events.LookatchEvent, outs []chan events.LookatchEvent) (multiplexer *Multiplexer) {
	multiplexer = &Multiplexer{
		in:   in,
		outs: outs,
	}
	go multiplexer.consumer()
	return
}

// consumer send event from source to sink
func (a *Multiplexer) consumer() {
	for event := range a.in {
		for value := range a.outs {
			a.outs[value] <- event
		}
	}
}
