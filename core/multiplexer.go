package core

import "github.com/Pirionfr/lookatch-common/events"

type Multiplexer struct {
	in   chan *events.LookatchEvent
	outs []chan *events.LookatchEvent
}

func NewMultiplexer(in chan *events.LookatchEvent, outs []chan *events.LookatchEvent) (multiplexer *Multiplexer) {
	multiplexer = &Multiplexer{
		in:   in,
		outs: outs,
	}
	go multiplexer.consumer()
	return
}

func (a *Multiplexer) consumer() {
	for event := range a.in {

		for value := range a.outs {
			a.outs[value] <- event
		}
	}
}
