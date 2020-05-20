package core

// DeMultiplexer is used to send offset from sink to source
type DeMultiplexer struct {
	ins []chan interface{}
	out chan interface{}
}

// NewDemultiplexer create new DeMultiplexer
// return an initialized  DeMultiplexer object
func NewDemultiplexer(ins []chan interface{}, out chan interface{}) (demux *DeMultiplexer) {
	demux = &DeMultiplexer{
		ins: ins,
		out: out,
	}
	demux.consumer()
	return
}

// consumer consume event from sinks and send it to source
func (d *DeMultiplexer) consumer() {

	// Start an output goroutine for each input channel in cs.  output
	output := func(c <-chan interface{}) {
		for n := range c {
			d.out <- n
		}
	}
	for _, c := range d.ins {
		go output(c)
	}
}
