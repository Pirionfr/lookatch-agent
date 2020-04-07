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
	go demux.consumer()
	return
}

// consumer consume event from sinks and send it to source
func (d *DeMultiplexer) consumer() {
	for _, in := range d.ins {
		for value := range in {
			d.out <- value
		}
	}
}
