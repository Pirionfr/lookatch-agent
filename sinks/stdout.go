package sinks

import (
	"encoding/json"

	"github.com/Pirionfr/lookatch-common/events"
	log "github.com/sirupsen/logrus"
)

// Stdout representation of sink
type Stdout struct {
	*Sink
}

// StdoutType type of sink
const StdoutType = "stdout"

// newStdout create new stdout sink
func newStdout(s *Sink) (SinkI, error) {
	return &Stdout{s}, nil
}

// Start stdout sink
func (s *Stdout) Start(i ...interface{}) (err error) {
	go func(messages chan *events.LookatchEvent) {
		for message := range messages {

			if message == nil {
				continue
			}
			var bytes []byte

			bytes, err = json.Marshal(message.Payload)
			if err != nil {
				log.WithFields(log.Fields{
					"error": err,
				}).Error("json.Marshal()")
				return
			}
			log.WithFields(log.Fields{
				"message": string(bytes),
			}).Info("Stdout Sink")
		}
	}(s.in)

	return
}

//GetInputChan return input channel attach to sink
func (s *Stdout) GetInputChan() chan *events.LookatchEvent {
	return s.in
}
