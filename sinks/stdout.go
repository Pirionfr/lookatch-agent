package sinks

import (
	"encoding/json"

	"github.com/Pirionfr/goDcCrypto/crypto"
	"github.com/Pirionfr/lookatch-agent/events"
	log "github.com/sirupsen/logrus"
)

// Stdout representation of sink
type Stdout struct {
	*Sink
}

// StdoutType type of sink
const StdoutType = "Stdout"

// NewStdout create new stdout sink
func NewStdout(s *Sink) (SinkI, error) {
	return &Stdout{s}, nil
}

// Start stdout sink
func (s *Stdout) Start(i ...interface{}) (err error) {
	go func(messages chan events.LookatchEvent) {
		for message := range messages {
			var bytes []byte
			bytes, err = json.Marshal(message.Payload)
			if err != nil {
				log.WithError(err).Error("json.Marshal()")
				return
			}
			msg := string(bytes)
			if s.EncryptionKey != "" {
				msg, err = crypto.EncryptString(string(bytes), s.EncryptionKey)
				if err != nil {
					log.WithError(err).Error("error while encrypting event")
					return
				}
			}

			log.WithField("message", msg).Info("Stdout Sink")
			s.SendCommit(message.Payload)
		}
	}(s.In)

	return
}
