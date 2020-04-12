package sinks

import (
	"context"
	"encoding/json"

	"github.com/Pirionfr/goDcCrypto/crypto"
	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"

	"github.com/Pirionfr/lookatch-agent/events"
)

// PulsarType type of sink
const PulsarType = "Pulsar"

type (

	// PulsarSinkConfig representation of kafka sink config
	PulsarSinkConfig struct {
		Topic string `json:"topic"`
		URL   string `json:"url"`
		Token string `json:"token"`
	}

	// Pulsar representation of Pulsar sink
	Pulsar struct {
		*Sink
		PulsarConf *PulsarSinkConfig
		Producer   pulsar.Producer
	}
)

// NewPulsar create new pulsar sink
func NewPulsar(s *Sink) (SinkI, error) {

	ksConf := &PulsarSinkConfig{}
	err := s.Conf.Unmarshal(ksConf)
	if err != nil {
		return nil, nil
	}
	return &Pulsar{
		Sink:       s,
		PulsarConf: ksConf,
	}, nil
}

// Start connect to pulsar and start Producer
func (p *Pulsar) Start(_ ...interface{}) error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                        p.PulsarConf.URL,
		Authentication:             pulsar.NewAuthenticationToken(p.PulsarConf.Token),
		TLSAllowInsecureConnection: true,
	})

	if err != nil {
		return err
	}

	p.Producer, err = client.CreateProducer(pulsar.ProducerOptions{
		Topic: p.PulsarConf.Topic,
	})

	if err != nil {
		return err
	}

	go p.StartProducer()

	return nil
}

// GetInputChan return the input channel attached to this sink
func (p *Pulsar) GetInputChan() chan events.LookatchEvent {
	return p.In
}

// StartConsumer consume input chan
func (p *Pulsar) StartProducer() {

	for msg := range p.In {
		err := p.ProcessEvent(msg)
		if err != nil {
			log.WithError(err).Error("Producer could not send message")
		}
	}

}

// ProcessEvent convert LookatchEvent to  Pulsar ProducerMessage
func (p *Pulsar) ProcessEvent(msg events.LookatchEvent) error {
	payload, _ := json.Marshal(msg)

	if len(p.EncryptionKey) > 0 {
		var err error
		payload, err = crypto.EncryptBytes(payload, p.EncryptionKey)
		if err != nil {
			log.WithError(err).Error("KafkaSink Encrypt Error")
		}
	}

	pulsarMsg := &pulsar.ProducerMessage{
		Payload: payload,
	}

	_, err := p.Producer.Send(context.Background(), pulsarMsg)
	return err
}
