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

	// pulsarSinkConfig representation of kafka sink config
	pulsarSinkConfig struct {
		Topic string `json:"topic"`
		URL   string `json:"url"`
		Token string `json:"token"`
	}

	// Pulsar representation of Pulsar sink
	Pulsar struct {
		*Sink
		pulsarConf *pulsarSinkConfig
		producer   pulsar.Producer
	}
)

// newPulsar create new pulsar sink
func newPulsar(s *Sink) (SinkI, error) {

	ksConf := &pulsarSinkConfig{}
	err := s.conf.Unmarshal(ksConf)
	if err != nil {
		return nil, nil
	}
	return &Pulsar{
		Sink:       s,
		pulsarConf: ksConf,
	}, nil
}

// Start connect to pulsar and start producer
func (p *Pulsar) Start(_ ...interface{}) error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                        p.pulsarConf.URL,
		Authentication:             pulsar.NewAuthenticationToken(p.pulsarConf.Token),
		TLSAllowInsecureConnection: true,
	})

	if err != nil {
		return err
	}

	p.producer, err = client.CreateProducer(pulsar.ProducerOptions{
		Topic: p.pulsarConf.Topic,
	})

	if err != nil {
		return err
	}

	go p.startProducer()

	return nil
}

// GetInputChan return the input channel attached to this sink
func (p *Pulsar) GetInputChan() chan events.LookatchEvent {
	return p.in
}

// startConsumer consume input chan
func (p *Pulsar) startProducer() {

	for msg := range p.in {
		err := p.processEvent(msg)
		if err != nil {
			log.WithError(err).Error("Producer could not send message")
		}
	}

}

// processEvent convert LookatchEvent to  Pulsar ProducerMessage
func (p *Pulsar) processEvent(msg events.LookatchEvent) error {
	payload, _ := json.Marshal(msg)

	if len(p.encryptionkey) > 0 {
		var err error
		payload, err = crypto.EncryptBytes(payload, p.encryptionkey)
		if err != nil {
			log.WithError(err).Error("KafkaSink Encrypt Error")
		}
	}

	pulsarMsg := &pulsar.ProducerMessage{
		Payload: payload,
	}

	_, err := p.producer.Send(context.Background(), pulsarMsg)
	return err
}
