package sinks

import (
	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Possible Statuses
const (
	// sink Possible Statuses
	SinkStatusOnError = "ON_ERROR"
	SinkStatusRunning = "RUNNING"
	SinkStatusWaiting = "WAITING"
)

type (
	// SinkI sink interface
	SinkI interface {
		Start(...interface{}) error
		GetInputChan() chan events.LookatchEvent
		GetCommitChan() chan interface{}
	}
	// Sink representation of sink
	Sink struct {
		in            chan events.LookatchEvent
		stop          chan error
		commit        chan interface{}
		name          string
		encryptionkey string
		conf          *viper.Viper
	}
)

//sinkCreator sink Creator func
type sinkCreator func(*Sink) (SinkI, error)

// Factory sink Factory
var Factory = map[string]sinkCreator{
	StdoutType: NewStdout,
	KafkaType:  NewKafka,
	PulsarType: NewPulsar,
}

// New create new sink
func New(name string, sinkType string, conf *viper.Viper, stop chan error) (SinkI, error) {
	//create sink from name
	sinkCreatorFunc, found := Factory[sinkType]
	if !found {
		return nil, errors.Errorf("Sink type not found '%s'", sinkType)
	}

	customConf := conf.Sub("sinks." + name)
	if customConf == nil {
		err := errors.Errorf("no custom config found for sink name '%s'", name)
		log.Error(err)
		return nil, err
	}

	eventChan := make(chan events.LookatchEvent, 1000)
	commitChan := make(chan interface{}, 1000)

	return sinkCreatorFunc(&Sink{eventChan, stop, commitChan, name, conf.GetString("agent.encryptionkey"), customConf})
}

// GetInputChan return input channel attach to sink
func (s *Sink) GetInputChan() chan events.LookatchEvent {
	return s.in
}

// GetCommitChan return the commit channel attached to this sink
func (s *Sink) GetCommitChan() chan interface{} {
	return s.commit
}

// SendCommit send a commit message into the commit channel of this sink
func (s *Sink) SendCommit(payload interface{}) {

	switch typedMsg := payload.(type) {
	case events.SQLEvent:
		if typedMsg.Offset != nil {
			s.commit <- typedMsg.Offset.Source
		}
	case events.GenericEvent:
		if typedMsg.Offset != nil {
			s.commit <- typedMsg.Offset.Source
		}
	case *events.Offset:
		if typedMsg != nil {
			s.commit <- typedMsg.Source
		}
	default:
		log.WithField("message", payload).Warn("Source  doesn't match any known type")
	}
}
