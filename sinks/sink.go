package sinks

import (
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/Pirionfr/lookatch-agent/events"
)

// Possible Statuses
const (
	// sink Possible Statuses
	SinkStatusOnError = "ON_ERROR"
	SinkStatusRunning = "RUNNING"
	SinkStatusWaiting = "WAITING"

	DefaultChannelSize = 100
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
		In            chan events.LookatchEvent
		Stop          chan error
		Commit        chan interface{}
		Name          string
		EncryptionKey string
		Conf          *viper.Viper
	}
)

// sinkCreator sink Creator func
type sinkCreator func(*Sink) (SinkI, error)

// Factory sink Factory
var Factory = map[string]sinkCreator{
	StdoutType: NewStdout,
	KafkaType:  NewKafka,
	PulsarType: NewPulsar,
}

// New create new sink
func New(name string, sinkType string, conf *viper.Viper, stop chan error) (SinkI, error) {
	//create sink from Name
	sinkCreatorFunc, found := Factory[sinkType]
	if !found {
		return nil, errors.Errorf("Sink type not found '%s'", sinkType)
	}

	customConf := conf.Sub("sinks." + name)
	if customConf == nil {
		err := errors.Errorf("no custom config found for sink Name '%s'", name)
		log.Error(err)
		return nil, err
	}

	channelSize := DefaultChannelSize
	if !conf.IsSet("sink." + name + ".chan_size") {
		channelSize = conf.GetInt("sink." + name + ".chan_size")
	}

	eventChan := make(chan events.LookatchEvent, channelSize)
	commitChan := make(chan interface{}, channelSize)

	return sinkCreatorFunc(&Sink{eventChan, stop, commitChan, name, conf.GetString("agent.EncryptionKey"), customConf})
}

// GetInputChan return input channel attach to sink
func (s *Sink) GetInputChan() chan events.LookatchEvent {
	return s.In
}

// GetCommitChan return the Commit channel attached to this sink
func (s *Sink) GetCommitChan() chan interface{} {
	return s.Commit
}

// SendCommit send a Commit message into the Commit channel of this sink
func (s *Sink) SendCommit(payload interface{}) {

	switch typedMsg := payload.(type) {
	case events.SQLEvent:
		if typedMsg.Offset != nil {
			s.Commit <- typedMsg.Offset.Source
		}
	case events.GenericEvent:
		if typedMsg.Offset != nil {
			s.Commit <- typedMsg.Offset.Source
		}
	case *events.Offset:
		if typedMsg != nil {
			s.Commit <- typedMsg.Source
		}
	default:
		log.WithField("message", payload).Warn("Source  doesn't match any known type")
	}
}
