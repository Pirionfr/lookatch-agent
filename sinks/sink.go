package sinks

import (
	"github.com/Pirionfr/lookatch-common/events"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type (
	SinkI interface {
		Start(...interface{}) error
		GetInputChan() chan *events.LookatchEvent
	}
	Sink struct {
		in   chan *events.LookatchEvent
		stop chan error
		name string
		conf *viper.Viper
	}
)

type sinkCreator func(*Sink) (SinkI, error)

var factory = map[string]sinkCreator{
	StdoutType:   newStdout,
	KafkaType:    newKafka,
	OvhKafkaType: newOvhKafka,
}

func New(name string, sinkType string, conf *viper.Viper, stop chan error, eventChan chan *events.LookatchEvent) (SinkI, error) {
	//create sink from name
	sinkCreatorFunc, found := factory[sinkType]
	if !found {
		return nil, errors.Errorf("Sink type not found '%s'", sinkType)
	}

	customConf := conf.Sub("sinks." + name)
	if customConf == nil {
		err := errors.Errorf("no custom config found for sink name '%s'", name)
		log.Error(err)
		return nil, err
	}

	return sinkCreatorFunc(&Sink{eventChan, stop, name, customConf})
}
