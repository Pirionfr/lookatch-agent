package sources

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/Pirionfr/lookatch-agent/utils"

	randomdata "github.com/Pallinder/go-randomdata"
	log "github.com/sirupsen/logrus"

	"github.com/Pirionfr/lookatch-agent/events"
)

// Random representation of Random
type Random struct {
	*Source
	config     RandomConfig
	NbMessages uint64
	metaMutex  sync.RWMutex
}

// RandomConfig representation of Random Config
type RandomConfig struct {
	Wait string `json:"wait"`
}

// RandomType type of source
const RandomType = "Random"

// NewRandom create new Random source
func NewRandom(s *Source) (SourceI, error) {
	randomConfig := RandomConfig{}
	err := s.Conf.UnmarshalKey("sources."+s.Name, &randomConfig)
	if err != nil {
		return nil, err
	}
	return &Random{
		Source: s,
		config: randomConfig,
	}, nil
}

// Start source
func (r *Random) Start(i ...interface{}) error {
	if err := r.Source.Start(i); err != nil {
		return err
	}
	go func() {
		wait, _ := time.ParseDuration(r.config.Wait)
		for {
			randomData := randomdata.Paragraph()
			log.WithField("data", randomData).Debug("random.Run()")
			r.OutputChannel <- events.LookatchEvent{
				Header: events.LookatchHeader{
					EventType: RandomType,
				},
				Payload: events.GenericEvent{
					Timestamp:   strconv.Itoa(int(time.Now().Unix())),
					Environment: r.AgentInfo.Tenant.Env,
					Value:       randomData,
					Offset: &events.Offset{
						Source: strconv.FormatUint(r.NbMessages, 10),
						Agent:  strconv.FormatInt(r.Offset, 10),
					},
				},
			}
			r.metaMutex.Lock()
			r.NbMessages++
			r.metaMutex.Unlock()
			time.Sleep(wait)
		}
	}()
	return nil
}

// HealthCheck returns true if the source is correctly configured and the collector is connected to it
func (r *Random) HealthCheck() bool {
	return true
}

// GetMeta get source meta
func (r *Random) GetMeta() map[string]utils.Meta {
	meta := make(map[string]utils.Meta)
	meta["nbMessages"] = utils.NewMeta("nbMessages", r.NbMessages)
	return meta
}

// GetSchema Get source Schema
func (r *Random) GetSchema() map[string]map[string]*Column {
	return map[string]map[string]*Column{
		"ramdom": {
			"line": &Column{
				Column:       "line",
				ColumnOrdPos: 0,
				DataType:     "string",
				ColumnType:   "string",
			},
		},
	}
}

// GetStatus Get source status
func (r *Random) GetStatus() interface{} {
	return SourceStatusRunning
}

// Process action
func (r *Random) Process(action string, params ...interface{}) interface{} {
	return errors.New("not Implemented")
}
