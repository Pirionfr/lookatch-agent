package sources

import (
	"strconv"
	"sync"
	"time"

	"github.com/Pallinder/go-randomdata"
	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	log "github.com/sirupsen/logrus"
)

// Random representation of Random
type Random struct {
	*Source
	config     RandomConfig
	NbMessages int
	metaMutex  sync.RWMutex
}

// RandomConfig representation of Random Config
type RandomConfig struct {
	Enabled bool   `json:"enabled"`
	Wait    string `json:"wait"`
}

// RandomType type of source
const RandomType = "random"

// create new Random source
func newRandom(s *Source) (SourceI, error) {

	randomConfig := RandomConfig{}
	s.Conf.UnmarshalKey("sources."+s.Name, &randomConfig)

	return &Random{
		Source: s,
		config: randomConfig,
	}, nil
}

// Init source
func (r *Random) Init() {

}

// Stop source
func (r *Random) Stop() error {
	return nil
}

// Start source
func (r *Random) Start(i ...interface{}) error {
	go func() {
		wait, _ := time.ParseDuration(r.config.Wait)
		for {
			randomData := randomdata.Paragraph()
			log.WithFields(log.Fields{
				"data": randomData,
			}).Debug("random.Run()")
			r.OutputChannel <- &events.LookatchEvent{
				Header: &events.LookatchHeader{
					EventType: RandomType,
				},
				Payload: &events.GenericEvent{
					Tenant:      r.AgentInfo.tenant.Id,
					AgentId:     r.AgentInfo.uuid,
					Timestamp:   strconv.Itoa(int(time.Now().Unix())),
					Environment: r.AgentInfo.tenant.Env,
					Value:       randomData,
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

// GetName get source name
func (r *Random) GetName() string {
	return r.Name
}

// GetOutputChan get output channel
func (r *Random) GetOutputChan() chan *events.LookatchEvent {
	return r.OutputChannel
}

// IsEnable check if source is enable
func (r *Random) IsEnable() bool {
	return true
}

// HealthCheck return true if ok
func (r *Random) HealthCheck() bool {
	return true
}

// GetMeta get source meta
func (r *Random) GetMeta() map[string]interface{} {
	meta := make(map[string]interface{})
	meta["nbMessages"] = r.NbMessages
	return meta
}

// GetSchema Get source Schema
func (r *Random) GetSchema() interface{} {
	return "String"
}

// GetStatus Get source status
func (r *Random) GetStatus() interface{} {
	return control.SourceStatusRunning
}

// GetAvailableActions returns available actions
func (r *Random) GetAvailableActions() map[string]*control.ActionDescription {
	return nil
}

// Process action
func (r *Random) Process(action string, params ...interface{}) interface{} {
	return nil
}
