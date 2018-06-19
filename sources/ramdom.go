package sources

import (
	"github.com/Pallinder/go-randomdata"
	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

type Random struct {
	*Source
	config     RandomConfig
	NbMessages int
}

type RandomConfig struct {
	Enabled bool   `json:"enabled"`
	Wait    string `json:"wait"`
}

const RandomType = "random"

func newRandom(s *Source) (SourceI, error) {

	randomConfig := RandomConfig{}
	s.Conf.UnmarshalKey("sources."+s.Name, &randomConfig)

	return &Random{
		Source: s,
		config: randomConfig,
	}, nil
}

func (d *Random) Init() {

}

func (d *Random) Stop() error {
	return nil
}

func (d *Random) Start(i ...interface{}) error {
	go func() {
		wait, _ := time.ParseDuration(d.config.Wait)
		for {
			randomData := randomdata.Paragraph()
			log.WithFields(log.Fields{
				"data": randomData,
			}).Debug("random.Run()")
			d.OutputChannel <- &events.LookatchEvent{
				Header: &events.LookatchHeader{
					EventType: RandomType,
				},
				Payload: &events.GenericEvent{
					Tenant:      d.AgentInfo.tenant.Id,
					AgentId:     d.AgentInfo.uuid,
					Timestamp:   strconv.Itoa(int(time.Now().Unix())),
					Environment: d.AgentInfo.tenant.Env,
					Value:       randomData,
				},
			}
			d.NbMessages += 1
			time.Sleep(wait)
		}
	}()
	return nil
}

func (d *Random) GetName() string {
	return d.Name
}

func (d *Random) GetOutputChan() chan *events.LookatchEvent {
	return d.OutputChannel
}

func (d *Random) IsEnable() bool {
	return true
}

func (d *Random) HealtCheck() bool {
	return true
}

func (r *Random) GetMeta() map[string]interface{} {
	meta := make(map[string]interface{})
	meta["nbMessages"] = r.NbMessages
	return meta
}

func (r *Random) GetSchema() interface{} {
	return "String"
}

func (r *Random) GetStatus() interface{} {
	return control.SourceStatusRunning
}

func (m *Random) GetAvailableActions() map[string]*control.ActionDescription {
	return nil
}

func (r *Random) Process(action string, params ...interface{}) interface{} {
	return nil
}
