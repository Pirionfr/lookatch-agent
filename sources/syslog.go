package sources

import (
	"strconv"
	"sync"
	"time"

	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	log "github.com/sirupsen/logrus"
	"gopkg.in/mcuadros/go-syslog.v2"
)

// SyslogType type of source
const SyslogType = "syslog"

// Syslog representation of Random
type Syslog struct {
	*Source
	server     *syslog.Server
	channel    syslog.LogPartsChannel
	config     SyslogConfig
	NbMessages int
	metaMutex  sync.RWMutex
}

// SyslogConfig representation of Random Config
type SyslogConfig struct {
	Type string `json:"Type"`
	Port int    `json:"Port"`
}

// newSyslog create new syslog source
func newSyslog(s *Source) (SourceI, error) {

	syslogConfig := SyslogConfig{}
	err := s.Conf.UnmarshalKey("sources."+s.Name, &syslogConfig)
	if err != nil {
		return nil, err
	}

	return &Syslog{
		Source: s,
		config: syslogConfig,
	}, nil
}

// GetMeta returns source meta
func (s *Syslog) GetMeta() map[string]interface{} {
	meta := make(map[string]interface{})
	meta["nbMessages"] = s.NbMessages
	return meta
}

// GetSchema returns schema
func (s *Syslog) GetSchema() interface{} {
	return nil
}

// Init syslog source
func (s *Syslog) Init() {
	channel := make(syslog.LogPartsChannel)
	handler := syslog.NewChannelHandler(channel)

	s.server = syslog.NewServer()
	s.server.SetFormat(syslog.RFC5424)
	s.server.SetHandler(handler)
	s.channel = channel
}

// Stop syslog source
func (s *Syslog) Stop() error {
	return nil
}

// Start syslog source
func (s *Syslog) Start(i ...interface{}) error {

	s.server.ListenUDP("0.0.0.0:" + strconv.Itoa(s.config.Port))
	s.server.Boot()

	go func() {
		for logParts := range s.channel {

			log.WithFields(log.Fields{
				"logParts": logParts,
			}).Debug("Run syslog")

			s.OutputChannel <- &events.LookatchEvent{
				Header: &events.LookatchHeader{
					EventType: SyslogType,
				},
				Payload: &events.GenericEvent{
					Tenant:      s.AgentInfo.tenant.Id,
					AgentId:     s.AgentInfo.uuid,
					Timestamp:   strconv.Itoa(int(time.Now().Unix())),
					Environment: s.AgentInfo.tenant.Env,
					Value:       logParts,
				},
			}
			s.metaMutex.Lock()
			s.NbMessages++
			s.metaMutex.Unlock()

		}
	}()

	return nil
}

// GetName get source name
func (s *Syslog) GetName() string {
	return s.Name
}

// GetOutputChan get output channel
func (s *Syslog) GetOutputChan() chan *events.LookatchEvent {
	return s.OutputChannel
}

// GetStatus get source status
func (s *Syslog) GetStatus() interface{} {
	return control.SourceStatusRunning
}

// IsEnable check if source is enable
func (s *Syslog) IsEnable() bool {
	return true
}

// HealthCheck return true if ok
func (s *Syslog) HealthCheck() bool {
	return true
}

// GetAvailableActions returns available actions
func (s *Syslog) GetAvailableActions() map[string]*control.ActionDescription {
	return nil
}

// Process action
func (s *Syslog) Process(action string, params ...interface{}) interface{} {
	return nil
}
