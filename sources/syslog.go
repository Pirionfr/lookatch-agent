package sources

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/Pirionfr/lookatch-agent/utils"

	log "github.com/sirupsen/logrus"
	syslog "gopkg.in/mcuadros/go-syslog.v2"

	"github.com/Pirionfr/lookatch-agent/events"
)

// SyslogType type of source
const SyslogType = "Syslog"

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

// NewSyslog create new syslog source
func NewSyslog(s *Source) (SourceI, error) {
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
func (s *Syslog) GetMeta() map[string]utils.Meta {
	meta := make(map[string]utils.Meta)
	meta["nbMessages"] = utils.NewMeta("nbMessages", s.NbMessages)
	return meta
}

// GetSchema returns schema
func (s *Syslog) GetSchema() map[string]map[string]*Column {
	return map[string]map[string]*Column{
		"syslog": {
			"line": &Column{
				Column:       "line",
				ColumnOrdPos: 0,
				DataType:     "string",
				ColumnType:   "string",
			},
		},
	}
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
	err := s.server.ListenUDP("0.0.0.0:" + strconv.Itoa(s.config.Port))
	if err != nil {
		return err
	}
	err = s.server.Boot()
	if err != nil {
		return err
	}
	go func() {
		for logParts := range s.channel {
			log.WithField("logParts", logParts).Debug("Run syslog")

			s.OutputChannel <- events.LookatchEvent{
				Header: events.LookatchHeader{
					EventType: SyslogType,
				},
				Payload: events.GenericEvent{
					Timestamp:   strconv.Itoa(int(time.Now().Unix())),
					Environment: s.AgentInfo.Tenant.Env,
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
func (s *Syslog) GetOutputChan() chan events.LookatchEvent {
	return s.OutputChannel
}

//GetCommitChan return commit channel attach to source
func (s *Syslog) GetCommitChan() chan interface{} {
	return s.CommitChannel
}

// GetStatus get source status
func (s *Syslog) GetStatus() interface{} {
	return SourceStatusRunning
}

// IsEnable check if source is enable
func (s *Syslog) IsEnable() bool {
	return true
}

// GetStatus returns the collector's source status
func (s *Syslog) HealthCheck() bool {
	return true
}

// GetCapabilities returns available actions
func (s *Syslog) GetCapabilities() map[string]*utils.TaskDescription {
	return nil
}

// Process action
func (s *Syslog) Process(action string, params ...interface{}) interface{} {
	return errors.New("task not implemented")
}
