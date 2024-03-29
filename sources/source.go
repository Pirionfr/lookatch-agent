package sources

import (
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/guregu/null.v3"

	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/Pirionfr/lookatch-agent/utils"
)

// Possible Statuses
const (
	// Source Possible Statuses
	SourceStatusOnError        = "ON_ERROR"
	SourceStatusRunning        = "RUNNING"
	SourceStatusWaiting        = "WAITING"
	SourceStatusWaitingForMETA = "WAITING_FOR_META"
	SourceStatusInit           = "INITIALIZING"

	DefaultChannelSize = 100
)

type (
	// AgentHeader representation of Agent Header
	// contain agent auth information for events
	AgentHeader struct {
		Tenant   events.LookatchTenantInfo
		Hostname string
		UUID     string
	}

	// Column representation of schema
	Column struct {
		Database               string   `json:"database,omitempty"`
		Schema                 string   `json:"schema,omitempty"`
		Table                  string   `json:"table,omitempty"`
		Column                 string   `json:"column"`
		ColumnOrdPos           int      `json:"column_ord_pos"`
		Nullable               bool     `json:"nullable"`
		DataType               string   `json:"data_type"`
		CharacterMaximumLength null.Int `json:"character_maximum_length,omitempty"`
		NumericPrecision       null.Int `json:"numeric_precision,omitempty"`
		NumericScale           null.Int `json:"numeric_scale,omitempty"`
		ColumnType             string   `json:"column_type"`
		ColumnKey              string   `json:"column_key,omitempty"`
	}
)

type (
	// SourceI interface of source
	SourceI interface {
		Init()
		Stop() error
		Start(...interface{}) error
		GetName() string
		GetOutputChan() chan events.LookatchEvent
		GetCommitChan() chan interface{}
		UpdateCommittedLsn()
		GetMeta() map[string]utils.Meta
		GetSchema() map[string]map[string]*Column
		GetStatus() interface{}
		IsEnable() bool
		HealthCheck() bool
		GetCapabilities() map[string]*utils.TaskDescription
		Process(string, ...interface{}) interface{}
	}

	// Source representation of source
	Source struct {
		Name          string
		OutputChannel chan events.LookatchEvent
		CommitChannel chan interface{}
		AgentInfo     *AgentHeader
		Conf          *viper.Viper
		Offset        int64
		Status        string
	}
)

// sourceCreatorT source Creator func
type sourceCreatorT func(*Source) (SourceI, error)

// Factory source Factory
var Factory = map[string]sourceCreatorT{
	RandomType:              NewRandom,
	MysqlQueryType:          NewMysqlQuery,
	MysqlCDCType:            NewMysqlCdc,
	PostgreSQLQueryType:     NewPostgreSQLQuery,
	PostgreSQLCDCType:       NewPostgreSQLCdc,
	SqlserverQueryType:      NewSqlserverSQLQuery,
	SqlserverCDCType:        NewSqlserverCDC,
	SyslogType:              NewSyslog,
	FileReadingFollowerType: NewFileReadingFollower,
}

// New create new source
func New(name string, sourceType string, config *viper.Viper) (s SourceI, err error) {
	//setup agentHeader
	agentInfo := &AgentHeader{
		Tenant: events.LookatchTenantInfo{
			ID:  config.GetString("agent.UUID"),
			Env: config.GetString("agent.env"),
		},
		Hostname: config.GetString("agent.Hostname"),
		UUID:     config.GetString("agent.UUID"),
	}

	sourceCreatorFunc, found := Factory[sourceType]
	if !found {
		return nil, errors.Errorf("Source type not found '%s'", sourceType)
	}

	if !config.IsSet("sources." + name) {
		return nil, errors.Errorf("no custom config found for source name '%s'", name)
	}
	channelSize := DefaultChannelSize
	if !config.IsSet("sources." + name + ".chan_size") {
		channelSize = config.GetInt("sources." + name + ".chan_size")
	}
	eventChan := make(chan events.LookatchEvent, channelSize)
	commitChan := make(chan interface{}, channelSize)

	baseSrc := &Source{
		Name:          name,
		OutputChannel: eventChan,
		CommitChannel: commitChan,
		AgentInfo:     agentInfo,
		Conf:          config,
		Offset:        0,
		Status:        SourceStatusWaitingForMETA,
	}

	s, err = sourceCreatorFunc(baseSrc)
	if err != nil {
		return nil, err
	}
	s.Init()
	return s, err
}

// Stop source
func (s *Source) Stop() error {
	return nil
}

// Start source
func (s *Source) Start(i ...interface{}) (err error) {
	s.Status = SourceStatusRunning
	log.Debug("start default UpdateCommittedLsn")
	go s.UpdateCommittedLsn()
	return nil
}

// GetName get name of source
func (s *Source) GetName() string {
	return s.Name
}

// GetOutputChan get output channel
func (s *Source) GetOutputChan() chan events.LookatchEvent {
	return s.OutputChannel
}

// GetCommitChan return commit channel attach to source
func (s *Source) GetCommitChan() chan interface{} {
	return s.CommitChannel
}

// GetMeta returns source meta
func (s *Source) GetMeta() map[string]utils.Meta {
	meta := make(map[string]utils.Meta)
	meta["offset_agent"] = utils.NewMeta("offset_agent", s.Offset)
	return meta
}

// IsEnable check if the configured source is enabled
func (s *Source) IsEnable() bool {
	return true
}

// GetStatus returns the collector's source status
func (s *Source) GetStatus() interface{} {
	return s.Status
}

// HealthCheck returns true if the source is correctly configured and the collector is connected to it
func (s *Source) HealthCheck() bool {
	return s.Status == SourceStatusRunning
}

// Init source
func (s *Source) Init() {

}

// GetCapabilities returns available actions
func (s *Source) GetCapabilities() map[string]*utils.TaskDescription {
	availableAction := make(map[string]*utils.TaskDescription)
	return availableAction
}

// UpdateCommittedLsn do noting avoid deadlock
func (s *Source) UpdateCommittedLsn() {
	for range s.CommitChannel {

	}
}
