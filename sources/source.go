package sources

import (
	"github.com/Pirionfr/lookatch-agent/control"
	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/juju/errors"
	"github.com/spf13/viper"
)

type (
	// AgentHeader representation of Agent Header
	AgentHeader struct {
		tenant   events.LookatchTenantInfo
		hostname string
		uuid     string
	}
)

type (
	// SourceI interface
	SourceI interface {
		Init()
		Stop() error
		Start(...interface{}) error
		GetName() string
		GetOutputChan() chan *events.LookatchEvent
		GetMeta() map[string]interface{}
		GetSchema() interface{}
		GetStatus() interface{}
		IsEnable() bool
		HealthCheck() bool
		GetAvailableActions() map[string]*control.ActionDescription
		Process(string, ...interface{}) interface{}
	}

	// Source representation of source
	Source struct {
		Name          string
		OutputChannel chan *events.LookatchEvent
		AgentInfo     *AgentHeader
		IsEnable      bool
		Conf          *viper.Viper
		Offset        int64
	}
)

// sourceCreatorT source Creator func
type sourceCreatorT func(*Source) (SourceI, error)

// factory source factory
var factory = map[string]sourceCreatorT{
	RandomType:              newRandom,
	MysqlQueryType:          newMysqlQuery,
	MysqlCDCType:            newMysqlCdc,
	PostgreSQLQueryType:     newPostgreSQLQuery,
	PostgreSQLCDCType:       newPostgreSQLCdc,
	MSSQLQueryType:          newMSSQLQuery,
	SyslogType:              newSyslog,
	FileReadingFollowerType: newFileReadingFollower,
}

// New create new source
func New(name string, sourceType string, config *viper.Viper, eventChan chan *events.LookatchEvent) (s SourceI, err error) {

	//setup agentHeader
	agentInfo := &AgentHeader{
		tenant: events.LookatchTenantInfo{
			ID:  config.GetString("agent.tenant"),
			Env: config.GetString("agent.env"),
		},
		hostname: config.GetString("agent.hostname"),
		uuid:     config.GetString("agent.uuid"),
	}

	sourceCreatorFunc, found := factory[sourceType]
	if !found {
		return nil, errors.Errorf("Source type not found '%s'", sourceType)
	}

	if !config.IsSet("sources." + name) {
		return nil, errors.Errorf("no custom config found for source name '%s'", name)
	}

	baseSrc := &Source{
		Name:          name,
		OutputChannel: eventChan,
		AgentInfo:     agentInfo,
		Conf:          config,
		Offset:        0,
	}

	s, err = sourceCreatorFunc(baseSrc)
	if err != nil {
		return
	}
	s.Init()

	if config.GetBool("sources." + name + ".autostart") {
		go s.Start()
	}

	return
}
