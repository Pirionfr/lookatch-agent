package sources

import (
	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	"github.com/juju/errors"
	"github.com/spf13/viper"
)

type (
	AgentHeader struct {
		tenant   events.LookatchTenantInfo
		hostname string
		uuid     string
	}
)

type (
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

	Source struct {
		Name          string
		OutputChannel chan *events.LookatchEvent
		AgentInfo     *AgentHeader
		IsEnable      bool
		Conf          *viper.Viper
		Offset        int64
	}
)

type sourceCreatorT func(*Source) (SourceI, error)

var factory = map[string]sourceCreatorT{
	DummyType:           newDummy,
	RandomType:          newRandom,
	MysqlQueryType:      newMysqlQuery,
	MysqlCDCType:        newMysqlCdc,
	PostgreSQLQueryType: newPostgreSQLQuery,
	PostgreSQLCDCType:   newPostgreSQLCdc,
	MSSQLQueryType:      newMSSQLQuery,
}

func New(name string, sourceType string, config *viper.Viper, eventChan chan *events.LookatchEvent) (s SourceI, err error) {

	//setup agentHeader
	agentInfo := &AgentHeader{
		tenant: events.LookatchTenantInfo{
			Id:  config.GetString("agent.tenant"),
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
	s.Init()

	if config.GetBool("sources." + name + ".autostart") {
		go s.Start()
	}

	return
}
