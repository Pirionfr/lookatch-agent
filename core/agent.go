package core

import (
	"bytes"

	"github.com/Pirionfr/lookatch-agent/sinks"
	"github.com/Pirionfr/lookatch-agent/sources"
	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	"github.com/juju/errors"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
	"fmt"
)

// Agent representation of agent
type Agent struct {
	sync.RWMutex
	config       *viper.Viper
	tenant       *events.LookatchTenantInfo
	hostname     string
	uuid         uuid.UUID
	srcMutex     sync.RWMutex
	sources      map[string]sources.SourceI
	sinksMutex   sync.RWMutex
	sinks        map[string]sinks.SinkI
	multiplexers map[string]*Multiplexer
	controller   *Controller
	stopper      chan error
	secretkey    string
	status       string
}

// Agent create new agent
func newAgent(config *viper.Viper, s chan error) (a *Agent, err error) {
	var controller *Controller
	status := control.AgentStatusStarting

	log.SetOutput(os.Stdout)
	//check log level
	level := config.GetString("agent.loglevel")
	if err != nil {
		log.WithFields(log.Fields{
			"level": config.Get("agent.loglevel"),
		}).Error("Error while retrieving LogLevel")
	} else {
		log.ParseLevel(level)
		log.WithFields(log.Fields{
			"level": config.Get("agent.loglevel"),
		}).Info("Agent.run()")
	}

	//check standalone mode
	if config.Get("auth") != nil {
		auth := newAuth(
			config.GetString("agent.tenant"),
			config.GetString("agent.uuid"),
			config.GetString("agent.secretkey"),
			config.GetString("agent.hostname"),
			config.GetString("auth.service"))
		log.Debug("Starting agent in connected mode")
		status = control.AgentStatusWaitingForConf
		controller = NewControllerClient(config.Sub("controller"), auth)

	} else {
		log.Debug("Starting agent in standlone mode")
	}

	a = &Agent{
		hostname:     config.GetString("agent.hostname"),
		config:       config,
		sources:      make(map[string]sources.SourceI),
		sinks:        make(map[string]sinks.SinkI),
		multiplexers: make(map[string]*Multiplexer),
		secretkey:    config.GetString("agent.secretkey"),
		tenant: &events.LookatchTenantInfo{
			Id:  config.GetString("agent.tenant"),
			Env: config.GetString("agent.env"),
		},
		controller: controller,
		stopper:    s,
		status:     status,
	}

	a.uuid, _ = uuid.FromString(a.config.GetString("agent.uuid"))

	return
}

// Run run agent
func Run(config *viper.Viper, s chan error) (err error) {
	a, err := newAgent(config, s)
	if err != nil {
		return
	}
	if config.Get("auth") != nil {
		err = a.ControllerStart()
	} else {
		err = a.InitConfig()
	}
	a.healthCheckChecker()

	return
}

// ControllerStart Start Controller
func (a *Agent) ControllerStart() error {

	a.controller.StartChannel()

	go a.controller.RecvMessage(a.controller.recv)

	go a.HandleMessage(a.controller.recv)

	//init
	err := a.GetConfig()
	if err != nil {
		return err
	}

	return nil
}

// updateConfig update Config
func (a *Agent) updateConfig(b []byte) (err error) {
	err = a.config.MergeConfig(bytes.NewReader(b))

	if err != nil {
		return
	}
	log.Info("Configuration updated")
	a.status = control.AgentStatusOnline
	err = a.InitConfig()
	return
}

// InitConfig Init Config
func (a *Agent) InitConfig() (err error) {
	//multiplexer prepare
	multiplexer := make(map[string][]string)

	//load sources
	err = a.LoadSources(&multiplexer)
	if err != nil {
		log.Error("Error While Loading Source")
		return
	}

	//load sinks
	err = a.LoadSinks()
	if err != nil {
		log.Error("Error While Loading Sinks")
		return
	}

	//loadMultiplexer
	a.LoadMultiplexer(&multiplexer)

	return
}

// LoadSources Load Sources
func (a *Agent) LoadSources(multiplexer *map[string][]string) (err error) {
	//load sources
	for name := range a.config.GetStringMap("sources") {
		eventChan := make(chan *events.LookatchEvent, 10000)
		if !a.config.GetBool("sources." + name + ".enabled") {
			continue
		}
		if typeSource := a.config.GetString("sources." + name + ".type"); typeSource != "" {
			err = a.LoadSource(name, typeSource, eventChan)
			if err != nil {
				return errors.Annotate(err, "error loading source")
			}
		} else {
			return errors.Errorf("source type not found for '%s'", name)
		}
		//fill multiplexer
		(*multiplexer)[name] = a.config.GetStringSlice("sources." + name + ".sinks")
	}

	//check sources
	if len(a.getSources()) == 0 {
		err = errors.New("No sources found")
	}
	return err
}

// LoadSource Load Source from name
func (a *Agent) LoadSource(sourceName string, sourceType string, eventChan chan *events.LookatchEvent) (err error) {
	defer errors.DeferredAnnotatef(&err, "LoadSource()")

	//check source
	_, ok := a.getSource(sourceName)
	if ok {
		return errors.New(sourceName + ".Source already exists")
	}
	//create sources
	aSource, err := sources.New(sourceName, sourceType, a.config, eventChan)
	if err != nil {
		return errors.Annotatef(err, "error creating new source")
	}
	a.setSource(sourceName, aSource)
	return
}

// LoadSinks Load Sinks
func (a *Agent) LoadSinks() (err error) {
	for name := range a.config.GetStringMap("sinks") {
		eventChan := make(chan *events.LookatchEvent, 10000)
		if !a.config.GetBool("sinks." + name + ".enabled") {
			log.WithFields(log.Fields{
				"name": name,
			}).Debug("Sink not enabled, it will be omitted")
			continue
		}
		if typeSource := a.config.GetString("sinks." + name + ".type"); typeSource != "" {
			err = a.LoadSink(name, typeSource, eventChan)
			if err != nil {
				return errors.Annotate(err, "error loading sink")
			}
		} else {
			return errors.Errorf("sink type not found for '%s'", name)
		}
	}

	if len(a.getSinks()) == 0 {
		err = errors.New("No sinks found")
	}
	return
}

// LoadSink Load Sinks from name
func (a *Agent) LoadSink(sinkName string, sinkType string, eventChan chan *events.LookatchEvent) (err error) {
	defer errors.DeferredAnnotatef(&err, "LoadSink()")
	//check source
	_, ok := a.getSink(sinkName)
	if ok {
		return errors.New(sinkName + ". Source already exists")
	}
	//create sources
	aSink, err := sinks.New(sinkName, sinkType, a.config, a.stopper, eventChan)
	if err != nil {
		return errors.Annotatef(err, "error creating new sink")
	}

	err = aSink.Start()
	if err != nil {
		return errors.Annotatef(err, "error starting sink")
	}

	a.setSink(sinkName, aSink)

	return
}

// LoadMultiplexer Load Multiplexer
func (a *Agent) LoadMultiplexer(multiplexer *map[string][]string) error {

	for sourceName, sinkList := range *multiplexer {
		var sinksChan []chan *events.LookatchEvent
		for _, sinkName := range sinkList {
			aSink, found := a.getSink(sinkName)
			if found {
				sinksChan = append(sinksChan, aSink.GetInputChan())
			} else {
				return errors.Errorf("sink name '%s' not found\n", sinkName)
			}
		}

		src, found := a.getSource(sourceName)
		if found {
			a.multiplexers[sourceName] = NewMultiplexer(src.GetOutputChan(), sinksChan)
		} else {
			return errors.Errorf("Source '%s' not found\n", sourceName)
		}
	}
	return nil
}

// getSources get all sources
func (a *Agent) getSources() map[string]sources.SourceI {
	a.srcMutex.RLock()
	src := a.sources
	a.srcMutex.RUnlock()
	return src
}

// getSources get initialised source from name
func (a *Agent) getSource(sourceName string) (sources.SourceI, bool) {
	a.srcMutex.RLock()
	src, ok := a.sources[sourceName]
	a.srcMutex.RUnlock()
	return src, ok
}

// setSources add new source
func (a *Agent) setSource(sourceName string, src sources.SourceI) {
	a.srcMutex.Lock()
	a.sources[sourceName] = src
	a.srcMutex.Unlock()
}

// delSource delete source from name
func (a *Agent) delSource(sourceName string) {
	a.srcMutex.Lock()
	delete(a.sources, sourceName)
	a.srcMutex.Unlock()
}

// getSinks get all sinks
func (a *Agent) getSinks() map[string]sinks.SinkI {
	a.sinksMutex.RLock()
	sink := a.sinks
	a.sinksMutex.RUnlock()
	return sink
}

// getSink from name
func (a *Agent) getSink(sinkName string) (sinks.SinkI, bool) {
	a.sinksMutex.RLock()
	sink, ok := a.sinks[sinkName]
	a.sinksMutex.RUnlock()
	return sink, ok
}

// setSink add new sink
func (a *Agent) setSink(sinkName string, s sinks.SinkI) {
	a.sinksMutex.Lock()
	a.sinks[sinkName] = s
	a.sinksMutex.Unlock()
}

// delSink delete sink from name
func (a *Agent) delSink(sinkName string) {
	a.sinksMutex.Lock()
	delete(a.sinks, sinkName)
	a.sinksMutex.Unlock()
}

// HealthCheck returns true if agent and all source are up
func (a *Agent) HealthCheck() (alive bool) {
	alive = true
	sourceList := a.getSources()
	for _, source := range sourceList {
		if !source.HealthCheck() {
			return false
		}
	}
	if a.controller != nil {
		alive = a.controller.Status != "READY"
	}
	return alive
}

// getSourceAvailableAction create message of source action
func (a *Agent) getSourceAvailableAction() *control.Agent {
	var sourceAction = make(map[string]map[string]*control.ActionDescription)
	sourceList := a.getSources()
	for _, source := range sourceList {
		sourceAction[source.GetName()] = source.GetAvailableActions()
	}
	aCtrl := &control.Agent{}
	return aCtrl.NewMessage(a.tenant.Id, a.uuid.String(), control.SourceAvailableAction).WithPayload(sourceAction)

}

// getAvailableAction create message of agent action
func (a *Agent) getAvailableAction() *control.Agent {
	var action = make(map[string]*control.ActionDescription)
	action[control.AgentStart] = control.DeclareNewAction(nil, "Start agent")
	action[control.AgentStop] = control.DeclareNewAction(nil, "Stop agent")
	action[control.AgentRestart] = control.DeclareNewAction(nil, "Restart agent")
	aCtrl := &control.Agent{}
	return aCtrl.NewMessage(a.tenant.Id, a.uuid.String(), control.AgentAvailableAction).WithPayload(action)

}

// getSourceMeta create message of source metadata
func (a *Agent) getSourceMeta() *control.Agent {
	var sourceMeta = make(map[string]*control.Meta)
	sourceList := a.getSources()
	for _, source := range sourceList {
		sourceMeta[source.GetName()] = &control.Meta{
			Timestamp: strconv.Itoa(int(time.Now().Unix())),
			Data:      source.GetMeta(),
		}
	}
	aCtrl := &control.Agent{}
	return aCtrl.NewMessage(a.tenant.Id, a.uuid.String(), control.SourceMeta).WithPayload(sourceMeta)

}

// getSourceStatus create message of source status
func (a *Agent) getSourceStatus() *control.Agent {
	var sourceStatus = make(map[string]control.Status)
	sourceList := a.getSources()
	for _, source := range sourceList {
		sourceStatus[source.GetName()] = control.Status{
			Code: source.GetStatus(),
		}
	}
	agentCtrl := &control.Agent{}
	return agentCtrl.NewMessage(a.tenant.Id, a.uuid.String(), control.SourceStatus).WithPayload(sourceStatus)

}

// GetSchemas create message of schemas
func (a *Agent) GetSchemas() *control.Agent {
	var sourceStatus = make(map[string]control.Schema)
	sourceList := a.getSources()
	for _, source := range sourceList {
		sourceStatus[source.GetName()] = control.Schema{
			Timestamp: strconv.Itoa(int(time.Now().Unix())),
			Raw:       source.GetSchema(),
		}
	}
	agentCtrl := &control.Agent{}
	return agentCtrl.NewMessage(a.tenant.Id, a.uuid.String(), control.SourceSchema).WithPayload(sourceStatus)
}

// healthCheckChecker start health check endpoint
func (a *Agent) healthCheckChecker() {
	port := a.config.GetInt("agent.healthport")
	log.WithFields(log.Fields{
		"port": port,
	}).Debug("Starting health check")
	var wg sync.WaitGroup
	wg.Add(1)
	http.HandleFunc("/health/status", func(w http.ResponseWriter, r *http.Request) {
		if a.HealthCheck() {
			w.WriteHeader(200)
		} else {
			w.WriteHeader(503)
		}
	})
	go func() {
		wg.Done()
		http.ListenAndServe(":"+ strconv.Itoa(port), nil)
	}()
	wg.Wait()
	url := fmt.Sprintf("http://localhost:%d/health/status", port)
	request, _ := http.NewRequest("GET", url, nil)
	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil && resp == nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("healthcheck webserver error :")
	}

}
