package core

import (
	"bytes"
	"strings"

	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/Pirionfr/lookatch-agent/sinks"
	"github.com/Pirionfr/lookatch-agent/sources"
	"github.com/Pirionfr/lookatch-agent/utils"

	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/juju/errors"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/google/uuid"
)

// Possible Statuses
const (
	AgentStatusStarting       = "STARTING"
	AgentStatusWaitingForConf = "WAITING_FOT_CONFIGURATION"
	AgentStatusRegistred      = "REGISTRED"
	AgentStatusUnRegistred    = "UNREGISTRED"
	AgentStatusOnline         = "ONLINE"
	AgentStatusOffline        = "OFFLINE"
	AgentStatusOnError        = "ON_ERROR"
)

// Agent representation of agent
type (
	Agent struct {
		sync.RWMutex
		config         *viper.Viper
		tenant         *events.LookatchTenantInfo
		hostname       string
		uuid           uuid.UUID
		srcMutex       sync.RWMutex
		sources        map[string]sources.SourceI
		sinksMutex     sync.RWMutex
		sinks          map[string]sinks.SinkI
		multiplexers   map[string]*Multiplexer
		deMultiplexers map[string]*DeMultiplexer
		controller     *Controller
		stopper        chan error
		encryptionKey  string
		status         string
		processingTask bool
	}
)

// newAgent creates a new agent using the given viper configuration
func newAgent(config *viper.Viper, s chan error) (a *Agent) {
	var controller *Controller
	status := AgentStatusStarting

	//check standalone mode
	if config.Get("controller") != nil {
		auth := NewAuth(
			config.GetString("agent.uuid"),
			config.GetString("agent.password"),
			config.GetString("controller.base_url"))
		log.Info("Starting agent in connected mode")
		status = AgentStatusWaitingForConf
		controller = NewControllerClient(config.Sub("controller"), auth)
	} else {
		log.Info("Starting agent in standalone mode")
	}

	a = &Agent{
		hostname:       config.GetString("agent.hostname"),
		config:         config,
		sources:        make(map[string]sources.SourceI),
		sinks:          make(map[string]sinks.SinkI),
		multiplexers:   make(map[string]*Multiplexer),
		deMultiplexers: make(map[string]*DeMultiplexer),
		encryptionKey:  config.GetString("agent.encryptionKey"),
		tenant: &events.LookatchTenantInfo{
			ID:  config.GetString("agent.uuid"),
			Env: config.GetString("agent.env"),
		},
		controller: controller,
		stopper:    s,
		status:     status,
	}

	a.uuid, _ = uuid.Parse(a.config.GetString("agent.uuid"))

	return a
}

// Run agent
// if controller part is present in config file
// remote will be init
// else agent will be standalone mode
func Run(config *viper.Viper, s chan error) (err error) {
	a := newAgent(config, s)
	a.healthCheckChecker()
	if config.Get("controller") != nil {
		err = a.RemoteInit()
	} else {
		err = a.InitAgent()
	}
	if err != nil {
		return err
	}

	err = a.Start()
	if err != nil {
		return err
	}

	a.status = AgentStatusOnline
	return nil
}

// RemoteInit init controller
// get configuration and meta from remote server
// send capabilities and schema to remote server
func (a *Agent) RemoteInit() error {
	log.Info("Waiting for configuration...")
	binconf, err := a.controller.GetConfiguration()
	if err != nil {
		return err
	}

	//get config from controller
	err = a.updateConfig(binconf)
	if err != nil {
		return err
	}

	err = a.InitAgent()
	if err != nil {
		return err
	}

	a.InitRemoteMeta()

	//send capability to controller
	err = a.SendCapabilities()
	if err != nil {
		return err
	}

	//send schema to controller
	sourceSchema := a.GetSchemas()
	for k, v := range sourceSchema {
		err = a.controller.SendSchema(k, v)
		if err != nil {
			return err
		}
	}

	go a.Poller()

	return err
}

// InitRemoteMeta get meta from remote and assign it to sources
func (a *Agent) InitRemoteMeta() {
	metas, err := a.controller.GetMeta("")
	if err != nil {
		return
	}

	for k, v := range a.getSources() {
		v.Process(utils.SourceMeta, metas.Sources[k])
	}
}

// updateConfig config will be merge with the current config
func (a *Agent) updateConfig(b []byte) (err error) {
	err = a.config.MergeConfig(bytes.NewReader(b))

	if err != nil {
		return
	}
	log.Info("Configuration updated")
	a.status = AgentStatusStarting
	a.encryptionKey = a.config.GetString("agent.encryptionKey")
	return
}

// InitAgent prepare agent from conf
// init multiplexer , source and sinks
func (a *Agent) InitAgent() (err error) {
	//multiplexer prepare
	multiplexer := make(map[string][]string)
	//LoadLsnDemux prepare
	LoadDemux := make(map[string][]string)

	//load sinks
	err = a.LoadSinks()
	if err != nil {
		log.WithError(err).Error("Error While Loading Sinks")
		return
	}

	//load sources
	err = a.LoadSources(&multiplexer, &LoadDemux)
	if err != nil {
		log.WithError(err).Error("Error While Loading Source")
		return
	}

	//Load DeMultiplexer
	err = a.LoadDeMultiplexer(&LoadDemux)
	if err != nil {
		log.WithError(err).Error("Error While Loading Multiplexer")
		return
	}

	//loadMultiplexer
	err = a.LoadMultiplexer(&multiplexer)
	if err != nil {
		log.WithError(err).Error("Error While Loading Multiplexer")
		return
	}

	return nil
}

func (a *Agent) Start() error {

	for _, sink := range a.sinks {
		err := sink.Start()
		if err != nil {
			return err
		}
	}

	for _, source := range a.sources {
		err := source.Start()
		if err != nil {
			return err
		}
	}
	return nil
}

// LoadSources Load all Sources
// init sources from conf
func (a *Agent) LoadSources(multiplexer *map[string][]string, demux *map[string][]string) (err error) {
	//load sources
	for srcName := range a.config.GetStringMap("sources") {
		if !a.config.GetBool("sources." + srcName + ".enabled") {
			continue
		}
		if typeSource := a.config.GetString("sources." + srcName + ".type"); typeSource != "" {
			err = a.LoadSource(srcName, typeSource)
			if err != nil {
				return errors.Annotate(err, "error loading source")
			}
		} else {
			return errors.Errorf("source type not found for '%s'", srcName)
		}

		//fill multiplexer and Lsndemux
		if linkedSinks := a.config.GetStringSlice("sources." + srcName + ".linked_sinks"); linkedSinks != nil {
			(*multiplexer)[srcName] = a.config.GetStringSlice("sources." + srcName + ".linked_sinks")
			(*demux)[srcName] = a.config.GetStringSlice("sources." + srcName + ".linked_sinks")
		} else {
			return errors.Errorf("Linked sinks not set for '%s'", srcName)
		}
	}

	//check sources
	if len(a.getSources()) == 0 {
		err = errors.New("No sources found")
	}
	return err
}

// LoadSource create Source from name
func (a *Agent) LoadSource(sourceName string, sourceType string) (err error) {
	defer errors.DeferredAnnotatef(&err, "LoadSource()")

	//check source
	_, ok := a.getSource(sourceName)
	if ok {
		return errors.New(sourceName + ".Source already exists")
	}
	//create sources
	aSource, err := sources.New(sourceName, sourceType, a.config)
	if err != nil {
		return errors.Annotatef(err, "error creating new source")
	}
	a.setSource(sourceName, aSource)
	return
}

// LoadSinks Load all sinks
// init sinks from conf
func (a *Agent) LoadSinks() (err error) {
	for sinkName := range a.config.GetStringMap("sinks") {
		if !a.config.GetBool("sinks." + sinkName + ".enabled") {
			log.WithField("sinkName", sinkName).Debug("Sink not enabled, it will be omitted")
			continue
		}
		if typeSource := a.config.GetString("sinks." + sinkName + ".type"); typeSource != "" {
			err = a.LoadSink(sinkName, typeSource)
			if err != nil {
				return errors.Annotate(err, "error loading sink")
			}
		} else {
			return errors.Errorf("sink type not found for '%s'", sinkName)
		}
	}

	if len(a.getSinks()) == 0 {
		err = errors.New("No sinks found")
	}
	return
}

// LoadSink create sink from name
func (a *Agent) LoadSink(sinkName string, sinkType string) (err error) {
	defer errors.DeferredAnnotatef(&err, "LoadSink()")

	//check source
	_, ok := a.getSink(sinkName)
	if ok {
		return errors.New(sinkName + ". Source already exists")
	}
	//create sources
	aSink, err := sinks.New(sinkName, sinkType, a.config, a.stopper)
	if err != nil {
		return errors.Annotatef(err, "error creating new sink")
	}

	a.setSink(sinkName, aSink)

	return
}

// LoadMultiplexer setup Multiplexer
// each source as is own multiplexer
func (a *Agent) LoadMultiplexer(multiplexer *map[string][]string) error {
	for sourceName, sinkList := range *multiplexer {
		var sinksChan []chan events.LookatchEvent
		src, found := a.getSource(sourceName)
		if !found {
			return errors.Errorf("Source '%s' not found\n", sourceName)
		}

		for _, sinkName := range sinkList {
			aSink, found := a.getSink(sinkName)
			if !found {
				return errors.Errorf("sink name '%s' not found\n", sinkName)
			}
			sinksChan = append(sinksChan, aSink.GetInputChan())
			log.WithFields(
				log.Fields{
					"sourceName": sourceName,
					"sinkName":   sinkName,
				}).Debug("create link")
		}
		a.multiplexers[sourceName] = NewMultiplexer(src.GetOutputChan(), sinksChan)
	}
	return nil
}

// LoadDeMultiplexer setup DeMultiplexer
// each source has its own DeMultiplexer
func (a *Agent) LoadDeMultiplexer(demux *map[string][]string) error {
	for sourceName, sinkList := range *demux {
		var sinksChan []chan interface{}
		src, found := a.getSource(sourceName)
		if !found {
			return errors.Errorf("Source '%s' not found\n", sourceName)
		}

		for _, sinkName := range sinkList {
			aSink, found := a.getSink(sinkName)
			if !found {
				return errors.Errorf("sink name '%s' not found\n", sinkName)
			}
			sinksChan = append(sinksChan, aSink.GetCommitChan())
		}
		a.deMultiplexers[sourceName] = NewDemultiplexer(sinksChan, src.GetCommitChan())

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

// HealthCheck returns true if agent and all source are up
func (a *Agent) HealthCheck() (alive bool) {
	if a.status == AgentStatusStarting {
		return true
	}

	sourceList := a.getSources()
	for _, source := range sourceList {
		if !source.HealthCheck() {
			return false
		}
	}
	return true
}

// getSourceCapabilities get source capabilities from source
// return TaskDescription for each source
func (a *Agent) getSourceCapabilities() map[string]map[string]*utils.TaskDescription {
	var sourceAction = make(map[string]map[string]*utils.TaskDescription)
	sourceList := a.getSources()
	for _, source := range sourceList {
		sourceAction[source.GetName()] = source.GetCapabilities()
	}

	return sourceAction
}

// getCapabilities get agent Capabilities
// return it as TaskDescription map
func (a *Agent) getCapabilities() map[string]*utils.TaskDescription {
	var action = make(map[string]*utils.TaskDescription)
	action[utils.AgentStart] = utils.DeclareNewTaskDescription(nil, "Start agent")
	action[utils.AgentStop] = utils.DeclareNewTaskDescription(nil, "Stop agent")
	action[utils.AgentRestart] = utils.DeclareNewTaskDescription(nil, "Restart agent")
	return action
}

// getSourceMeta get source meta from sources
// return Metas for each source
func (a *Agent) getSourceMeta() map[string]map[string]utils.Meta {
	var sourceMeta = make(map[string]map[string]utils.Meta)
	sourceList := a.getSources()
	for _, source := range sourceList {
		sourceMeta[source.GetName()] = source.GetMeta()
	}

	return sourceMeta
}

// getSourceStatus get status from sources
// return status of each source as Meta
func (a *Agent) getSourceStatus() map[string]utils.Meta {
	var sourceStatus = make(map[string]utils.Meta)
	sourceList := a.getSources()
	for _, source := range sourceList {
		sourceStatus[source.GetName()] = utils.NewMeta("status", source.GetStatus())
	}

	return sourceStatus
}

// GetSchemas get schema from sources
func (a *Agent) GetSchemas() map[string]map[string]map[string]*sources.Column {
	var sourceSchemas = make(map[string]map[string]map[string]*sources.Column)
	sourceList := a.getSources()
	for _, source := range sourceList {
		sourceSchemas[source.GetName()] = source.GetSchema()
	}

	return sourceSchemas
}

// healthCheckChecker start health check endpoint
// check status of agent and return 200 if ok and 503 if navailable
func (a *Agent) healthCheckChecker() {
	port := a.config.GetInt("agent.healthport")

	log.WithField("port", port).Debug("Starting health check")
	var wg sync.WaitGroup
	wg.Add(1)
	http.HandleFunc("/health/status", func(w http.ResponseWriter, r *http.Request) {
		if a.HealthCheck() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	go func() {
		wg.Done()
		err := http.ListenAndServe(":"+strconv.Itoa(port), nil)
		if err != nil {
			log.WithError(err).Error("Enable to start health check server")
		}
	}()
	wg.Wait()
	url := fmt.Sprintf("http://localhost:%d/health/status", port)
	request, _ := http.NewRequest("GET", url, nil)
	client := &http.Client{}
	r, err := client.Do(request)
	if r != nil {
		defer r.Body.Close()
	}
	if err != nil && r == nil {
		log.WithError(err).Error("healthcheck webserver error")
	}
}

// Poller send meta and get task at each tick
// on every tick call sendMetaAndGetProcessTask
func (a *Agent) Poller() {
	wait, err := time.ParseDuration(a.controller.conf.PollerTicker)
	if err != nil {
		log.WithError(err).Fatal("Error while parsing ticker duration")
	}
	ticker := time.NewTicker(wait)

	for range ticker.C {
		err := a.sendMetaAndGetProcessTask()
		if err != nil {
			log.WithError(err).Error("error while polling meta")
		}
	}
}

// sendMetaAndGetProcessTask send meta and get task
// send meta of agent and sources and get pending task number
// if number is > 0 get the oldest task in TODO, , PENDING or IN_PROGRESS status
// from server and handle it
func (a *Agent) sendMetaAndGetProcessTask() (err error) {
	metas := utils.NewMetas()
	log.Debug("Send Meta")

	//get meta from sources
	metas.Sources = a.getSourceMeta()
	for k, v := range a.getSourceStatus() {
		metas.SetMetaSources(k, v)
	}
	//set status
	metas.Agent["status"] = utils.NewMeta("status", a.status)
	metas.Agent["version"] = utils.NewMeta("version", a.config.GetString("agent.version"))
	metas.Agent["date"] = utils.NewMeta("date", a.config.GetString("agent.date"))
	err = a.controller.SendMeta(metas)
	if err != nil {
		return
	}

	//get task and process it if pending task
	if a.controller.PendingTask != 0 {
		taskList, err := a.controller.GetTasks(1)
		if err != nil {
			return err
		}
		if !a.processingTask {
			go func() {
				err := a.ProcessTask(taskList[0])
				if err != nil {
					log.WithError(err).Error("Task failed")
				}
			}()
		}
	}

	return err
}

// SendCapabilities send capability to server
func (a *Agent) SendCapabilities() (err error) {
	err = a.controller.SendCapabilities(a.getCapabilities())
	if err != nil {
		return err
	}

	sourceTask := a.getSourceCapabilities()
	for k, v := range sourceTask {
		if v != nil {
			err = a.controller.SendSourcesCapabilities(k, v)
			if err != nil {
				return err
			}
		}
	}
	return
}

// ProcessTask process given task from server and update status
func (a *Agent) ProcessTask(task utils.Task) (err error) {
	defer func() {
		task.EndDate = time.Now().Unix()
		if err != nil {
			task.Status = utils.TaskOnError
			task.ErrorDetails = err.Error()
			log.WithError(err).Error("Error while Processing task")
		} else {
			task.Status = utils.TaskDone
		}
		err = a.controller.UpdateTasks(task)
		if err != nil {
			log.WithError(err).Error("Error while Updating task")
		}
		a.processingTask = false
	}()

	log.WithFields(log.Fields{
		"taskId": task.ID,
		"target": task.Target,
		"type":   task.TaskType,
		"params": task.Parameters,
	}).Info("run task")

	// update status
	a.processingTask = true
	task.Status = utils.TaskRunning
	task.StartDate = time.Now().Unix()
	err = a.controller.UpdateTasks(task)
	if err != nil {
		log.WithError(err).Error("Error while Updating task")
	}

	target := strings.Split(task.Target, "::")
	//handle source task
	if target[0] == "sources" {
		s, ok := a.getSource(target[1])
		if !ok {
			err = errors.New("source name not found")
			return
		}
		switch task.TaskType {
		case utils.SourceStop:
			err = s.Stop()

		case utils.SourceStart:
			err = s.Start()

		case utils.SourceRestart:
			if err = s.Stop(); err == nil {
				err = s.Start()
			}
		default:
			result := s.Process(task.TaskType, task.Parameters)
			errProcess, ok := result.(error)
			if ok {
				err = errProcess
			}
		}
	}
	return nil
}
