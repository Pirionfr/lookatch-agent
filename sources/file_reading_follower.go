package sources

import (
	"encoding/json"
	"io"
	"strconv"
		"time"

	"github.com/Pirionfr/lookatch-agent/control"
	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/Pirionfr/lookatch-agent/util"
	"github.com/papertrail/go-tail/follower"
	log "github.com/sirupsen/logrus"
	"sync"
)

// FileReadingFollower representation of FileReadingFollower
type FileReadingFollower struct {
	*Source
	config FileReadingFollowerConfig
	status string
}

// FileReadingFollowerConfig representation of FileReadingFollower Config
type FileReadingFollowerConfig struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
}

// FileReadingFollowerType type of source
const FileReadingFollowerType = "fileReadingFollower"

// create new FileReadingFollower source
func newFileReadingFollower(s *Source) (SourceI, error) {

	fileReadingFollowerConfig := FileReadingFollowerConfig{}
	s.Conf.UnmarshalKey("sources."+s.Name, &fileReadingFollowerConfig)

	return &FileReadingFollower{
		Source: s,
		config: fileReadingFollowerConfig,
		status: control.SourceStatusWaitingForMETA,
	}, nil
}

// Init source
func (f *FileReadingFollower) Init() {
	if util.IsStandalone(f.Conf) {
		f.status = control.SourceStatusRunning
		f.config.Offset = 0
		f.Offset = 0
	}
}

// Stop source
func (f *FileReadingFollower) Stop() error {
	return nil
}

// Start source
func (f *FileReadingFollower) Start(i ...interface{}) error {

	var wg sync.WaitGroup
	wg.Add(1)
	//wait for changeStatus
	go func() {
		for f.status == control.SourceStatusWaitingForMETA {
			time.Sleep(time.Second)
		}
		wg.Done()
	}()
	wg.Wait()
	go f.read()
	return nil
}

// GetName get source name
func (f *FileReadingFollower) GetName() string {
	return f.Name
}

// GetOutputChan get output channel
func (f *FileReadingFollower) GetOutputChan() chan *events.LookatchEvent {
	return f.OutputChannel
}

// IsEnable check if source is enable
func (f *FileReadingFollower) IsEnable() bool {
	return true
}

// HealthCheck return true if ok
func (f *FileReadingFollower) HealthCheck() bool {
	return f.status == control.SourceStatusRunning
}

// GetMeta get source meta
func (f *FileReadingFollower) GetMeta() map[string]interface{} {
	meta := make(map[string]interface{})
	if f.status != control.SourceStatusWaitingForMETA {
		meta["offset"] = f.config.Offset
		meta["offset_agent"] = f.Offset
	}
	return meta
}

// GetSchema Get source Schema
func (f *FileReadingFollower) GetSchema() interface{} {
	return "String"
}

// GetStatus Get source status
func (f *FileReadingFollower) GetStatus() interface{} {
	return f.status
}

// GetAvailableActions returns available actions
func (f *FileReadingFollower) GetAvailableActions() map[string]*control.ActionDescription {
	availableAction := make(map[string]*control.ActionDescription)
	return availableAction
}

// Process action
func (f *FileReadingFollower) Process(action string, params ...interface{}) interface{} {
	switch action {
	case control.SourceMeta:
		meta := &control.Meta{}
		payload := params[0].([]byte)
		err := json.Unmarshal(payload, meta)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("Unable to unmarshal meta event")
		} else {
			if val, ok := meta.Data["offset"].(float64); ok {
				f.config.Offset = int64(val)
			}

			if val, ok := meta.Data["offset_agent"].(float64); ok {
				f.Offset = int64(val)
			}

			f.status = control.SourceStatusRunning
		}

	default:
		log.WithFields(log.Fields{
			"action": action,
		}).Error("action not implemented")
	}
	return nil
}

func (f *FileReadingFollower) read() {
	t, err := follower.New(f.config.Path, follower.Config{
		Whence: io.SeekCurrent,
		Offset: f.config.Offset,
		Reopen: true,
	})
	if err != nil {
		log.WithError(err).Error("Error while start reader")
	}

	for line := range t.Lines() {

		f.OutputChannel <- &events.LookatchEvent{
			Header: &events.LookatchHeader{
				EventType: FileReadingFollowerType,
			},
			Payload: &events.GenericEvent{
				Tenant:      f.AgentInfo.tenant.ID,
				AgentID:     f.AgentInfo.uuid,
				Timestamp:   strconv.Itoa(int(time.Now().Unix())),
				Environment: f.AgentInfo.tenant.Env,
				Value:       line.String(),
			},
		}
		f.config.Offset++
		f.Offset++
	}

	if t.Err() != nil {
		log.WithError(t.Err()).Error("Error while reading File")
	}
}
