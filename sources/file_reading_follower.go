package sources

import (
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"time"

	"github.com/papertrail/go-tail/follower"
	log "github.com/sirupsen/logrus"

	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/Pirionfr/lookatch-agent/utils"
)

// FileReadingFollower representation of FileReadingFollower
type FileReadingFollower struct {
	*Source
	config FileReadingFollowerConfig
}

// FileReadingFollowerConfig representation of FileReadingFollower Config
type FileReadingFollowerConfig struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
}

// FileReadingFollowerType type of source
const FileReadingFollowerType = "FileReadingFollower"

// NewFileReadingFollower create new FileReadingFollower source
func NewFileReadingFollower(s *Source) (SourceI, error) {
	fileReadingFollowerConfig := FileReadingFollowerConfig{}
	err := s.Conf.UnmarshalKey("sources."+s.Name, &fileReadingFollowerConfig)
	if err != nil {
		return nil, err
	}
	return &FileReadingFollower{
		Source: s,
		config: fileReadingFollowerConfig,
	}, nil
}

// Start source
func (f *FileReadingFollower) Start(i ...interface{}) (err error) {
	err = f.Source.Start(i)
	if err != nil {
		return
	}

	go f.UpdateCommittedLsn()

	go f.read()
	return
}

// GetMeta get source meta
func (f *FileReadingFollower) GetMeta() map[string]utils.Meta {
	meta := f.Source.GetMeta()
	if f.Status != SourceStatusWaitingForMETA {
		meta["offset"] = utils.NewMeta("offset", f.config.Offset)
	}
	return meta
}

// GetSchema Get source Schema
func (f *FileReadingFollower) GetSchema() map[string]map[string]*Column {
	filename := filepath.Base(f.config.Path)
	return map[string]map[string]*Column{
		filename: {
			"line": &Column{
				Column:       "line",
				ColumnOrdPos: 0,
				DataType:     "string",
				ColumnType:   "string",
			},
		},
	}
}

// GetCapabilities returns available actions
func (f *FileReadingFollower) GetCapabilities() map[string]*utils.TaskDescription {
	availableAction := make(map[string]*utils.TaskDescription)
	return availableAction
}

// Process action
func (f *FileReadingFollower) Process(action string, params ...interface{}) interface{} {
	switch action {
	case utils.SourceMeta:
		meta := params[0].(map[string]utils.Meta)

		if val, ok := meta["offset"]; ok {
			f.config.Offset = int64(val.Value.(float64))
		}

		if val, ok := meta["offset_agent"]; ok {
			f.Offset = int64(val.Value.(float64))
		}

		f.Status = SourceStatusRunning

	default:
		return errors.New("task not implemented")
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

	currentOffset := f.config.Offset

	for line := range t.Lines() {
		f.OutputChannel <- events.LookatchEvent{
			Header: events.LookatchHeader{
				EventType: FileReadingFollowerType,
			},
			Payload: events.GenericEvent{
				Timestamp:   strconv.Itoa(int(time.Now().Unix())),
				Environment: f.AgentInfo.Tenant.Env,
				Value:       line.String(),
				Offset: &events.Offset{
					Source: strconv.FormatInt(currentOffset, 10),
					Agent:  strconv.FormatInt(f.Offset, 10),
				},
			},
		}
		currentOffset++
		f.Offset++
	}

	if t.Err() != nil {
		log.WithError(t.Err()).Error("Error while reading File")
	}
}

// UpdateCommittedLsn update CommittedLsn
func (f *FileReadingFollower) UpdateCommittedLsn() {
	for committedLsn := range f.CommitChannel {
		if f.Offset > committedLsn.(int64) {
			f.Offset = committedLsn.(int64)
		}
	}
}
