package sources

import (
	"encoding/json"

	"context"
	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	log "github.com/sirupsen/logrus"
	"stash.ovh.net/sdk/go/gwsdk"
	"strconv"
	"time"
)

// FileReadingFollower representation of FileReadingFollower
type InternalApiPoller struct {
	*Source
	config InternalApiPollerConfig
	status string
}

// pagination representation pagination information
type Pagination struct {
	Parameter string   `json:"parameter"`
	Result   string `json:"result"`
}

// FileReadingFollowerConfig representation of FileReadingFollower Config
type InternalApiPollerConfig struct {
	ServiceAlias        string            `json:"service_alias" mapstructure:"service_alias"`
	EndPoint            string            `json:"end_point" mapstructure:"end_point"`
	Method              string            `json:"method"`
	Parameters          map[string]string `json:"parameters"`
	Pagination Pagination            `json:"pagination"`
	Interval            string            `json:"interval"`
	Offset              string            `json:"offset"`
}

// FileReadingFollowerType type of source
const InternalApiPollerType = "internalApiPoller"

// create new FileReadingFollower source
func newInternalApiPoller(s *Source) (SourceI, error) {

	internalApiPollerConfig := InternalApiPollerConfig{}
	s.Conf.UnmarshalKey("sources."+s.Name, &internalApiPollerConfig)

	return &InternalApiPoller{
		Source: s,
		config: internalApiPollerConfig,
		status: control.SourceStatusWaitingForMETA,
	}, nil
}

// Init source
func (a *InternalApiPoller) Init() {

}

// Stop source
func (a *InternalApiPoller) Stop() error {
	return nil
}

func (a *InternalApiPoller) Start(i ...interface{}) error {

	client := gwsdk.NewGWClient(a.config.ServiceAlias, nil, nil)
	var results []map[string]interface{}
	ctx := context.Background()



	go func() {
		wait, _ := time.ParseDuration(a.config.Interval)
		for {

			if a.config.Pagination != (Pagination{}) {
				a.config.Parameters[a.config.Pagination.Parameter] = a.config.Offset
			}

			err := client.Get(ctx,
				a.config.EndPoint,
				a.config.Parameters,
				&results,
			)

			if err != nil {
				log.WithError(err).Error("Can't get")
				time.Sleep(wait)
				continue
			}
			for i := range results {
				result := results[i]
				a.OutputChannel <- &events.LookatchEvent{
					Header: &events.LookatchHeader{
						EventType: InternalApiPollerType,
					},
					Payload: &events.GenericEvent{
						Tenant:      a.AgentInfo.tenant.Id,
						AgentId:     a.AgentInfo.uuid,
						Timestamp:   strconv.Itoa(int(time.Now().Unix())),
						Environment: a.AgentInfo.tenant.Env,
						Value:       result,
					},
				}


			}
			if len(results)> 0 {
				if a.config.Pagination != (Pagination{}) {
					a.config.Offset = results[len(results)-1][a.config.Pagination.Result].(string)
				}
			} else {
				time.Sleep(wait)
			}

		}
	}()

	return nil
}

// GetName get source name
func (a *InternalApiPoller) GetName() string {
	return a.Name
}

// GetOutputChan get output channel
func (a *InternalApiPoller) GetOutputChan() chan *events.LookatchEvent {
	return a.OutputChannel
}

// IsEnable check if source is enable
func (a *InternalApiPoller) IsEnable() bool {
	return true
}

// HealthCheck return true if ok
func (a *InternalApiPoller) HealthCheck() bool {
	return a.status == control.SourceStatusRunning
}

// GetMeta get source meta
func (a *InternalApiPoller) GetMeta() map[string]interface{} {
	meta := make(map[string]interface{})
	if a.status != control.SourceStatusWaitingForMETA {
		meta["offset"] = a.config.Offset
	}
	return meta
}

// GetSchema Get source Schema
func (a *InternalApiPoller) GetSchema() interface{} {
	return "String"
}

// GetStatus Get source status
func (a *InternalApiPoller) GetStatus() interface{} {
	return a.status
}

// GetAvailableActions returns available actions
func (a *InternalApiPoller) GetAvailableActions() map[string]*control.ActionDescription {
	availableAction := make(map[string]*control.ActionDescription)
	return availableAction
}

// Process action
func (a *InternalApiPoller) Process(action string, params ...interface{}) interface{} {
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
			if val, ok := meta.Data["offset"].(string); ok {
				a.config.Offset = val
			}
			a.status = control.SourceStatusRunning
		}

	default:
		log.WithFields(log.Fields{
			"action": action,
		}).Error("action not implemented")
	}
	return nil
}
