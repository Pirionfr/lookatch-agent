package control

import (
	"encoding/json"

	"github.com/sirupsen/logrus"
)

// Source Available Actions
const (
	SourceStart           = "StartSource"
	SourceStop            = "StopSource"
	SourceRestart         = "RestartSource"
	SourceStatus          = "StatusSource"
	SourceMeta            = "MetaSource"
	SourceSchema          = "SchemaSource"
	SourceQuery           = "QuerySource"
	SourceAvailableAction = "AvailableActionSource"
	SourceOffset          = "OffsetSource"
)

// Source Possible Status
const (
	SourceStatusOnError        = "ON_ERROR"
	SourceStatusRunning        = "RUNNING"
	SourceStatusWaiting        = "WAITING"
	SourceStatusWaitingForMETA = "WAITING_FOR_META"
)

type (

	// Source Message This message is used for agent control
	Source struct {
		Action  string `json:"action"`
		Name    string `json:"name"`
		Token   string `json:"token"`
		Payload []byte `json:"payload"`
	}

	// Status status format
	Status struct {
		Code interface{} `json:"Code"`
	}

	// Meta meta format
	Meta struct {
		Timestamp string                 `json:"timestamp"`
		Data      map[string]interface{} `json:"data"`
	}

	// Schema type document
	Schema struct {
		Timestamp string      `json:"Timestamp"`
		Raw       interface{} `json:"Schema"`
	}
)

// GetAction action getter
func (a Source) GetAction() string {
	return a.Action
}

// GetName name getter
func (a Source) GetName() string {
	return a.Name
}

// GetToken token getter
func (a Source) GetToken() string {
	return a.Token
}

// GetTypeName type name getter
func (a Source) GetTypeName() string {
	return TypeSource
}

// NewMessage new source message
func (a Source) NewMessage(tenantToken string, uuid string, action string) *Source {
	return &Source{
		Token:  tenantToken,
		Name:   uuid,
		Action: action,
	}
}

// SetToken token setter
func (a *Source) SetToken(token string) *Source {
	a.Token = token
	return a
}

// SetName name setter
func (a *Source) SetName(name string) *Source {
	a.Name = name
	return a
}

// SetAction action setter
func (a *Source) SetAction(action string) *Source {
	a.Action = action
	return a
}

// WithPayload add payload to source message
func (a *Source) WithPayload(i interface{}) *Source {
	b, err := json.Marshal(i)
	if err != nil {
		logrus.Fatal(err)
	}
	a.Payload = b
	return a
}
