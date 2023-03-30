package utils

import (
	"reflect"
)

// Available Task constants
const (
	AgentStart   = "StartAgent"
	AgentStop    = "StopAgent"
	AgentRestart = "RestartAgent"

	SinkStart   = "StartSink"
	SinkStop    = "StopSink"
	SinkRestart = "RestartSink"

	SourceStart   = "StartSource"
	SourceStop    = "StopSource"
	SourceRestart = "RestartSource"
	SourceQuery   = "QuerySource"
	SourceMeta    = "SourceMeta"

	TaskPending = "PENDING"
	TaskRunning = "IN_PROGRESS"
	TaskDone    = "SUCCEEDED"
	TaskOnError = "FAILED"
)

// ParametersDescription parameters description
type ParametersDescription struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
}

// TaskDescription task description
type TaskDescription struct {
	Description string                   `json:"description"`
	Parameters  []*ParametersDescription `json:"params" mapstructure:"params"`
}

// Task task description
// a task is an action getted from API
type Task struct {
	ID           string                 `json:"id"`
	TaskType     string                 `json:"taskType" mapstructure:"taskType"`
	Target       string                 `json:"target"`
	CreatedAt    int64                  `json:"created_at"`
	StartDate    int64                  `json:"start_date"`
	EndDate      int64                  `json:"end_date"`
	Status       string                 `json:"status"`
	Description  string                 `json:"description"`
	Parameters   map[string]interface{} `json:"params" mapstructure:"params"`
	ErrorDetails string                 `json:"error_details,omitempty"`
}

// DeclareNewTaskDescription Create a new task type by describing it
func DeclareNewTaskDescription(class interface{}, description string) (task *TaskDescription) {
	task = &TaskDescription{
		Description: description,
	}

	if class == nil {
		return
	}
	t := reflect.TypeOf(class)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		task.Parameters = append(task.Parameters, &ParametersDescription{
			Name:        field.Tag.Get("name"),
			Description: field.Tag.Get("description"),
			Type:        field.Type.Name(),
			Required:    field.Tag.Get("required") == "true",
		})
	}

	return
}
