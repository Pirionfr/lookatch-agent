package control

// lookatch type message
const (
	TypeAgent  = "AGENT_CONTROL"
	TypeSource = "SOURCE_CONTROL"
	TypeSink   = "SINK_CONTROL"
)

// Control message interface
// All control messages must contain :
// - An action to be executed
// - UUID of target agent
// - Token to be authenticated
// - ControlType to be correctly unmarhalled both side
// - A method to create a new message
type Control interface {
	GetAction() string
	GetUUID() string
	GetToken() string
	GetTypeName() string
	NewMessage(...interface{}) *Control
}
