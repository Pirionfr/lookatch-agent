package sources

import (
	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
)

// DummyType type of source
const DummyType = "dummy"

// Dummy representation of Dummy
type Dummy struct {
	*Source
}

// newDummy create new dummy source
func newDummy(s *Source) (SourceI, error) {
	return &Dummy{s}, nil
}

// GetMeta returns source meta
func (d *Dummy) GetMeta() map[string]interface{} {
	return nil
}

// GetSchema returns schema
func (d *Dummy) GetSchema() interface{} {
	return nil
}

// Init dummy source
func (d *Dummy) Init() {

}

// Stop dummy source
func (d *Dummy) Stop() error {
	return nil
}

// Start dummy source
func (d *Dummy) Start(i ...interface{}) error {
	return nil
}

// GetName get source name
func (d *Dummy) GetName() string {
	return d.Name
}

// GetOutputChan get output channel
func (d *Dummy) GetOutputChan() chan *events.LookatchEvent {
	return d.OutputChannel
}

// GetStatus get source status
func (d *Dummy) GetStatus() interface{} {
	return control.SourceStatusRunning
}

// IsEnable check if source is enable
func (d *Dummy) IsEnable() bool {
	return true
}

// HealthCheck return true if ok
func (d *Dummy) HealthCheck() bool {
	return true
}

// GetAvailableActions returns available actions
func (d *Dummy) GetAvailableActions() map[string]*control.ActionDescription {
	return nil
}

// Process action
func (d *Dummy) Process(action string, params ...interface{}) interface{} {
	return nil
}
