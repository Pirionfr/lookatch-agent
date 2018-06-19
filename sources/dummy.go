package sources

import (
	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
)

type Dummy struct {
	*Source
}

const DummyType = "dummy"

func newDummy(s *Source) (SourceI, error) {
	return &Dummy{s}, nil
}

func (d *Dummy) GetMeta() map[string]interface{} {
	return nil
}

func (d *Dummy) GetSchema() interface{} {
	return nil
}

func (d *Dummy) Init() {

}

func (d *Dummy) Stop() error {
	return nil
}

func (d *Dummy) Start(i ...interface{}) error {
	return nil
}

func (d *Dummy) GetName() string {
	return d.Name
}

func (d *Dummy) GetOutputChan() chan *events.LookatchEvent {
	return d.OutputChannel
}

func (d *Dummy) GetStatus() interface{} {
	return control.SourceStatusRunning
}

func (d *Dummy) IsEnable() bool {
	return true
}

func (d *Dummy) HealtCheck() bool {
	return true
}

func (m *Dummy) GetAvailableActions() map[string]*control.ActionDescription {
	return nil
}

func (d *Dummy) Process(action string, params ...interface{}) interface{} {
	return nil
}
