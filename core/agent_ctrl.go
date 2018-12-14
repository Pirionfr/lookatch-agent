package core

import "github.com/Pirionfr/lookatch-agent/control"

import (
	"encoding/json"

	"github.com/Pirionfr/lookatch-agent/rpc"
	log "github.com/sirupsen/logrus"
)

//dispatchAgentFactory dispatch agent factory
var dispatchAgentFactory = map[string]func(*Agent, *control.Agent) error{
	control.AgentStatus:           (*Agent).SendStatus,
	control.AgentConfigure:        (*Agent).UpdateConfig,
	control.SourceMeta:            (*Agent).SendMeta,
	control.SourceSchema:          (*Agent).SendSchema,
	control.SourceAvailableAction: (*Agent).SendAvailableAction,
}

// HandleMessage Handle Message from rpc
func (a *Agent) HandleMessage(async chan *rpc.Message) {

	for request := range async {
		//log.Debug("got event type : ", request.Type)
		switch request.Type {
		case control.TypeAgent:
			//handle agent message
			agentCtrl := &control.Agent{}
			json.Unmarshal(request.Payload, agentCtrl)
			a.DispatchAgent(agentCtrl)

		case control.TypeSink:
			sinkCtrl := &control.Sink{}
			json.Unmarshal(request.Payload, sinkCtrl)
			log.WithFields(log.Fields{
				"sink": sinkCtrl,
			}).Debug("Got Sink message, dispatching")
			a.DispatchSink(request, sinkCtrl.GetName())

		case control.TypeSource:
			sourceCtrl := &control.Source{}
			json.Unmarshal(request.Payload, sourceCtrl)
			log.WithFields(log.Fields{
				"action": sourceCtrl.Action,
			}).Debug("Got Source message, dispatching")
			a.DispatchSource(request, sourceCtrl.GetName())
		}
	}
}

// DispatchAgent dispatch agent message
func (a *Agent) DispatchAgent(agentMsg *control.Agent) error {
	fn, found := dispatchAgentFactory[agentMsg.Action]
	if !found {
		log.WithFields(log.Fields{
			"action": agentMsg.Action,
		}).Error("Got an Agent message with unhandled action")
		return nil
	}
	return fn(a, agentMsg)
}

// DispatchSource dispatch source message
func (a *Agent) DispatchSource(payload *rpc.Message, sourceName string) {
	s, ok := a.getSource(sourceName)
	if !ok {
		log.WithFields(log.Fields{
			"name":     sourceName,
			"Currents": a.getSources(),
		}).Debug("source name not found")
		return
	}
	srcCtrl := control.Source{}

	//We only get source requests, no need to check type
	err := json.Unmarshal(payload.Payload, &srcCtrl)
	if err != nil {
		log.WithFields(log.Fields{
			"payload": payload,
		}).Error("Unable to unmarshal message")
		return
	}

	switch srcCtrl.Action {
	case control.SourceStart:
		s.Start()

	case control.SourceStop:
		s.Stop()

	case control.SourceRestart:
		s.Stop()
		s.Start()

	case control.SourceAvailableAction:
		a.GetSourceAvailableAction(sourceName, &srcCtrl)

	default:
		log.Debug("Controller asked for action, sending it")
		a.getSources()[sourceName].Process(srcCtrl.Action, srcCtrl.Payload)
	}
}

// DispatchSink dispatch sink message
func (a *Agent) DispatchSink(payload *rpc.Message, sinkName string) {

	//@TODO add a control of sinks

}

// GetSourceAvailableAction Controller asked for Available Actions, sending it
func (a *Agent) GetSourceAvailableAction(sourceName string, srcCtrl *control.Source) {
	log.Debug("Controller asked for Available Actions, sending it")
	aAction := a.getSources()[sourceName].GetAvailableActions()

	msg := control.Source{}.NewMessage(srcCtrl.Token, srcCtrl.Name, control.SourceAvailableAction).WithPayload(aAction)
	a.SendEncapsMessage(msg, control.TypeSource)
}

// GetConfig Get Configuration from server
func (a *Agent) GetConfig() error {
	agentCtrl := &control.Agent{}
	msg := agentCtrl.NewMessage(a.tenant.ID, a.uuid.String(), control.AgentStatus).WithPayload(control.AgentStatusWaitingForConf)
	return a.SendEncapsMessage(msg, control.TypeAgent)
}

// SendAvailableAction send all available action
func (a *Agent) SendAvailableAction(agentCtrl *control.Agent) (err error) {
	log.Debug("Controller asked for Available Action, sending it")

	err = a.SendSourceAvailableAction(agentCtrl)
	if err != nil {
		return
	}
	return a.SendAgentAvailableAction(agentCtrl)
}

// SendAgentAvailableAction send agent action
func (a *Agent) SendAgentAvailableAction(agentCtrl *control.Agent) error {
	msg := a.getAvailableAction()
	return a.SendEncapsMessage(msg, control.TypeAgent)
}

// SendSourceAvailableAction send source action
func (a *Agent) SendSourceAvailableAction(agentCtrl *control.Agent) error {
	msg := a.getSourceAvailableAction()
	return a.SendEncapsMessage(msg, control.TypeAgent)
}

// SendMeta Send all meta schema
func (a *Agent) SendMeta(agentCtrl *control.Agent) error {
	//log.Debug("Controller asked for meta, sending it")
	msg := a.getSourceMeta()
	return a.SendEncapsMessage(msg, control.TypeAgent)

}

//SendSchema send all sources schema
func (a *Agent) SendSchema(agentCtrl *control.Agent) error {
	msg := a.GetSchemas()
	return a.SendEncapsMessage(msg, control.TypeAgent)
}

//SendStatus send agent status and source status
func (a *Agent) SendStatus(agentCtrl *control.Agent) (err error) {

	//First send agent status
	//send hearbeat when status ask
	err = a.SendAgentStatus(agentCtrl)
	if err != nil {
		return
	}
	//send sources status
	return a.SendSourceStatus(agentCtrl)

}

// UpdateConfig Update agent Config
func (a *Agent) UpdateConfig(agentCtrl *control.Agent) (err error) {
	err = a.updateConfig(agentCtrl.Payload)
	if err != nil {
		log.Error("Unable to update config")
	}
	return
}

// SendAgentStatus send agent status
func (a *Agent) SendAgentStatus(agentCtrl *control.Agent) error {
	msg := agentCtrl.NewMessage(a.tenant.ID, a.uuid.String(), control.AgentStatus).WithPayload(a.status)
	return a.SendEncapsMessage(msg, control.TypeAgent)
}

//SendSourceStatus send source status
func (a *Agent) SendSourceStatus(agentCtrl *control.Agent) error {
	//send sources status
	//log.Debug("Controller asked for status, sending it")
	msg := a.getSourceStatus()
	return a.SendEncapsMessage(msg, control.TypeAgent)
}

// SendEncapsMessage encaspule message and send to rpc
func (a *Agent) SendEncapsMessage(msg interface{}, typeMessage string) (err error) {
	payload, err := json.Marshal(&msg)
	if err != nil {
		return
	}
	err = a.controller.stream.Send(&rpc.Message{
		Type:    typeMessage,
		Payload: payload,
	})

	return
}
