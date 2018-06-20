package core

import (
	"encoding/json"
	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/rpc"
	rpcmock "github.com/Pirionfr/lookatch-common/rpc/mock_rpc"
	"github.com/golang/mock/gomock"
	"google.golang.org/grpc/metadata"
	"testing"
)

func PrepareGrpcMockSend(crtlClient *Controller, ctrl *gomock.Controller, msg *rpc.Message) {

	// Create mock for the stream returned by RouteChat
	stream := rpcmock.NewMockController_ChannelClient(ctrl)
	// set expectation on sending.
	stream.EXPECT().Send(
		msg,
	).Return(nil)
	// Create mock for the client interface.
	rpcClient := rpcmock.NewMockControllerClient(ctrl)
	// Set expectation on RouteChat

	crtlClient.client = rpcClient
	crtlClient.stream = stream
}

func TestGetConfig(t *testing.T) {
	a := NewTestAgent()
	if a == nil {
		t.Fail()
	}
	a.controller = NewControllerClient(vCtrl.Sub("controller"), auth)
	if a.controller == nil {
		t.Fail()
	}
	a.controller.md = metadata.New(map[string]string{
		"authorization": "test",
	})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	agentCtrl := &control.Agent{}
	aMsg := agentCtrl.NewMessage(a.tenant.Id, a.uuid.String(), control.AgentStatus).WithPayload(control.AgentStatusWaitingForConf)
	payload, err := json.Marshal(&aMsg)
	if err != nil {
		t.Fail()
	}
	msg := &rpc.Message{
		Type:    control.TypeAgent,
		Payload: payload,
	}

	PrepareGrpcMockSend(a.controller, ctrl, msg)

	a.GetConfig()

}

func TestAgentAvailableAction(t *testing.T) {
	a := NewTestAgent()
	if a == nil {
		t.Fail()
	}
	a.controller = NewControllerClient(vCtrl.Sub("controller"), auth)
	if a.controller == nil {
		t.Fail()
	}
	a.controller.md = metadata.New(map[string]string{
		"authorization": "test",
	})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	agentCtrl := &control.Agent{}
	msgA := a.getAvailableAction()
	payload, err := json.Marshal(&msgA)
	if err != nil {
		t.Fail()
	}
	msg := &rpc.Message{
		Type:    control.TypeAgent,
		Payload: payload,
	}

	PrepareGrpcMockSend(a.controller, ctrl, msg)

	a.SendAgentAvailableAction(agentCtrl)

}

func TestSourceAvailableAction(t *testing.T) {
	a := NewTestAgent()
	if a == nil {
		t.Fail()
	}
	a.controller = NewControllerClient(vCtrl.Sub("controller"), auth)
	if a.controller == nil {
		t.Fail()
	}
	a.controller.md = metadata.New(map[string]string{
		"authorization": "test",
	})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	agentCtrl := &control.Agent{}
	msgA := a.getSourceAvailableAction()
	payload, err := json.Marshal(&msgA)
	if err != nil {
		t.Fail()
	}
	msg := &rpc.Message{
		Type:    control.TypeAgent,
		Payload: payload,
	}

	PrepareGrpcMockSend(a.controller, ctrl, msg)

	a.SendSourceAvailableAction(agentCtrl)

}

func TestSendMeta(t *testing.T) {
	a := NewTestAgent()
	if a == nil {
		t.Fail()
	}
	a.controller = NewControllerClient(vCtrl.Sub("controller"), auth)
	if a.controller == nil {
		t.Fail()
	}
	a.controller.md = metadata.New(map[string]string{
		"authorization": "test",
	})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	agentCtrl := &control.Agent{}
	msgA := a.getSourceMeta()
	payload, err := json.Marshal(&msgA)
	if err != nil {
		t.Fail()
	}
	msg := &rpc.Message{
		Type:    control.TypeAgent,
		Payload: payload,
	}

	PrepareGrpcMockSend(a.controller, ctrl, msg)

	a.SendMeta(agentCtrl)

}

func TestSendSchema(t *testing.T) {
	a := NewTestAgent()
	if a == nil {
		t.Fail()
	}
	a.controller = NewControllerClient(vCtrl.Sub("controller"), auth)
	if a.controller == nil {
		t.Fail()
	}
	a.controller.md = metadata.New(map[string]string{
		"authorization": "test",
	})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	agentCtrl := &control.Agent{}
	msgA := a.GetSchemas()
	payload, err := json.Marshal(&msgA)
	if err != nil {
		t.Fail()
	}
	msg := &rpc.Message{
		Type:    control.TypeAgent,
		Payload: payload,
	}

	PrepareGrpcMockSend(a.controller, ctrl, msg)

	a.SendSchema(agentCtrl)

}

func TestSendAgentStatus(t *testing.T) {
	a := NewTestAgent()
	if a == nil {
		t.Fail()
	}
	a.controller = NewControllerClient(vCtrl.Sub("controller"), auth)
	if a.controller == nil {
		t.Fail()
	}
	a.controller.md = metadata.New(map[string]string{
		"authorization": "test",
	})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	agentCtrl := &control.Agent{}
	msgA := agentCtrl.NewMessage(a.tenant.Id, a.uuid.String(), control.AgentStatus).WithPayload(control.AgentStatusStarting)
	payload, err := json.Marshal(&msgA)
	if err != nil {
		t.Fail()
	}
	msg := &rpc.Message{
		Type:    control.TypeAgent,
		Payload: payload,
	}

	PrepareGrpcMockSend(a.controller, ctrl, msg)

	a.SendAgentStatus(agentCtrl)

}

func TestSendSourceStatus(t *testing.T) {
	a := NewTestAgent()
	if a == nil {
		t.Fail()
	}
	a.controller = NewControllerClient(vCtrl.Sub("controller"), auth)
	if a.controller == nil {
		t.Fail()
	}
	a.controller.md = metadata.New(map[string]string{
		"authorization": "test",
	})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	agentCtrl := &control.Agent{}
	msgA := a.getSourceStatus()
	payload, err := json.Marshal(&msgA)
	if err != nil {
		t.Fail()
	}
	msg := &rpc.Message{
		Type:    control.TypeAgent,
		Payload: payload,
	}

	PrepareGrpcMockSend(a.controller, ctrl, msg)

	a.SendSourceStatus(agentCtrl)

}
