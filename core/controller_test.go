package core

import (
	"github.com/Pirionfr/lookatch-common/rpc"
	rpcmock "github.com/Pirionfr/lookatch-common/rpc/mock_rpc"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/viper"
	"google.golang.org/grpc/metadata"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

var (
	vCtrl *viper.Viper
	auth  *Auth
)

func init() {
	vCtrl = viper.New()
	vCtrl.Set("controller", map[string]interface{}{
		"address": "localhost",
		"port":    8080,
		"secure":  false,
	})
	auth = &Auth{
		tenant:    "test",
		uuid:      "test",
		secretkey: "test",
		hostname:  "test",
		authURL:   "test",
	}

}

func TestNewControllerClient(t *testing.T) {
	crtl := NewControllerClient(vCtrl.Sub("controller"), auth)
	if crtl == nil {
		t.Fail()
	}
}

func TestNewControllerClientTls(t *testing.T) {
	vCtrl.Set("controller.secure", true)
	crtl := NewControllerClient(vCtrl.Sub("controller"), auth)
	if crtl == nil {
		t.Fail()
	}
}

func TestStartChannel(t *testing.T) {

	crtlClient := NewControllerClient(vCtrl.Sub("controller"), auth)
	if crtlClient == nil {
		t.Fail()
	}
	crtlClient.md = metadata.New(map[string]string{
		"authorization": "test",
	})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stream := rpcmock.NewMockController_ChannelClient(ctrl)
	// Create mock for the stream
	chanClient := rpcmock.NewMockControllerClient(ctrl)

	chanClient.EXPECT().Channel(gomock.Any()).Return(stream, nil)
	crtlClient.client = chanClient

	crtlClient.StartChannel()
}

func TestStartChannelWithAuth(t *testing.T) {

	handler := func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		io.WriteString(w, `{"token":"test"}`)
	}

	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	auth = newAuth("tenant", "uuid", "secret", "host", server.URL)

	crtlClient := NewControllerClient(vCtrl.Sub("controller"), auth)
	if crtlClient == nil {
		t.Fail()
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stream := rpcmock.NewMockController_ChannelClient(ctrl)
	// Create mock for the stream
	chanClient := rpcmock.NewMockControllerClient(ctrl)

	chanClient.EXPECT().Channel(gomock.Any()).Return(stream, nil)
	crtlClient.client = chanClient

	crtlClient.StartChannel()
}

func TestRecvMessage(t *testing.T) {
	crtlClient := NewControllerClient(vCtrl.Sub("controller"), auth)
	if crtlClient == nil {
		t.Fail()
	}
	crtlClient.md = metadata.New(map[string]string{
		"authorization": "test",
	})

	msg := &rpc.Message{
		Type:    "test",
		Payload: []byte("test"),
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	PrepareGrpcMock(crtlClient, ctrl, msg)

	async := make(chan *rpc.Message)
	go crtlClient.RecvMessage(async)

	//send Test msg
	crtlClient.stream.Send(msg)

	result := <-async

	if !proto.Equal(result, msg) {
		t.Fail()
	}
}

func PrepareGrpcMock(crtlClient *Controller, ctrl *gomock.Controller, msg *rpc.Message) {

	// Create mock for the stream returned by Channel
	stream := rpcmock.NewMockController_ChannelClient(ctrl)
	// set expectation on sending.
	stream.EXPECT().Send(
		msg,
	).Return(nil)
	// Set expectation on receiving.
	stream.EXPECT().Recv().Return(msg, nil)
	stream.EXPECT().Recv().Return(&rpc.Message{}, nil)
	// Create mock for the client interface.
	rpcClient := rpcmock.NewMockControllerClient(ctrl)
	// Set expectation on RouteChat
	rpcClient.EXPECT().Channel(
		gomock.Any(),
	).Return(stream, nil)

	crtlClient.client = rpcClient
	crtlClient.StartChannel()
}
