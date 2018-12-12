package core

import (
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"strconv"
	"time"

	"github.com/Pirionfr/lookatch-agent/rpc"
	"github.com/spf13/viper"
	"google.golang.org/grpc/status"
)

type (
	// ControllerConfig representation of controller config
	ControllerConfig struct {
		Address string `json:"address"`
		Port    int    `json:"port"`
		Secure  bool   `json:"secure"`
	}

	// Controller representation of controller
	Controller struct {
		client rpc.ControllerClient
		stream rpc.Controller_ChannelClient
		conf   *ControllerConfig
		md     metadata.MD
		recv   chan *rpc.Message
		auth   *Auth
		Status string
	}
)

// NewControllerClient create new controller client
func NewControllerClient(conf *viper.Viper, auth *Auth) *Controller {

	var conn *grpc.ClientConn
	var err error

	cConf := &ControllerConfig{}
	conf.Unmarshal(cConf)

	if cConf.Secure {
		log.Debug("GRPC: Establishing TLS connection")
		conn, err = grpc.Dial(cConf.Address+":"+strconv.Itoa(cConf.Port), grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, cConf.Address)))
	} else {
		log.Debug("GRPC: Establishing unsecure connection")
		conn, err = grpc.Dial(cConf.Address+":"+strconv.Itoa(cConf.Port), grpc.WithInsecure())
	}

	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Unable to connect")
	} else {
		log.Info("GRPC established")
	}

	c := rpc.NewControllerClient(conn)

	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Unable to connect")
	}

	ctrl := &Controller{
		client: c,
		conf:   cConf,
		recv:   make(chan *rpc.Message, 1000),
		auth:   auth,
	}
	go func() {
		for range (time.NewTicker(time.Second * 10)).C {
			ctrl.Status = conn.GetState().String()
		}
	}()

	return ctrl
}

// StartChannel start channel
func (c *Controller) StartChannel() {

	var err error
	if c.md == nil {
		token, err := c.auth.GetToken()
		for err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("Error while authenticating wait 5 seconds...")
			time.Sleep(time.Second * 5)
			token, err = c.auth.GetToken()
		}
		c.md = metadata.New(map[string]string{
			"authorization": token,
		})
	}

	ctx := metadata.NewOutgoingContext(context.Background(), c.md)
	c.stream, err = c.client.Channel(ctx)
	for err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Error building rpc wait 5 seconds...")
		time.Sleep(time.Second * 5)
		c.stream, err = c.client.Channel(ctx)
	}
	log.Info("Channel Started")
}

// RecvMessage receive message from  channel
func (c *Controller) RecvMessage(async chan *rpc.Message) {
	for {
		req, err := c.stream.Recv()

		if err != nil {
			//check if token is expired
			statusError := status.Convert(err)
			if statusError.Message() == "Token is expired" {
				token, err := c.auth.GetToken()
				if err != nil {
					log.WithFields(log.Fields{
						"error": err,
					}).Fatal("Error while authenticating")
					return
				}
				c.md = metadata.New(map[string]string{
					"authorization": token,
				})
			}

			log.WithFields(log.Fields{
				"error": err,
			}).Error("Failed to receive a note")

			err2 := c.stream.CloseSend()
			if err2 != nil {
				log.WithFields(log.Fields{
					"error": err2,
				}).Error("Closing a.controller.stream Failed and trying to reconnect")
			} else {
				log.Error("Closing a.controller.stream OK and trying to reconnect")

			}
			log.Error("Restarting rpc")
			c.StartChannel()
		} else {
			async <- req
		}
	}
}
