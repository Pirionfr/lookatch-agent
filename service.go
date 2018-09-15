package main

import (
	"github.com/Pirionfr/lookatch-agent/core"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
)

var (
	closing chan error
)

func runAgent(cmd *cobra.Command, s []string) {

	config, err := initializeConfig()

	if err != nil {
		closing <- errors.Annotate(err, "Error when Initialize Config")
		return
	}

	logLevel, err := log.ParseLevel(config.GetString("agent.loglevel"))
	if err == nil {
		log.SetLevel(logLevel)
	} else {
		log.SetLevel(log.DebugLevel)
	}

	go func() {
		err := core.Run(config, closing)
		if err != nil {
			log.WithFields(log.Fields{
				"error": errors.ErrorStack(err),
			}).Fatal("Error running agent")
			closing <- err
		}
		log.Info("Agent started without errors")
	}()

	err = <-closing
	if err != nil {
		log.WithFields(log.Fields{
			"error": errors.ErrorStack(err),
		}).Error("There was an error")
	}
	log.Info("Closing, Bye !")
}

// init() fills a channel that can be used for shutdown
// notifications for commands. This channel will send a message for every
// interrupt received.
func init() {

	closing = make(chan error)

	signalCh := make(chan os.Signal, 4)
	signal.Notify(signalCh, os.Interrupt)
	go func() {
		<-signalCh
		log.Info("Got SIGINT signal, I quit")
		close(closing)
	}()
}
