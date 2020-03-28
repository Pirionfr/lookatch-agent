/*
Package lookatch is a pure Go client for dealing with Query and change data capture (CDC)

Coupled to an ingest business layer (not part of this project),
it makes it possible for you to process your data in the same way,
no matter the database backend they come from.
You can then feed any application you may need so that they can react almost
in real time to the changes in your configured source data.

*/
package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"

	"github.com/google/uuid"
	"github.com/spf13/viper"

	"github.com/Pirionfr/lookatch-agent/core"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	closing          chan error
	cfgPath, cfgFile string
	v                *viper.Viper
)

var (
	version = "0.0.0"
	githash = "HEAD"
	date    = "1970-01-01T00:00:00Z UTC"

	app = &cobra.Command{
		Use:   "Lookatch",
		Short: "Lookatch short...",
		Long:  "Long version...",
	}

	agentCmd = &cobra.Command{
		Use:   "run",
		Short: "run collector",
		Long:  `run an instance of the collector`,
		Run: func(cmd *cobra.Command, args []string) {
			runAgent()
		},
	}

	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Show version",
		Run: func(cmd *cobra.Command, arguments []string) {
			fmt.Printf("lookatch-agent version %s %s\n", version, githash)
			fmt.Printf("lookatch-agent build date %s\n", date)
			fmt.Printf("go version %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
		},
	}
)

// init collector
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

	app.AddCommand(agentCmd, versionCmd)
	agentCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $PWD/config.json)")
	agentCmd.PersistentFlags().StringVarP(&cfgPath, "configPath", "p", "", "config file path (default is $PWD)")
}

// main execute the collector
func main() {
	err := app.Execute()
	if err != nil {
		log.WithError(err).Error("Error while Execute lookatch")
	}
}

// initializeConfig initializes a config file with sensible default configuration flags.
func initializeConfig() (*viper.Viper, error) {
	v = viper.New()

	v.SetEnvPrefix("DCC")
	v.AutomaticEnv()

	if cfgFile != "" {
		v.SetConfigFile(cfgFile)
	} else {
		if cfgPath == "" {
			v.AddConfigPath(".")
		} else {
			v.AddConfigPath(cfgPath)
		}
	}

	err := v.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigParseError); !ok {
			return v, fmt.Errorf("unable to parse Config file : %v", err)
		}
	}
	m := v.GetStringMap("agent")

	if EnvUUID := os.Getenv("UUID"); EnvUUID != "" {
		m["uuid"] = EnvUUID
	}
	if env := os.Getenv("ENV"); env != "" {
		m["env"] = env
	}

	if pwd := os.Getenv("PASSWORD"); pwd != "" {
		m["password"] = pwd
	}
	if port := v.GetInt("agent.healthport"); port != 0 {
		m["healthport"] = port
	} else {
		m["healthport"] = 8080
	}

	hostname, err := os.Hostname()
	if err != nil {
		return v, fmt.Errorf("unable to get hostname : %v", err)
	}
	m["hostname"] = hostname
	m["version"] = version + "." + githash
	m["date"] = date

	u1, ok := m["uuid"]
	if ok {
		if _, err = uuid.Parse(u1.(string)); err != nil {
			return v, fmt.Errorf("unable to Parse uuid : %v", err)
		}
	} else {
		m["uuid"] = uuid.New()
	}

	v.Set("agent", m)
	return v, nil
}

// runAgent run an instance of the collector
func runAgent() {
	config, err := initializeConfig()
	if err != nil {
		closing <- errors.Annotate(err, "Error when Initialize Config")
		return
	}

	log.SetOutput(os.Stdout)
	logLevel, err := log.ParseLevel(config.GetString("agent.loglevel"))
	if err != nil {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)
	log.WithField("level", log.DebugLevel).Info("log level")

	go func() {
		err = core.Run(config, closing)
		if err != nil {
			closing <- err
			return
		}
		log.Info("Agent started")
	}()

	err = <-closing
	if err != nil {
		log.WithError(err).Error("Error running agent")
	}
	log.Info("Closing, Bye !")
}
