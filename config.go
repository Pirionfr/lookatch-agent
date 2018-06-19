package main

import (
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/spf13/viper"
	"os"
)

var (
	cfgPath, cfgFile string
	v                *viper.Viper
)

type config struct {
	Agent      map[string]interface{} `yaml:"agent" json:"agent"`
	Auth       map[string]interface{} `yaml:"auth" json:"auth"`
	Controller map[string]interface{} `yaml:"controller" json:"controller"`
	Sinks      map[string]interface{} `yaml:"sinks" json:"sinks"`
	Sources    map[string]interface{} `yaml:"sources" json:"sources"`
}

// initializeConfig initializes a config file with sensible default configuration flags.
func initializeConfig() (*viper.Viper, error) {

	v = viper.New()

	v.SetEnvPrefix("OVH_DC")
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
		if _, ok := err.(viper.ConfigParseError); ok {
			return v, fmt.Errorf("Unable to parse Config file : %v", err)
		} else {
			return v, fmt.Errorf("Unable to locate Config file : %v", err)
		}

	}
	m := v.GetStringMap("agent")
	//get config from Environnement
	if tenant := os.Getenv("TENANT"); tenant != "" {
		m["tenant"] = tenant
	}

	if EnvUuid := os.Getenv("UUID"); EnvUuid != "" {
		m["uuid"] = EnvUuid
	}
	if env := os.Getenv("ENV"); env != "" {
		m["env"] = env
	}

	if key := os.Getenv("SECRETKEY"); key != "" {
		m["secretkey"] = key
	}

	hostname, err := os.Hostname()
	if err != nil {
		return v, fmt.Errorf("Unable to get hostname : %v", err)
	}
	m["hostname"] = hostname

	u1, ok := m["uuid"]
	if ok {
		if _, err = uuid.FromString(u1.(string)); err == nil {
			return v, nil
		}
	}

	m["uuid"], _ = uuid.NewV4()
	v.Set("agent", m)
	return v, nil
}
