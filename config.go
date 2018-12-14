package main

import (
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/spf13/viper"
)

var (
	cfgPath, cfgFile string
	v                *viper.Viper
)

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
		if _, ok := err.(viper.ConfigParseError); !ok {
			return v, fmt.Errorf("Unable to parse Config file : %v", err)
		}

	}
	m := v.GetStringMap("agent")
	//get config from Environnement
	if tenant := os.Getenv("TENANT"); tenant != "" {
		m["tenant"] = tenant
	}

	if EnvUUID := os.Getenv("UUID"); EnvUUID != "" {
		m["uuid"] = EnvUUID
	}
	if env := os.Getenv("ENV"); env != "" {
		m["env"] = env
	}

	if pwd := os.Getenv("PASSWORD"); pwd != "" {
		m["password"] = pwd
	}

	if key := os.Getenv("SECRETKEY"); key != "" {
		m["secretkey"] = key
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

	u1, ok := m["uuid"]
	if ok {
		if _, err = uuid.Parse(u1.(string)); err == nil {
			return v, nil
		}
	}

	m["uuid"] = uuid.New()
	v.Set("agent", m)
	return v, nil
}
