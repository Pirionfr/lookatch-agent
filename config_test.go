package main

import (
	"os"
	"testing"
)

func TestExtractDatabaseTable1(t *testing.T) {
	os.Setenv("TENANT", "faketenant")
	os.Setenv("UUID", "fe20e86f-eecf-4838-8266-3bdeb8eb0685")
	os.Setenv("ENV", "test")
	cfgFile = "./config-test.json"
	v, err := initializeConfig()
	if err != nil {
		t.Error(err)
	}
	if v.GetString("agent.uuid") != "fe20e86f-eecf-4838-8266-3bdeb8eb0685" {
		t.Fail()
	}
}
