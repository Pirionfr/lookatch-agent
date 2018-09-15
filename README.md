# lookatch

[![version](https://img.shields.io/badge/status-alpha-orange.svg)](https://github.com/Pirionfr/**lookatch-agent**)
[![Build Status](https://travis-ci.org/Pirionfr/lookatch-agent.svg?branch=master)](https://travis-ci.org/Pirionfr/lookatch-agent)
[![Go Report Card](https://goreportcard.com/badge/github.com/Pirionfr/lookatch-agent)](https://goreportcard.com/report/github.com/Pirionfr/lookatch-agent)
[![codecov](https://codecov.io/gh/Pirionfr/lookatch-agent/branch/master/graph/badge.svg)](https://codecov.io/gh/Pirionfr/lookatch-agent)



lookatch allows you to replicate and synchronize your database

That way, you can process data no matter the backend it comes from and feed any application with changes that remotely happened on databases.


### Configuration example
```
{
  "agent": {
    "env": "<environement string>",
    "loglevel": 5,
  },
  "sinks": {
    "default": {
      "enabled": true,
      "type" : "stdout"
    }
  },
  "sources": {
    "default": {
      "autostart": true,
      "enabled": true,
      "dummy" : "test",
      "type" : "dummy",
      "sinks": ["default"]
    }
  }
}
``` 

## Run

Export your credentials as environment variables.

```
export TENANT=xxxxx
export UUID=xxxxx
export SECRETKEY=xxxxx
```

```
lookatch-agent run -c config.json
```