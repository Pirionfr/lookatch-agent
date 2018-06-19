# lookatch

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