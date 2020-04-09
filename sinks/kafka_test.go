package sinks

import (
	"testing"

	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/spf13/viper"

	"strconv"
	"time"
)

var (
	vKafka     *viper.Viper
	eventChan  chan events.LookatchEvent
	commitChan chan interface{}
	stop       chan error
	threshold  int
)

func init() {
	threshold = 10 << 10
	eventChan = make(chan events.LookatchEvent, 1)
	commitChan = make(chan interface{}, 1)
	stop = make(chan error)

	vKafka = viper.New()
	vKafka.Set("sinks", map[string]interface{}{
		"kafka": map[string]interface{}{
			"brokers": []string{
				"test:9093",
			},
			"enabled":           true,
			"max_message_bytes": threshold,
			"nb_producer":       1,
			"tls":               true,
			"topic_prefix":      "lookatch.test_batch",
			"type":              "Kafka",
			"producer": map[string]interface{}{
				"user":     "lookatch.test",
				"password": "test",
			},
			"consumer": map[string]interface{}{
				"user":     "lookatch.test",
				"password": "test",
			},
		},
	})

}

func TestBuildKafkaSinkConfig(t *testing.T) {

	sink = &Sink{eventChan, stop, commitChan, "kafka", "", vKafka.Sub("sinks.kafka")}

	ksink, err := NewKafka(sink)
	if err != nil {
		t.Error(err)
	}
	conf := ksink.(*Kafka).kafkaConf

	if conf.Brokers == nil {
		t.Fail()
	}

	if conf.Producer.Password != "test" {
		t.Fail()
	}

	if conf.TopicPrefix != "lookatch.test_batch" {
		t.Fail()
	}

	if conf.Producer.User != "lookatch.test" {
		t.Fail()
	}

}

func TestBuildKafkaSinkConfigTopicSet(t *testing.T) {

	vKafka.Set("sinks.kafka.topic", "test")
	sink = &Sink{eventChan, stop, commitChan, "kafka", "", vKafka.Sub("sinks.kafka")}

	ksink, err := NewKafka(sink)
	if err != nil {
		t.Error(err)
	}
	conf := ksink.(*Kafka).kafkaConf

	if conf.Topic != "test" {
		t.Fail()
	}
}

func TestBuildKafkaSinktls(t *testing.T) {

	vKafka.Set("sinks.kafka.tls", false)
	sink = &Sink{eventChan, stop, commitChan, "kafka", "", vKafka.Sub("sinks.kafka")}

	ksink, err := NewKafka(sink)
	if err != nil {
		t.Error(err)
	}
	conf := ksink.(*Kafka).kafkaConf

	if conf.TLS {
		t.Fail()
	}
}

func TestBuildKafkaSinkClientID(t *testing.T) {

	vKafka.Set("sinks.kafka.client_id", "test")
	sink = &Sink{eventChan, stop, commitChan, "kafka", "", vKafka.Sub("sinks.kafka")}

	ksink, err := NewKafka(sink)
	if err != nil {
		t.Error(err)
	}
	conf := ksink.(*Kafka).kafkaConf

	if conf.ClientID != "test" {
		t.Fail()
	}
}

func TestBuildKafkaSinkSecret(t *testing.T) {

	sink = &Sink{eventChan, stop, commitChan, "kafka", "test", vKafka.Sub("sinks.kafka")}

	ksink, err := NewKafka(sink)
	if err != nil {
		t.Error(err)
	}
	typedSink := ksink.(*Kafka)

	if typedSink.encryptionkey != "test" {
		t.Fail()
	}
}

func TestProcessGenericEvent(t *testing.T) {
	sink = &Sink{eventChan, stop, commitChan, "kafka", "", vKafka.Sub("sinks.kafka")}

	ksink, err := NewKafka(sink)
	if err != nil {
		t.Error(err)
	}
	timestamp := strconv.Itoa(int(time.Now().Unix()))

	genericMsg := &events.GenericEvent{
		Environment: "Envtest",
		Timestamp:   timestamp,
		Value:       "test",
	}

	msg, err := ksink.(*Kafka).processGenericEvent(genericMsg)
	if err != nil {
		t.Error(err)
	}

	if msg.Value == nil {
		t.Fail()
	}
}

func TestProcessSqlEvent(t *testing.T) {
	sink = &Sink{eventChan, stop, commitChan, "kafka", "", vKafka.Sub("sinks.kafka")}

	ksink, err := NewKafka(sink)
	if err != nil {
		t.Error(err)
	}
	timestamp := strconv.Itoa(int(time.Now().Unix()))

	msgSQL := &events.SQLEvent{
		Environment: "Envtest",
		Tenant:      "faketenant",
		Table:       "testTable",
		Database:    "testDatabase",
		Timestamp:   timestamp,
		Method:      "insert",
		PrimaryKey:  "ID",
		Statement: map[string]interface{}{
			"test": "test",
		},
	}

	msg, err := ksink.(*Kafka).processSQLEvent(msgSQL)
	if err != nil {
		t.Error(err)
	}

	if msg.Value == nil {
		t.Fail()
	}
}
