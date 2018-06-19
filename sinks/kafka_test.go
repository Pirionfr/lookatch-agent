package sinks

import (
	"github.com/Pirionfr/lookatch-common/events"
	"github.com/spf13/viper"
	"testing"

	"github.com/Shopify/sarama"
	"strconv"
	"time"
)

var (
	vKafka    *viper.Viper
	eventChan chan *events.LookatchEvent
	stop      chan error
	threshold int
)

func init() {
	threshold = 10 << 10
	eventChan = make(chan *events.LookatchEvent, 1)
	stop = make(chan error)

	vKafka = viper.New()
	vKafka.Set("sinks", map[string]interface{}{
		"kafka": map[string]interface{}{
			"brokers": []string{
				"test:9093",
			},
			"enabled": true,

			"tls":          true,
			"topic_prefix": "lookatch.test_batch",
			"type":         "kafka",
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

	sink = &Sink{eventChan, stop, "kafka", vKafka.Sub("sinks.kafka")}

	ksink, err := newKafka(sink)
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

	if conf.Topic_prefix != "lookatch.test_batch" {
		t.Fail()
	}

	if conf.Producer.User != "lookatch.test" {
		t.Fail()
	}

}

func TestBuildKafkaSinkConfigTopicSet(t *testing.T) {

	vKafka.Set("sinks.kafka.topic", "test")
	sink = &Sink{eventChan, stop, "kafka", vKafka.Sub("sinks.kafka")}

	ksink, err := newKafka(sink)
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
	sink = &Sink{eventChan, stop, "kafka", vKafka.Sub("sinks.kafka")}

	ksink, err := newKafka(sink)
	if err != nil {
		t.Error(err)
	}
	conf := ksink.(*Kafka).kafkaConf

	if conf.Tls != false {
		t.Fail()
	}
}

func TestBuildKafkaSinkClientID(t *testing.T) {

	vKafka.Set("sinks.kafka.client_id", "test")
	sink = &Sink{eventChan, stop, "kafka", vKafka.Sub("sinks.kafka")}

	ksink, err := newKafka(sink)
	if err != nil {
		t.Error(err)
	}
	conf := ksink.(*Kafka).kafkaConf

	if conf.Client_id != "test" {
		t.Fail()
	}
}

func TestBuildKafkaSinkSecret(t *testing.T) {

	vKafka.Set("sinks.kafka.secret", "test")
	sink = &Sink{eventChan, stop, "kafka", vKafka.Sub("sinks.kafka")}

	ksink, err := newKafka(sink)
	if err != nil {
		t.Error(err)
	}
	conf := ksink.(*Kafka).kafkaConf

	if conf.Secret != "test" {
		t.Fail()
	}
}

func TestProcessGenericEvent(t *testing.T) {
	sink = &Sink{eventChan, stop, "kafka", vKafka.Sub("sinks.kafka")}

	ksink, err := newKafka(sink)
	if err != nil {
		t.Error(err)
	}
	timestamp := strconv.Itoa(int(time.Now().Unix()))

	conf := ksink.(*Kafka).kafkaConf

	genericMsg := &events.GenericEvent{
		Environment: "Envtest",
		AgentId:     "IdTest",
		Tenant:      "faketenant",
		Timestamp:   timestamp,
		Value:       "test",
	}

	msg, err := processGenericEvent(genericMsg, conf, threshold)
	if err != nil {
		t.Error(err)
	}

	if msg.Value.Length() == 0 {
		t.Fail()
	}
}

func TestProcessSqlEvent(t *testing.T) {
	sink = &Sink{eventChan, stop, "kafka", vKafka.Sub("sinks.kafka")}

	ksink, err := newKafka(sink)
	if err != nil {
		t.Error(err)
	}
	timestamp := strconv.Itoa(int(time.Now().Unix()))

	conf := ksink.(*Kafka).kafkaConf

	msgSql := &events.SqlEvent{
		Environment: "Envtest",
		Tenant:      "faketenant",
		Table:       "testTable",
		Database:    "testDatabase",
		Timestamp:   timestamp,
		Method:      "insert",
		PrimaryKey:  "ID",
		Statement:   "test",
	}

	msg, err := processSqlEvent(msgSql, conf, threshold)
	if err != nil {
		t.Error(err)
	}

	if msg.Value.Length() == 0 {
		t.Fail()
	}
}

func TestProcessKafkaMsg(t *testing.T) {
	sink = &Sink{eventChan, stop, "kafka", vKafka.Sub("sinks.kafka")}

	ksink, err := newKafka(sink)
	if err != nil {
		t.Error(err)
	}

	conf := ksink.(*Kafka).kafkaConf

	msgKafka := &sarama.ConsumerMessage{
		Value:          []byte("test"),
		Timestamp:      time.Now(),
		Topic:          "test",
		Offset:         1,
		Key:            []byte("test"),
		BlockTimestamp: time.Now(),
		Partition:      1,
	}

	msg, err := processKafkaMsg(msgKafka, conf, threshold)
	if err != nil {
		t.Error(err)
	}

	if msg.Value.Length() == 0 {
		t.Fail()
	}
}
