package sinks

import (
	"encoding/json"
	"time"

	"encoding/binary"

	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/Pirionfr/lookatch-agent/util"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

// KafkaType type of sink
const KafkaType = "kafka"

type (
	// kafkaUser representation of kafka User
	kafkaUser struct {
		User     string `json:"user"`
		Password string `json:"password"`
	}

	// kafkaSinkConfig representation of kafka sink config
	kafkaSinkConfig struct {
		TLS             bool       `json:"tls"`
		Topic           string     `json:"topic"`
		TopicPrefix     string     `json:"topic_prefix" mapstructure:"topic_prefix"`
		ClientID        string     `json:"client_id" mapstructure:"client_id"`
		Brokers         []string   `json:"brokers"`
		Producer        *kafkaUser `json:"producer"`
		Consumer        *kafkaUser `json:"consumer"`
		MaxMessageBytes int        `json:"max_message_bytes" mapstructure:"max_message_bytes"`
		NbProducer      int        `json:"nb_producer" mapstructure:"nb_producer"`
	}

	// Kafka representation of kafka sink
	Kafka struct {
		*Sink
		kafkaConf *kafkaSinkConfig
	}
)

// newKafka create new kafka sink
func newKafka(s *Sink) (SinkI, error) {

	ksConf := &kafkaSinkConfig{}
	s.conf.Unmarshal(ksConf)

	return &Kafka{
		Sink:      s,
		kafkaConf: ksConf,
	}, nil
}

// Start kafka sink
func (k *Kafka) Start(_ ...interface{}) error {

	resendChan := make(chan *sarama.ProducerMessage, 10000)
	// Notice order could get altered having more than 1 producer
	log.WithFields(log.Fields{
		"Name":       k.name,
		"NbProducer": k.kafkaConf.NbProducer,
	}).Debug("Starting sink producers")
	for x := 0; x < k.kafkaConf.NbProducer; x++ {
		go k.startProducer(resendChan, k.stop)
	}

	//current kafka threshold is 10MB

	log.WithFields(log.Fields{
		"threshold": k.kafkaConf.MaxMessageBytes,
	}).Debug("KafkaSink: started with threshold")

	go k.startConsumer(resendChan)

	//Send empty event every Minutes as to flush buffer
	ticker := time.NewTicker(time.Minute * 1)
	go func() {
		for range ticker.C {
			resendChan <- &sarama.ProducerMessage{Topic: "", Key: sarama.StringEncoder(""), Value: sarama.StringEncoder("")}
		}
	}()

	return nil
}

//GetInputChan return input channel attach to sink
func (k *Kafka) GetInputChan() chan *events.LookatchEvent {
	return k.in
}

// startConsumer consume input chan
func (k *Kafka) startConsumer(kafkaChan chan *sarama.ProducerMessage) {
	for {
		for eventMsg := range k.in {

			//id event is too heavy it wont fit in kafka threshold so we have to skip it
			switch typedMsg := eventMsg.Payload.(type) {
			case *events.SQLEvent:
				producerMsg, err := k.processSQLEvent(typedMsg)
				if err != nil {
					break
				}
				kafkaChan <- producerMsg
			case *events.GenericEvent:
				producerMsg, err := k.processGenericEvent(typedMsg)
				if err != nil {
					break
				}
				kafkaChan <- producerMsg
			case *sarama.ConsumerMessage:
				producerMsg, err := k.processKafkaMsg(typedMsg)
				if err != nil {
					break
				}
				kafkaChan <- producerMsg
			default:
				log.WithFields(log.Fields{
					"message": eventMsg,
				}).Debug("KafkaSink: event doesn't match any known type: ", eventMsg)
			}
		}
	}
}

// processGenericEvent process Generic Event
func (k *Kafka) processGenericEvent(genericMsg *events.GenericEvent) (*sarama.ProducerMessage, error) {
	var topic string
	if len(k.kafkaConf.Topic) == 0 {
		topic = k.kafkaConf.TopicPrefix + genericMsg.Environment
	} else {
		topic = k.kafkaConf.Topic
	}
	var msgToSend []byte
	serializedEventPayload, err := json.Marshal(genericMsg)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("KafkaSink Marshal Error")
	}
	if len(k.encryptionkey) > 0 {
		var err error
		msgToSend, err = util.EncryptBytes(serializedEventPayload, k.encryptionkey)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("KafkaSink Encrypt Error")
		}
	} else {
		msgToSend = serializedEventPayload
	}
	//if message is heavier than threshold we must skip it
	if len(msgToSend) > k.kafkaConf.MaxMessageBytes {
		errMsg := "KafkaSink: Skip too heavy event : "
		log.WithFields(log.Fields{
			"size":      len(msgToSend),
			"threshold": k.kafkaConf.MaxMessageBytes,
			"topic":     topic,
		}).Debug("KafkaSink: Skip too heavy event")
		return nil, errors.New(errMsg)
	}

	return &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(genericMsg.Environment), Value: sarama.StringEncoder(msgToSend)}, nil

}

// processSQLEvent process Sql Event
func (k *Kafka) processSQLEvent(sqlEvent *events.SQLEvent) (*sarama.ProducerMessage, error) {
	var topic string
	if len(k.kafkaConf.Topic) == 0 {
		topic = k.kafkaConf.TopicPrefix + sqlEvent.Environment + "_" + sqlEvent.Database
	} else {
		topic = k.kafkaConf.Topic
	}
	//log.WithFields(log.Fields{
	//	"topic": topic,
	//}).Debug("KafkaSink: Sending event to topic")
	key := sqlEvent.PrimaryKey
	serializedEventPayload, err := json.Marshal(sqlEvent)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("KafkaSink Marshal Error")
	}
	if len(k.encryptionkey) > 0 {

		result, err := util.EncryptString(string(serializedEventPayload[:]), k.encryptionkey)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("KafkaSink Encrypt Error")
		}
		serializedEventPayload = []byte(result)
	}
	//if message is heavier than threshold we must skip it
	if len(serializedEventPayload) > k.kafkaConf.MaxMessageBytes {
		errMsg := "KafkaSink: Skip too heavy event"
		log.WithFields(log.Fields{
			"size":      len(serializedEventPayload),
			"threshold": k.kafkaConf.MaxMessageBytes,
			"event":     sqlEvent.Database + "." + sqlEvent.Table,
		}).Debug(errMsg)
		return nil, errors.New(errMsg)
	}

	return &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(string(serializedEventPayload))}, nil
}

// processKafkaMsg process Kafka Msg
func (k *Kafka) processKafkaMsg(kafkaMsg *sarama.ConsumerMessage) (*sarama.ProducerMessage, error) {
	log.WithFields(log.Fields{
		"topic": kafkaMsg.Topic,
		"Value": kafkaMsg.Value,
	}).Debug("KafkaSink: incoming Msg")

	var topic string
	if len(k.kafkaConf.Topic) == 0 {
		topic = k.kafkaConf.TopicPrefix + kafkaMsg.Topic
	} else {
		topic = k.kafkaConf.Topic
	}
	var msgToSend []byte
	if k.encryptionkey != "" {
		var err error
		msgToSend, err = util.EncryptBytes(kafkaMsg.Value, k.encryptionkey)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("KafkaSink Encrypt Error: ")
		}
	} else {
		msgToSend = kafkaMsg.Value
	}
	//if message is heavier than threshold we must skip it
	if len(msgToSend) > k.kafkaConf.MaxMessageBytes {
		errMsg := "KafkaSink: Skip too heavy event"
		log.WithFields(log.Fields{
			"size":      len(msgToSend),
			"threshold": k.kafkaConf.MaxMessageBytes,
			"topic":     kafkaMsg.Topic,
		}).Debug(errMsg)
		return nil, errors.New(errMsg)
	}

	return &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(kafkaMsg.Key), Value: sarama.StringEncoder(msgToSend)}, nil
}

// startProducer send message to kafka
func (k *Kafka) startProducer(in chan *sarama.ProducerMessage, stop chan error) {

	saramaConf := sarama.NewConfig()
	saramaConf.Producer.Retry.Max = 5
	saramaConf.Producer.Return.Successes = true
	saramaConf.Producer.MaxMessageBytes = k.kafkaConf.MaxMessageBytes

	if len(k.kafkaConf.ClientID) == 0 {
		log.Debug("No client id")
		saramaConf.Net.SASL.Enable = true
		log.Debug("SASL CLient")
		if k.kafkaConf.TLS {
			log.Debug("TLS connection")
			saramaConf.Net.TLS.Enable = k.kafkaConf.TLS
		}
		saramaConf.Net.SASL.User = k.kafkaConf.Producer.User
		saramaConf.Net.SASL.Password = k.kafkaConf.Producer.Password
	} else {
		saramaConf.ClientID = k.kafkaConf.ClientID
		log.WithFields(log.Fields{
			"clientID": saramaConf.ClientID,
		}).Debug("sink_conf sarama_conf ")
	}

	if err := saramaConf.Validate(); err != nil {
		errMsg := "startProducer: sarama configuration not valid : "
		stop <- errors.Annotate(err, errMsg)
	}
	producer, err := sarama.NewSyncProducer(k.kafkaConf.Brokers, saramaConf)
	if err != nil {
		errMsg := "Error when Initialize NewSyncProducer"
		stop <- errors.Annotate(err, errMsg)
	}

	log.Debug("startProducer: New SyncProducer created")

	defer func() {
		if err := producer.Close(); err != nil {
			stop <- errors.Annotate(err, "Error while closing kafka producer")
		}
		log.Debug("Successfully Closed kafka producer")
	}()

	k.producerLoop(producer, in)

	log.Info("startProducer")
}

func (k *Kafka) producerLoop(producer sarama.SyncProducer, in chan *sarama.ProducerMessage) {
	var (
		msg                *sarama.ProducerMessage
		msgs               []*sarama.ProducerMessage
		lastSend, timepass int64
		msgsSize, msgSize  int
	)
	lastSend = time.Now().Unix()
	for {
		select {
		case msg = <-in:
			if msg.Value.Length() != 0 {

				//calcul size
				msgSize = msgByteSize(msg)
				if msgSize > k.kafkaConf.MaxMessageBytes {
					log.Warn("Skip Message")

				} else if msgsSize+msgSize < k.kafkaConf.MaxMessageBytes {
					msgs = append(msgs, msg)
					msgsSize += msgSize
				} else {
					lastSend = sendMsg(msgs, producer)
					msgs = []*sarama.ProducerMessage{}
					msgs = append(msgs, msg)
					msgsSize = msgSize
				}

			}

			//use to clear slice
			now := time.Now().Unix()
			timepass = now - lastSend
			if timepass >= 1 {
				lastSend = sendMsg(msgs, producer)
				msgs = []*sarama.ProducerMessage{}
				msgsSize = 0
			}

		case <-k.stop:
			log.Info("startProducer: Signal received, closing producer")
			return
		}
	}
}

func sendMsg(msgs []*sarama.ProducerMessage, producer sarama.SyncProducer) int64 {
	retries := 0
	err := producer.SendMessages(msgs)
	for err != nil {
		producerErrs := err.(sarama.ProducerErrors)
		msgs = []*sarama.ProducerMessage{}
		for _, v := range producerErrs {
			log.WithFields(log.Fields{
				"error": v.Err,
			}).Warn("failed to push to kafka")
			msgs = append(msgs, v.Msg)
		}

		if retries > 20 {
			log.WithFields(log.Fields{
				"nbRetry": retries,
			}).Panic("Failed to push event to kafka. Stopping agent.")
		}
		retries++
		err = producer.SendMessages(msgs)
	}
	return time.Now().Unix()
}

func msgByteSize(msg *sarama.ProducerMessage) int {
	// the metadata overhead of CRC, flags, etc.
	size := 26
	for _, h := range msg.Headers {
		size += len(h.Key) + len(h.Value) + 2*binary.MaxVarintLen32
	}
	if msg.Key != nil {
		size += msg.Key.Length()
	}
	if msg.Value != nil {
		size += msg.Value.Length()
	}
	return size
}
