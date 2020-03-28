package sinks

import (
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/Pirionfr/lookatch-agent/utils"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

// KafkaType type of sink
const KafkaType = "Kafka"

// MaxRetry retry max
const MaxRetry = 20

type (
	// kafkaUser representation of kafka User
	kafkaUser struct {
		User     string `json:"user"`
		Password string `json:"password"`
	}

	KafkaGSSAPI struct {
		ServiceName        string `json:"service_name" mapstructure:"service_name"`
		Realm              string `json:"realm"`
		KerberosConfigPath string `json:"kerberos_config_path" mapstructure:"kerberos_config_path"`
		KeyTabPath         string `json:"key_tab_path" mapstructure:"key_tab_path"`
	}

	// kafkaSinkConfig representation of kafka sink config
	kafkaSinkConfig struct {
		TLS             bool         `json:"tls"`
		Kerberos        bool         `json:"kerberos"`
		Topic           string       `json:"topic"`
		TopicPrefix     string       `json:"topic_prefix" mapstructure:"topic_prefix"`
		ClientID        string       `json:"client_id" mapstructure:"client_id"`
		Brokers         []string     `json:"brokers"`
		Producer        *kafkaUser   `json:"producer"`
		Consumer        *kafkaUser   `json:"consumer"`
		GSSAPI          *KafkaGSSAPI `json:"gssapi"`
		MaxMessageBytes int          `json:"max_message_bytes" mapstructure:"max_message_bytes"`
		NbProducer      int          `json:"nb_producer" mapstructure:"nb_producer"`
	}

	// kafkaSinkConfig representation of Kafka Message
	KafkaMessage struct {
		// The Kafka topic for this message.
		Topic string
		// The partitioning key for this message.
		Key string
		// The source offset of message
		Offset *events.Offset
		// The actual serialized message to store in Kafka.
		Value []byte
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
	err := s.conf.Unmarshal(ksConf)
	if err != nil {
		return nil, err
	}

	return &Kafka{
		Sink:      s,
		kafkaConf: ksConf,
	}, nil
}

// Start kafka sink
func (k *Kafka) Start(_ ...interface{}) error {
	resendChan := make(chan *KafkaMessage, 1000)
	// Notice order could get altered having more than 1 producer
	log.WithFields(log.Fields{
		"Name":       k.name,
		"NbProducer": k.kafkaConf.NbProducer,
	}).Debug("Starting sink producers")

	for x := 0; x < k.kafkaConf.NbProducer; x++ {
		go k.startProducer(resendChan, k.stop)
	}

	log.WithField("threshold", k.kafkaConf.MaxMessageBytes).Debug("KafkaSink: started with threshold")

	go k.startConsumer(resendChan)

	//Send empty event every Minutes as to flush buffer
	ticker := time.NewTicker(time.Minute * 1)
	go func() {
		for range ticker.C {
			resendChan <- &KafkaMessage{}
		}
	}()

	return nil
}

// startConsumer consume input chan
func (k *Kafka) startConsumer(kafkaChan chan *KafkaMessage) {
	for eventMsg := range k.in {
		switch typedMsg := eventMsg.Payload.(type) {
		case events.SQLEvent:
			producerMsg, err := k.processSQLEvent(&typedMsg)
			if err != nil {
				break
			}
			kafkaChan <- producerMsg
		case events.GenericEvent:
			producerMsg, err := k.processGenericEvent(&typedMsg)
			if err != nil {
				break
			}
			kafkaChan <- producerMsg
		default:
			log.WithField("message", eventMsg).Warn("KafkaSink: event doesn't match any known type: ")
		}
	}
}

// processGenericEvent process Generic Event
func (k *Kafka) processGenericEvent(genericMsg *events.GenericEvent) (*KafkaMessage, error) {
	var topic string
	if len(k.kafkaConf.Topic) == 0 {
		topic = k.kafkaConf.TopicPrefix + genericMsg.Environment
	} else {
		topic = k.kafkaConf.Topic
	}
	serializedEventPayload, err := json.Marshal(genericMsg)
	if err != nil {
		log.WithError(err).Error("KafkaSink Marshal Error")
		return nil, err
	}

	return &KafkaMessage{
		Topic:  topic,
		Key:    genericMsg.Environment,
		Value:  serializedEventPayload,
		Offset: genericMsg.Offset,
	}, nil
}

// processSQLEvent process Sql Event
func (k *Kafka) processSQLEvent(sqlEvent *events.SQLEvent) (*KafkaMessage, error) {
	var topic string
	if len(k.kafkaConf.Topic) == 0 {
		topic = k.kafkaConf.TopicPrefix + sqlEvent.Environment + "_" + sqlEvent.Database
	} else {
		topic = k.kafkaConf.Topic
	}

	key := sqlEvent.PrimaryKey
	serializedEventPayload, err := json.Marshal(sqlEvent)
	if err != nil {
		log.WithError(err).Error("KafkaSink Marshal Error")
		return nil, err
	}
	producerMsg := &KafkaMessage{Topic: topic, Key: key, Value: serializedEventPayload, Offset: sqlEvent.Offset}

	return producerMsg, nil
}

// startProducer send message to kafka
func (k *Kafka) startProducer(in chan *KafkaMessage, stop chan error) {
	saramaConf := sarama.NewConfig()
	saramaConf.Producer.Retry.Max = 5
	saramaConf.Producer.Return.Successes = true
	saramaConf.Producer.MaxMessageBytes = k.kafkaConf.MaxMessageBytes

	switch {
	case k.kafkaConf.Kerberos:
		saramaConf.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
		saramaConf.Net.SASL.Enable = true
		saramaConf.Net.SASL.GSSAPI.ServiceName = k.kafkaConf.GSSAPI.ServiceName
		saramaConf.Net.SASL.GSSAPI.Realm = k.kafkaConf.GSSAPI.Realm
		saramaConf.Net.SASL.GSSAPI.Username = k.kafkaConf.Producer.User
		saramaConf.Net.SASL.GSSAPI.KerberosConfigPath = k.kafkaConf.GSSAPI.KerberosConfigPath

		if len(k.kafkaConf.GSSAPI.KeyTabPath) != 0 {
			saramaConf.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
			saramaConf.Net.SASL.GSSAPI.KeyTabPath = k.kafkaConf.GSSAPI.KeyTabPath
		} else {
			saramaConf.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
			saramaConf.Net.SASL.GSSAPI.Password = k.kafkaConf.Producer.Password
		}
	case len(k.kafkaConf.ClientID) == 0:
		saramaConf.Net.SASL.Enable = true
		saramaConf.Net.SASL.User = k.kafkaConf.Producer.User
		saramaConf.Net.SASL.Password = k.kafkaConf.Producer.Password
	default:
		saramaConf.ClientID = k.kafkaConf.ClientID
		log.WithField("clientID", saramaConf.ClientID).Debug("sink_conf sarama_conf")
	}

	if k.kafkaConf.TLS {
		log.Debug("TLS connection")
		saramaConf.Net.TLS.Enable = k.kafkaConf.TLS
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

func (k *Kafka) producerLoop(producer sarama.SyncProducer, in chan *KafkaMessage) {
	var (
		msg                *KafkaMessage
		saramaMsg          *sarama.ProducerMessage
		msgs               []*sarama.ProducerMessage
		lastSend, timepass int64
		msgsSize, msgSize  int
		lastOffset         *events.Offset
	)
	lastSend = time.Now().Unix()
	for {
		select {
		case msg = <-in:
			if msg.Value != nil {
				saramaMsg = &sarama.ProducerMessage{Topic: msg.Topic, Key: sarama.ByteEncoder(msg.Key)}
				if len(k.encryptionkey) > 0 {
					result, err := utils.EncryptString(string(msg.Value), k.encryptionkey)
					if err != nil {
						log.WithError(err).Error("KafkaSink Encrypt Error")
						continue
					}
					saramaMsg.Value = sarama.ByteEncoder([]byte(result))
				} else {
					saramaMsg.Value = sarama.ByteEncoder(msg.Value)
				}

				//calcul size
				msgSize = msgByteSize(saramaMsg)
				if msgSize > k.kafkaConf.MaxMessageBytes {
					log.Warn("Skip Message")
					continue
				}
				if msgsSize+msgSize < k.kafkaConf.MaxMessageBytes {
					msgs = append(msgs, saramaMsg)
					lastOffset = msg.Offset
					msgsSize += msgSize
				} else {
					lastSend = sendMsg(msgs, producer)
					k.SendCommit(lastOffset)
					msgs = []*sarama.ProducerMessage{}
					msgs = append(msgs, saramaMsg)
					msgsSize = msgSize
				}
			}
			//use to clear slice
			now := time.Now().Unix()
			timepass = now - lastSend
			if timepass >= 1 {
				lastOffset = msg.Offset
				lastSend = sendMsg(msgs, producer)
				k.SendCommit(lastOffset)
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
			log.WithError(err).Warn("failed to push to kafka")
			msgs = append(msgs, v.Msg)
		}

		if retries > MaxRetry {
			log.WithField("nbRetry", retries).Error("Failed to push event to kafka. Stopping agent.")
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
