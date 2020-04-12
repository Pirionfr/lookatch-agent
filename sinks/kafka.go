package sinks

import (
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"

	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/Pirionfr/lookatch-agent/utils"
)

// KafkaType type of sink
const KafkaType = "Kafka"

// MaxRetry retry max
const MaxRetry = 20

type (
	// KafkaUser representation of kafka User
	KafkaUser struct {
		User     string `json:"user"`
		Password string `json:"password"`
	}

	KafkaGSSAPI struct {
		ServiceName        string `json:"service_name" mapstructure:"service_name"`
		Realm              string `json:"realm"`
		KerberosConfigPath string `json:"kerberos_config_path" mapstructure:"kerberos_config_path"`
		KeyTabPath         string `json:"key_tab_path" mapstructure:"key_tab_path"`
	}

	// KafkaSinkConfig representation of kafka sink config
	KafkaSinkConfig struct {
		TLS             bool         `json:"tls"`
		Kerberos        bool         `json:"kerberos"`
		ShuffleEvent    bool         `json:"shuffle_event" mapstructure:"shuffle_event"`
		Topic           string       `json:"topic"`
		TopicPrefix     string       `json:"topic_prefix" mapstructure:"topic_prefix"`
		ClientID        string       `json:"client_id" mapstructure:"client_id"`
		Brokers         []string     `json:"brokers"`
		Producer        *KafkaUser   `json:"Producer"`
		Consumer        *KafkaUser   `json:"consumer"`
		GSSAPI          *KafkaGSSAPI `json:"gssapi"`
		MaxMessageBytes int          `json:"max_message_bytes" mapstructure:"max_message_bytes"`
		NbProducer      int          `json:"nb_producer" mapstructure:"nb_producer"`
	}

	// KafkaSinkConfig representation of Kafka Message
	KafkaMessage struct {
		// The Kafka topic for this message.
		Topic string
		// The partitioning key for this message.
		Key string
		// The source offset of message
		Offset *events.Offset
		// The actual serialized message to store In Kafka.
		Value []byte
	}

	// Kafka representation of kafka sink
	Kafka struct {
		*Sink
		KafkaConf *KafkaSinkConfig
	}
)

// NewKafka create new kafka sink
func NewKafka(s *Sink) (SinkI, error) {
	ksConf := &KafkaSinkConfig{}
	err := s.Conf.Unmarshal(ksConf)
	if err != nil {
		return nil, err
	}

	return &Kafka{
		Sink:      s,
		KafkaConf: ksConf,
	}, nil
}

// Start kafka sink
func (k *Kafka) Start(_ ...interface{}) error {
	resendChan := make(chan *KafkaMessage, cap(k.In))
	// Notice order could get altered having more than 1 Producer
	log.WithFields(log.Fields{
		"Name":       k.Name,
		"NbProducer": k.KafkaConf.NbProducer,
	}).Debug("Starting sink producers")

	for x := 0; x < k.KafkaConf.NbProducer; x++ {
		go k.StartProducer(resendChan, k.Stop)
	}

	log.WithField("threshold", k.KafkaConf.MaxMessageBytes).Debug("KafkaSink: started with threshold")

	go k.StartConsumer(resendChan)

	//Send empty event every Minutes as to flush buffer
	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for range ticker.C {
			resendChan <- &KafkaMessage{}
		}
	}()

	return nil
}

// StartConsumer consume input chan
func (k *Kafka) StartConsumer(kafkaChan chan *KafkaMessage) {
	for eventMsg := range k.In {
		switch typedMsg := eventMsg.Payload.(type) {
		case events.SQLEvent:
			producerMsg, err := k.ProcessSQLEvent(&typedMsg)
			if err != nil {
				break
			}
			kafkaChan <- producerMsg
		case events.GenericEvent:
			producerMsg, err := k.ProcessGenericEvent(&typedMsg)
			if err != nil {
				break
			}
			kafkaChan <- producerMsg
		default:
			log.WithField("message", eventMsg).Warn("KafkaSink: event doesn't match any known type: ")
		}
	}
}

// ProcessGenericEvent process Generic Event
func (k *Kafka) ProcessGenericEvent(genericMsg *events.GenericEvent) (*KafkaMessage, error) {
	var topic string
	if len(k.KafkaConf.Topic) == 0 {
		topic = k.KafkaConf.TopicPrefix + genericMsg.Environment
	} else {
		topic = k.KafkaConf.Topic
	}
	serializedEventPayload, err := json.Marshal(genericMsg)
	if err != nil {
		log.WithError(err).Error("KafkaSink Marshal Error")
		return nil, err
	}
	key := genericMsg.Environment
	if k.KafkaConf.ShuffleEvent {
		key += genericMsg.Offset.Agent
	}

	return &KafkaMessage{
		Topic:  topic,
		Key:    key,
		Value:  serializedEventPayload,
		Offset: genericMsg.Offset,
	}, nil
}

// ProcessSQLEvent process Sql Event
func (k *Kafka) ProcessSQLEvent(sqlEvent *events.SQLEvent) (*KafkaMessage, error) {
	var topic string
	if len(k.KafkaConf.Topic) == 0 {
		topic = k.KafkaConf.TopicPrefix + sqlEvent.Environment + "_" + sqlEvent.Database
	} else {
		topic = k.KafkaConf.Topic
	}

	serializedEventPayload, err := json.Marshal(sqlEvent)
	if err != nil {
		log.WithError(err).Error("KafkaSink Marshal Error")
		return nil, err
	}
	key := sqlEvent.Table + sqlEvent.PrimaryKey
	if k.KafkaConf.ShuffleEvent {
		key += sqlEvent.Offset.Agent
	}
	producerMsg := &KafkaMessage{Topic: topic, Key: key, Value: serializedEventPayload, Offset: sqlEvent.Offset}

	return producerMsg, nil
}

// StartProducer send message to kafka
func (k *Kafka) StartProducer(in chan *KafkaMessage, stop chan error) {
	saramaConf := sarama.NewConfig()
	saramaConf.Producer.Retry.Max = 5
	saramaConf.Producer.Return.Successes = true
	saramaConf.Producer.MaxMessageBytes = k.KafkaConf.MaxMessageBytes

	switch {
	case k.KafkaConf.Kerberos:
		saramaConf.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
		saramaConf.Net.SASL.Enable = true
		saramaConf.Net.SASL.GSSAPI.ServiceName = k.KafkaConf.GSSAPI.ServiceName
		saramaConf.Net.SASL.GSSAPI.Realm = k.KafkaConf.GSSAPI.Realm
		saramaConf.Net.SASL.GSSAPI.Username = k.KafkaConf.Producer.User
		saramaConf.Net.SASL.GSSAPI.KerberosConfigPath = k.KafkaConf.GSSAPI.KerberosConfigPath

		if len(k.KafkaConf.GSSAPI.KeyTabPath) != 0 {
			saramaConf.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
			saramaConf.Net.SASL.GSSAPI.KeyTabPath = k.KafkaConf.GSSAPI.KeyTabPath
		} else {
			saramaConf.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
			saramaConf.Net.SASL.GSSAPI.Password = k.KafkaConf.Producer.Password
		}
	case len(k.KafkaConf.ClientID) == 0:
		saramaConf.Net.SASL.Enable = true
		saramaConf.Net.SASL.User = k.KafkaConf.Producer.User
		saramaConf.Net.SASL.Password = k.KafkaConf.Producer.Password
	default:
		saramaConf.ClientID = k.KafkaConf.ClientID
		log.WithField("clientID", saramaConf.ClientID).Debug("sink_conf sarama_conf")
	}

	if k.KafkaConf.TLS {
		log.Debug("TLS connection")
		saramaConf.Net.TLS.Enable = k.KafkaConf.TLS
	}

	if err := saramaConf.Validate(); err != nil {
		errMsg := "StartProducer: sarama configuration not valid : "
		stop <- errors.Annotate(err, errMsg)
	}
	producer, err := sarama.NewSyncProducer(k.KafkaConf.Brokers, saramaConf)
	if err != nil {
		errMsg := "Error when Initialize NewSyncProducer"
		stop <- errors.Annotate(err, errMsg)
	}

	log.Debug("StartProducer: New SyncProducer created")

	defer func() {
		if err := producer.Close(); err != nil {
			stop <- errors.Annotate(err, "Error while closing kafka Producer")
		}
		log.Debug("Successfully Closed kafka Producer")
	}()

	k.ProducerLoop(producer, in)
}

func (k *Kafka) ProducerLoop(producer sarama.SyncProducer, in chan *KafkaMessage) {
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
				if len(k.EncryptionKey) > 0 {
					result, err := utils.EncryptString(string(msg.Value), k.EncryptionKey)
					if err != nil {
						log.WithError(err).Error("KafkaSink Encrypt Error")
						continue
					}
					saramaMsg.Value = sarama.ByteEncoder([]byte(result))
				} else {
					saramaMsg.Value = sarama.ByteEncoder(msg.Value)
				}

				//calcul size
				msgSize = MsgByteSize(saramaMsg)
				if msgSize > k.KafkaConf.MaxMessageBytes {
					log.Warn("Skip Message")
					continue
				}
				if msgsSize+msgSize < k.KafkaConf.MaxMessageBytes {
					msgs = append(msgs, saramaMsg)
					lastOffset = msg.Offset
					msgsSize += msgSize
				} else {
					lastSend = SendMsg(msgs, producer)
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
				lastSend = SendMsg(msgs, producer)
				k.SendCommit(lastOffset)
				msgs = []*sarama.ProducerMessage{}
				msgsSize = 0
			}

		case <-k.Stop:
			log.Info("StartProducer: Signal received, closing Producer")
			return
		}
	}
}

func SendMsg(msgs []*sarama.ProducerMessage, producer sarama.SyncProducer) int64 {
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
			log.WithField("nbRetry", retries).Error("Failed to push event to kafka. Waiting 10s before retrying...")
			time.Sleep(time.Second * 10)
		}
		retries++
		err = producer.SendMessages(msgs)
	}

	return time.Now().Unix()
}

func MsgByteSize(msg *sarama.ProducerMessage) int {
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
