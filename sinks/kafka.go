package sinks

import (
	"encoding/json"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/Pirionfr/lookatch-common/events"
	"github.com/Pirionfr/lookatch-common/util"
)

const KafkaType = "kafka"

type (
	kafkaUser struct {
		User     string `json:"user"`
		Password string `json:"password"`
	}

	kafkaSinkConfig struct {
		Tls          bool       `json:"tls"`
		Topic        string     `json:"topic"`
		Topic_prefix string     `json:"topic_prefix"`
		Client_id    string     `json:"client_id"`
		Brokers      []string   `json:"brokers"`
		Producer     *kafkaUser `json:"producer"`
		Consumer     *kafkaUser `json:"consumer"`
		BatchSize    int        `json:"batchsize"`
		NbProducer   int        `json:"nbproducer"`
		Secret       string     `json:"secret"`
	}

	Kafka struct {
		*Sink
		kafkaConf *kafkaSinkConfig
	}
)

func newKafka(s *Sink) (SinkI, error) {

	ksConf := &kafkaSinkConfig{}
	s.conf.Unmarshal(ksConf)

	return &Kafka{
		Sink:      s,
		kafkaConf: ksConf,
	}, nil
}

func (k *Kafka) Start(_ ...interface{}) error {
	var topic string
	resendChan := make(chan *sarama.ProducerMessage, 10000)
	// Notice order could get altered having more than 1 producer
	log.WithFields(log.Fields{
		"NbProducer": k.kafkaConf.NbProducer,
	}).Debug("Starting sink producers")
	for x := 0; x < k.kafkaConf.NbProducer; x++ {
		go startProducer(k.kafkaConf, resendChan, k.stop)
	}

	//current kafka threshold is 10MB
	threshold := 10 << 10
	log.WithFields(log.Fields{
		"threshold": threshold,
	}).Debug("KafkaSink: started with threshold")

	go startConsumer(k.kafkaConf, k.in, topic, threshold, resendChan)

	return nil
}

func (k *Kafka) GetInputChan() chan *events.LookatchEvent {
	return k.in
}

func startConsumer(conf *kafkaSinkConfig, input chan *events.LookatchEvent, topic string, threshold int, kafkaChan chan *sarama.ProducerMessage) {
	for {
		for eventMsg := range input {

			//id event is too heavy it wont fit in kafka threshold so we have to skip it
			switch typedMsg := eventMsg.Payload.(type) {
			case *events.SqlEvent:
				producerMsg, err := processSqlEvent(typedMsg, conf, topic, threshold)
				if err != nil {
					break
				}
				kafkaChan <- producerMsg
			case *events.GenericEvent:
				producerMsg, err := processGenericEvent(typedMsg, conf, topic, threshold)
				if err != nil {
					break
				}
				kafkaChan <- producerMsg
			case *sarama.ConsumerMessage:
				producerMsg, err := processKafkaMsg(typedMsg, conf, topic, threshold)
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

func processGenericEvent(genericMsg *events.GenericEvent, conf *kafkaSinkConfig, topic string, threshold int) (*sarama.ProducerMessage, error) {
	if len(conf.Topic) == 0 {
		topic = conf.Topic_prefix + genericMsg.Environment
	} else {
		topic = conf.Topic
	}
	var msgToSend []byte
	serializedEventPayload, err := json.Marshal(genericMsg)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("KafkaSink Marshal Error")
	}
	if len(conf.Secret) > 0 {
		var err error
		msgToSend, err = util.EncryptBytes(serializedEventPayload, conf.Secret)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("KafkaSink Encrypt Error")
		}
	} else {
		msgToSend = serializedEventPayload
	}
	//if message is heavier than threshold we must skip it
	if len(msgToSend) > threshold {
		errMsg := "KafkaSink: Skip too heavy event : "
		log.WithFields(log.Fields{
			"size":      len(msgToSend),
			"threshold": threshold,
			"topic":     topic,
		}).Debug("KafkaSink: Skip too heavy event")
		return nil, errors.New(errMsg)
	}

	log.WithFields(log.Fields{
		"topic": topic,
	}).Debug("KafkaSink: sending to topic")
	return &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(genericMsg.Environment), Value: sarama.StringEncoder(msgToSend)}, nil

}

func processSqlEvent(sqlEvent *events.SqlEvent, conf *kafkaSinkConfig, topic string, threshold int) (*sarama.ProducerMessage, error) {

	if len(conf.Topic) == 0 {
		topic = conf.Topic_prefix + sqlEvent.Environment + "_" + sqlEvent.Database
	} else {
		topic = conf.Topic
	}
	log.WithFields(log.Fields{
		"topic": topic,
	}).Debug("KafkaSink: Sending event to topic")
	key := sqlEvent.PrimaryKey
	serializedEventPayload, err := json.Marshal(sqlEvent)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("KafkaSink Marshal Error")
	}
	if len(conf.Secret) > 0 {

		result, err := util.EncryptString(string(serializedEventPayload[:]), conf.Secret)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("KafkaSink Encrypt Error")
		}
		serializedEventPayload = []byte(result)
	}
	//if message is heavier than threshold we must skip it
	if len(serializedEventPayload) > threshold {
		errMsg := "KafkaSink: Skip too heavy event"
		log.WithFields(log.Fields{
			"size":      len(serializedEventPayload),
			"threshold": threshold,
			"event":     sqlEvent.Database + "." + sqlEvent.Table,
		}).Debug(errMsg)
		return nil, errors.New(errMsg)
	}

	return &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(string(serializedEventPayload))}, nil
}

func processKafkaMsg(kafkaMsg *sarama.ConsumerMessage, conf *kafkaSinkConfig, topic string, threshold int) (*sarama.ProducerMessage, error) {
	log.WithFields(log.Fields{
		"topic": kafkaMsg.Topic,
		"Value": kafkaMsg.Value,
	}).Debug("KafkaSink: incoming Msg")

	if len(conf.Topic) == 0 {
		topic = conf.Topic_prefix + kafkaMsg.Topic
	} else {
		topic = conf.Topic
	}
	var msgToSend []byte
	if conf.Secret != "" {
		var err error
		msgToSend, err = util.EncryptBytes(kafkaMsg.Value, conf.Secret)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("KafkaSink Encrypt Error: ")
		}
	} else {
		msgToSend = kafkaMsg.Value
	}
	//if message is heavier than threshold we must skip it
	if len(msgToSend) > threshold {
		errMsg := "KafkaSink: Skip too heavy event"
		log.WithFields(log.Fields{
			"size":      len(msgToSend),
			"threshold": threshold,
			"topic":     kafkaMsg.Topic,
		}).Debug(errMsg)
		return nil, errors.New(errMsg)
	}

	return &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(kafkaMsg.Key), Value: sarama.StringEncoder(msgToSend)}, nil
}

func startProducer(conf *kafkaSinkConfig, in chan *sarama.ProducerMessage, stop chan error) {
	retries := 0
	saramaConf := sarama.NewConfig()
	saramaConf.Producer.Retry.Max = 5
	saramaConf.Producer.Return.Successes = true
	if len(conf.Client_id) == 0 {
		log.Debug("No client id")
		saramaConf.Net.SASL.Enable = true
		log.Debug("SASL CLient ")
		if conf.Tls {
			log.Debug("TLS connection ")
			saramaConf.Net.TLS.Enable = conf.Tls
		}
		saramaConf.Net.SASL.User = conf.Producer.User
		saramaConf.Net.SASL.Password = conf.Producer.Password
	} else {
		saramaConf.ClientID = conf.Client_id
		log.WithFields(log.Fields{
			"clientID": saramaConf.ClientID,
		}).Debug("sink_conf sarama_conf ")
	}

	if err := saramaConf.Validate(); err != nil {
		errMsg := "startProducer: sarama configuration not valid : "
		stop <- errors.Annotate(err, errMsg)
	}
	producer, err := sarama.NewSyncProducer(conf.Brokers, saramaConf)
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

	//log.Println("DEBUG: eventProducer spawning loop")
	var (
		enqueued           int
		msg                *sarama.ProducerMessage
		msgs               []*sarama.ProducerMessage
		lastSend, timepass int64
	)
	lastSend = time.Now().Unix()
ProducerLoop:
	for {
		select {
		case msg = <-in:
			if msg.Value.Length() != 0 {
				msgs = append(msgs, msg)
				enqueued++
				timepass = time.Now().Unix() - lastSend
				if timepass >= 1 || len(msgs) >= conf.BatchSize {
					err := producer.SendMessages(msgs)
					if err != nil {
						producerErr := err.(sarama.ProducerErrors)
						for len(producerErr) > 0 && retries < 20 {
							log.Warn("basicProducer: failed to push to kafka, try to resend")
							//TODO check if fisrt error is first event
							for i, v := range msgs {
								if v.Value == producerErr[0].Msg.Value {
									msgs = msgs[i:]
								}
							}
							retries++
							//resend
							err = producer.SendMessages(msgs)
							if err != nil {
								producerErr = err.(sarama.ProducerErrors)
							} else {
								continue
							}

						}
						if retries >= 20 {
							log.WithFields(log.Fields{
								"nbRetry": retries,
							}).Panic("Failed to push event to kafka. Stopping agent.")
						}
					}

					log.WithFields(log.Fields{
						"size":     len(msgs),
						"Timepass": timepass,
						"enqueued": enqueued,
					}).Debug("basic producer length")
					msgs = []*sarama.ProducerMessage{}
					retries = 0
					enqueued = 0

				}
			}
		case <-stop:
			log.Info("startProducer: Signal received, closing producer")
			break ProducerLoop
		}
	}
	log.WithFields(log.Fields{
		"Enqueued": enqueued,
	}).Info("startProducer")
}
