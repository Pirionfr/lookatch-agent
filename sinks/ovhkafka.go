package sinks

const OvhKafkaType = "ovhkafka"

func newOvhKafka(s *Sink) (SinkI, error) {

	ksConf := &kafkaSinkConfig{}
	s.conf.Unmarshal(ksConf)

	return &Kafka{
		Sink:      s,
		kafkaConf: ksConf,
	}, nil
}
