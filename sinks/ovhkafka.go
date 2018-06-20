package sinks

// OvhKafkaType type of sink
const OvhKafkaType = "ovhkafka"

// newOvhKafka create new ovh kafka sink
func newOvhKafka(s *Sink) (SinkI, error) {

	ksConf := &kafkaSinkConfig{}
	s.conf.Unmarshal(ksConf)

	return &Kafka{
		Sink:      s,
		kafkaConf: ksConf,
	}, nil
}
