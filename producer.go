package kafka

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type KafkaProducer struct {
	msg      chan *sarama.ProducerMessage
	stop     chan interface{}
	producer sarama.AsyncProducer
}

func NewKafkaProducer(addr string) *KafkaProducer {
	return NewKafkaProducerCluster([]string{addr})
}

func NewKafkaProducerCluster(addrs []string) *KafkaProducer {
	cfg := sarama.NewConfig()

	// if set true， you must handle the `producer.Success`
	// otherwise the channel will be blocked.
	//cfg.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(addrs, cfg)
	if err != nil {
		panic(err)
	}

	// producer errors
	go func() {
		for err := range producer.Errors() {
			log.Error(err.Error())
		}
	}()

	kp := &KafkaProducer{
		msg:      make(chan *sarama.ProducerMessage),
		stop:     make(chan interface{}),
		producer: producer,
	}

	go kp.publishLoop()

	return kp
}

func (k *KafkaProducer) publishLoop() {
	for {
		select {
		case msg := <-k.msg:
			k.producer.Input() <- msg
		case <-k.stop:
			return
		}
	}
}

func (k *KafkaProducer) Publish(topic string, data []byte) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}

	log.Debugf("publish %+v", msg)
	k.msg <- msg
}

func (k *KafkaProducer) Close() {
	log.Infoln("stop publish")
	close(k.stop)
	k.producer.AsyncClose()
}
