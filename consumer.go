package kafka

import (
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	log "github.com/sirupsen/logrus"
)

type KafkaConsumerCluster struct {
	consumer *cluster.Consumer

	receiver sync.Map

	stop chan interface{}
}

var (
	GroupID = "demo-group"
)

func NewKafkaConsumerGroupCluster(addrs []string, topics []string) *KafkaConsumerCluster {
	return NewKafkaConsumerGroupClusterWithGroupID(addrs, topics, GroupID)
}

func NewKafkaConsumerGroupClusterWithGroupID(addrs []string, topics []string, gID string) *KafkaConsumerCluster {
	cfg := cluster.NewConfig()
	cfg.Consumer.Return.Errors = true

	consumer, err := cluster.NewConsumer(addrs, gID, topics, cfg)
	if err != nil {
		panic(err)
	}

	// Track errors
	go func() {
		for err := range consumer.Errors() {
			log.Errorf("consumer error: %v", err.Error())
		}
	}()

	kc := &KafkaConsumerCluster{
		stop:     make(chan interface{}),
		consumer: consumer,
	}

	// init topic receiver
	for _, topic := range topics {
		kc.receiver.Store(topic, make(chan *sarama.ConsumerMessage))
	}

	// consume messages
	go func() {
		for {
			select {
			case <-kc.stop:
				return
			case msg, ok := <-consumer.Messages():
				if ok {
					if msgChan, ok := kc.receiver.Load(msg.Topic); ok {
						msgChan.(chan *sarama.ConsumerMessage) <- msg
					}
					consumer.MarkOffset(msg, "")
				}
			}
		}
	}()

	return kc
}

func (k *KafkaConsumerCluster) Receive(topic string) (chan *sarama.ConsumerMessage, error) {

	msgChan, ok := k.receiver.Load(topic)
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	return msgChan.(chan *sarama.ConsumerMessage), nil
}

func (k *KafkaConsumerCluster) Close() {
	log.Infoln("stop consume")

	close(k.stop)
	if err := k.consumer.Close(); err != nil {
		log.Error("stop consume error: %s", err)
	}

}
