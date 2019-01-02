package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
	cluster "github.com/meitu/go-consumergroup"
	log "github.com/sirupsen/logrus"
)

type KafkaConsumerCluster struct {
	consumer *cluster.ConsumerGroup
}

var (
	GroupID = "demo-group"
)

func NewKafkaConsumerGroupCluster(addrs []string, topics []string) *KafkaConsumerCluster {
	return NewKafkaConsumerGroupClusterWithGroupID(addrs, topics, GroupID)
}

func NewKafkaConsumerGroupClusterWithGroupID(addrs []string, topics []string, gID string) *KafkaConsumerCluster {
	cfg := cluster.NewConfig()
	cfg.ZkList = addrs
	cfg.TopicList = topics
	cfg.GroupID = gID

	consumer, err := cluster.NewConsumerGroup(cfg)
	if err != nil {
		panic(err)
	}

	if err = consumer.Start(); err != nil {
		panic(err)
	}

	kc := &KafkaConsumerCluster{
		consumer: consumer,
	}

	return kc
}

func (k *KafkaConsumerCluster) Receive(topic string) (<-chan *sarama.ConsumerMessage, error) {

	// deal with error
	go func() {
		if topicErrChan, ok := k.consumer.GetErrors(topic); ok {
			for err := range topicErrChan {
				if err != nil {
					log.WithField("topic", topic).Errorf("consumer error: %v", err)
				}
			}
		}
	}()

	msgChan, ok := k.consumer.GetMessages(topic)
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	return msgChan, nil
}

func (k *KafkaConsumerCluster) Close() {
	log.Infoln("stop consume")
	k.consumer.Stop()
}
