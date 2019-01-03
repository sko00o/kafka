package kafka

import (
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const (
	zkServer     = "192.168.2.43:2181"
	kafkaServer  = "192.168.2.43:9092"
	kafkaServer1 = "192.168.2.43:9093"
	kafkaServer2 = "192.168.2.43:9094"
	topic        = "test_topic"
	groupID      = "test_group"
	gapTime      = time.Second * 2
)

func TestKafkaBackend(t *testing.T) {
	ast := assert.New(t)
	testMsg := make(chan string, 1)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		consumer := NewKafkaConsumerGroupClusterWithGroupID([]string{zkServer}, []string{topic}, groupID)
		defer consumer.Close()

		msgChan, err := consumer.Receive(topic)
		ast.NoError(err)

		for {
			select {
			case <-time.After(gapTime + time.Second):
				return
			case gotMsg, ok := <-msgChan:
				if ok {
					log.Infof("received: %s", gotMsg.Value)
					msg := <-testMsg
					ast.Equal(msg, string(gotMsg.Value))
				}
			}
		}

	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		producer := NewKafkaProducerCluster([]string{kafkaServer, kafkaServer1, kafkaServer2})
		defer producer.Close()

		for times := 0; times < 5; times++ {
			msg := time.Now().String()

			select {
			case <-time.After(gapTime):
				testMsg <- msg

				producer.Publish(topic, []byte(msg))
				log.Infof("send: %s", msg)

			}
		}
	}()

	wg.Wait()
}
