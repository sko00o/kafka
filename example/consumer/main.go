package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/sko00o/kafka"
)

/*

# create topic first

bin/kafka-topics.sh --create \
	--topic test_topic \
	--replication-factor 1 \
	--partitions 3 \
	--zookeeper zk.domain:2181

*/

const (
	kfkServer = "127.0.0.1:9092"
	topic     = "test_topic"
	groupID   = "test_group"
)

func main() {

	kfk := flag.String("kfk", kfkServer, "set kafka host:port here")
	tpc := flag.String("tpc", topic, "set topics here")
	gid := flag.String("gid", groupID, "set groupID here")
	flag.Parse()

	c := kafka.NewKafkaConsumerGroupClusterWithGroupID(
		strings.Split(*kfk, ","),
		strings.Split(*tpc, ","),
		*gid,
	)
	defer c.Close()

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	stop := make(chan interface{})
	var wg sync.WaitGroup

	for _, t := range strings.Split(*tpc, ",") {
		msgChan, err := c.Receive(t)
		if err != nil {
			panic(err)
		}

		wg.Add(1)
		go func() {
			wg.Done()
			for {
				select {
				case <-stop:
					return
				case m, ok := <-msgChan:
					if ok {
						log.WithFields(log.Fields{
							"topic":     m.Topic,
							"partition": m.Partition,
							"offset":    m.Offset,
						}).Infof("receive: %s", m.Value)
					}
				}
			}
		}()
	}

	<-sig
	close(stop)
	wg.Wait()
}
