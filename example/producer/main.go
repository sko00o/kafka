package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/sko00o/kafka"
)

const (
	kafkaServers = "127.0.0.1:9092"
	topic        = "test_topic"
)

func main() {
	kfk := flag.String("kfk", kafkaServers, "set kafka broker list here")
	tpc := flag.String("tpc", topic, "set topic here")
	flag.Parse()

	p := kafka.NewKafkaProducerCluster(strings.Split(*kfk, ","))
	defer p.Close()

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	log.SetLevel(log.DebugLevel)

	for {
		select {
		case <-sig:
			return
		case <-time.After(time.Second * 2):
			p.Publish(*tpc, []byte(time.Now().String()))
		}
	}

}
