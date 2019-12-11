package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	log "github.com/sirupsen/logrus"
)

const (
	kfkServer = "127.0.0.1:9092"
	topic     = "test_topic"
	groupID   = "test_group"
)

var (
	kfk = flag.String("kfk", kfkServer, "set kafka host:port here")
	tpc = flag.String("tpc", topic, "set topics here")
	gid = flag.String("gid", groupID, "set groupID here")
	sc  = flag.Bool("s", false, "enable string convert")
	sig = make(chan os.Signal)
)

func init() {
	flag.Parse()
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go run(ctx, *kfk, *tpc, *gid, *sc)

	for {
		select {
		case s := <-sig:
			log.Infof("quit by signal %s", s)
			cancel()
		case <-ctx.Done():
			return
		}
	}
}

func run(ctx context.Context, servers, topics, groupID string, stringConvert bool) {
	h := func(ctx goka.Context, m interface{}) {
		lg := log.WithFields(log.Fields{
			"topic":     ctx.Topic(),
			"partition": ctx.Partition(),
			"offset":    ctx.Offset(),
		})

		if stringConvert {
			lg.Infof("receive: %x (%s)", m, m)
		} else {
			lg.Infof("receive: %x", m)
		}
	}

	var es goka.Edges
	for _, t := range strings.Split(topics, ",") {
		topic := strings.TrimSpace(t)
		if topic == "" {
			continue
		}
		e := goka.Input(goka.Stream(topic), new(codec.Bytes), h)
		es = append(es, e)
	}
	es = append(es, goka.Persist(new(codec.Int64)))

	g := goka.DefineGroup(goka.Group(groupID), es...)

	l := goka.WithLogger(log.New())

	brokers := strings.Split(servers, ",")
	p, err := goka.NewProcessor(brokers, g, l)
	if err != nil {
		log.Fatalln(err)
	}

	go func() {
		if err := p.Run(ctx); err != nil {
			log.Fatal(err)
		}
	}()
	log.Info("start consume...")
}
