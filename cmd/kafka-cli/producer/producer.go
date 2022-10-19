package producer

import (
	"bufio"
	"context"
	"encoding/hex"
	"math/rand"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	sk "github.com/sko00o/kafka"
	"github.com/sko00o/kafka/cmd/kafka-cli/helper"
	"github.com/sko00o/kafka/producer/kafkago"
	"github.com/sko00o/kafka/producer/sarama"
	"github.com/spf13/cobra"
)

var (
	useSarama    bool
	typeMode     bool
	sendInterval time.Duration
	topic        string
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "producer",
		PreRunE: helper.BindFlagConfigs(map[string][]string{}),
		Run: helper.RunFunc(log.New(), func(ctx context.Context, c helper.ConfigUnmarshaler) error {
			var cfg sk.ProducerConfig
			if err := c.Unmarshal(&cfg); err != nil {
				return err
			}
			log.Debugf("config: %+v", cfg)

			return runProducer(ctx, cfg, typeMode, sendInterval)
		}),
	}

	flags := cmd.Flags()
	flags.StringP("compression", "p", "", "compression for produce")
	flags.BoolP("async", "a", false, "enable async mode")
	flags.StringP("version", "v", "", "set kafka version (optional)")

	flags.StringVarP(&topic, "topic", "t", "test_topic", "topic for produce")
	flags.BoolVar(&useSarama, "sarama", false, "use sarama client")
	flags.BoolVar(&typeMode, "type", false, "type mode")
	flags.DurationVar(&sendInterval, "interval", 2*time.Second, "set send interval in auto mode")

	return cmd
}

func runProducer(ctx context.Context, cfg sk.ProducerConfig, typeMode bool, sendInterval time.Duration) error {
	var producer sk.Producer
	{
		sLog := log.New()
		var err error
		if useSarama {
			producer, err = sarama.New(cfg, sarama.WithLogger(sLog))
			if err != nil {
				return err
			}
		} else {
			producer, err = kafkago.New(cfg, kafkago.WithLogger(sLog))
			if err != nil {
				return err
			}
		}
	}
	defer producer.Stop()

	innerCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	if typeMode {
		wg.Add(1)
		go func() {
			defer wg.Done()

			input := bufio.NewScanner(os.Stdin)
			for {
				select {
				case <-innerCtx.Done():
					return
				default:
				}

				print("> ")
				input.Scan()
				msg := input.Bytes()

				start := time.Now()
				if err := producer.Send(topic, msg); err != nil {
					if err == innerCtx.Err() {
						return
					}
					log.Error(err)
					continue
				}
				log.Infof("send: %s, spent: %s", msg, time.Since(start))
			}
		}()

	} else {
		log.Infof("send random bytes every %s...", sendInterval)
		rand.Seed(time.Now().Unix())

		ticker := time.NewTicker(sendInterval)
		wg.Add(1)
		go func() {
			wg.Done()
			var msg [4]byte

			for {
				rand.Read(msg[:])
				start := time.Now()
				msg := hex.EncodeToString(msg[:])
				if err := producer.Send(topic, []byte(msg)); err != nil {
					if err == innerCtx.Err() {
						return
					}
					log.Error(err)
					continue
				}
				log.Infof("send: %s, spent: %s", msg, time.Since(start))

				select {
				case <-innerCtx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}

	<-ctx.Done()
	cancel()
	wg.Wait()
	return nil
}
