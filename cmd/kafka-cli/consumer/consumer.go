package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/sko00o/kafka/consumer/kafkago"
	"github.com/sko00o/kafka/consumer/sarama"

	log "github.com/sirupsen/logrus"
	sk "github.com/sko00o/kafka"
	"github.com/sko00o/kafka/cmd/kafka-cli/helper"
	"github.com/spf13/cobra"
)

var (
	useSarama bool
	verbose   bool
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "consumer",
		PreRunE: helper.BindFlagConfigs(map[string][]string{
			"group":        {"group_id"},
			"start-offset": {"start_offset"},
		}),
		Run: helper.RunFunc(log.New(), func(ctx context.Context, c helper.ConfigUnmarshaler) error {
			var cfg sk.ConsumerConfig
			if err := c.Unmarshal(&cfg); err != nil {
				return err
			}
			log.Debugf("config: %+v", cfg)

			var wg sync.WaitGroup
			var consumer sk.Consumer
			{
				sLog := &SilentLogger{log.New()}
				var err error
				if useSarama {
					consumer, err = sarama.New(cfg, sarama.WithLogger(sLog))
					if err != nil {
						return err
					}
				} else {
					consumer, err = kafkago.New(cfg, kafkago.WithLogger(sLog))
					if err != nil {
						return err
					}
				}
			}
			defer func() {
				log.Info("stop consume...")
				consumer.Stop()
				wg.Wait()
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				for msg := range consumer.Receive() {
					if verbose {
						fmt.Printf("Topic: %s Partition: %d Offset: %d\n%s\n",
							msg.Topic(),
							msg.Partition(),
							msg.Offset(),
							msg.Value(),
						)
					} else {
						fmt.Printf("%s\n", msg.Value())
					}
				}
			}()

			if err := consumer.Run(); err != nil {
				return err
			}
			log.Info("start consume...")

			<-ctx.Done()
			return nil
		}),
	}

	flags := cmd.Flags()
	flags.StringSliceP("topics", "t", []string{"test_topic"}, "topics for consume")
	flags.StringP("group", "g", "test_group", "topics for consume")
	flags.StringP("start-offset", "s", "last", "set start offset")
	flags.StringP("version", "v", "", "set kafka version (optional)")

	flags.BoolVar(&useSarama, "sarama", false, "use sarama client")
	flags.BoolVar(&verbose, "verbose", false, "print verbose")

	return cmd
}

type SilentLogger struct {
	*log.Logger
}

func (l SilentLogger) Infof(format string, v ...interface{}) {
	// NOTE: kafka client is verbose, we need to keep it quite
	l.Logger.Debugf(format, v...)
}
