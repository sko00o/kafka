package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/sko00o/kafka/cmd/kafka-cli/consumer"
	"github.com/sko00o/kafka/cmd/kafka-cli/helper"
	"github.com/sko00o/kafka/cmd/kafka-cli/producer"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use: "kafka-cli",
	PersistentPreRunE: helper.PersistentBindFlagConfigs(map[string][]string{
		"brokers": {"addresses"},
	}),
}

func init() {
	flags := rootCmd.PersistentFlags()
	flags.StringSliceP("brokers", "k", []string{"127.0.0.1:9092"}, "kafka brokers")

	rootCmd.AddCommand(
		consumer.NewCommand(),
		producer.NewCommand(),
	)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
