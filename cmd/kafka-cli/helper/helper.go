package helper

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type (
	CobraRun  func(cmd *cobra.Command, args []string)
	CobraRunE func(cmd *cobra.Command, args []string) error
	ctxRun    func(ctx context.Context, cmd *cobra.Command, args []string) error
)

type ConfigUnmarshaler interface {
	Unmarshal(interface{}, ...viper.DecoderConfigOption) error
}

const (
	ConfigKeyDelimiter = `\`
	FlagKeyDelimiter   = "."
)

var (
	allConfig = viper.NewWithOptions(
		viper.KeyDelimiter(ConfigKeyDelimiter),
	)
)

func BindFlagConfigs(flagConfigs map[string][]string) CobraRunE {
	return func(cmd *cobra.Command, _ []string) error {
		for f, c := range flagConfigs {
			if err := BindPFlagGetter(
				ConfigKey(c...),
				CommandFlag(cmd, f),
			); err != nil {
				return err
			}
		}

		return nil
	}
}

func PersistentBindFlagConfigs(flagConfigs map[string][]string) CobraRunE {
	return func(cmd *cobra.Command, _ []string) error {
		if err := BindPFlags(cmd.Flags()); err != nil {
			return fmt.Errorf("bind pflags: %w", err)
		}

		for f, c := range flagConfigs {
			if err := BindPFlagGetter(
				ConfigKey(c...),
				CommandFlag(cmd, f),
			); err != nil {
				return err
			}
		}

		return nil
	}
}

func BindPFlag(key string, flag *pflag.Flag) error {
	return allConfig.BindPFlag(key, flag)
}

func BindPFlags(flags *pflag.FlagSet) (err error) {
	flags.VisitAll(func(flag *pflag.Flag) {
		vKeyName := strings.ReplaceAll(
			flag.Name,
			FlagKeyDelimiter,
			ConfigKeyDelimiter,
		)
		if err = BindPFlag(vKeyName, flag); err != nil {
			return
		}
	})

	return nil
}

type (
	KeyGetter  func() string
	FlagGetter func() *pflag.Flag
)

func BindPFlagGetter(key KeyGetter, flag FlagGetter) error {
	return BindPFlag(key(), flag())
}

func ConfigKey(args ...string) func() string {
	return func() string {
		return strings.Join(args, ConfigKeyDelimiter)
	}
}

func CommandFlag(cmd *cobra.Command, flagName string) func() *pflag.Flag {
	return func() *pflag.Flag {
		if cmd == nil {
			return nil
		}
		return cmd.Flags().Lookup(flagName)
	}
}

type Logger interface {
	Info(args ...interface{})
	Infof(tmpl string, args ...interface{})
	Errorf(tmpl string, args ...interface{})
}

func RunFunc(log Logger, f func(ctx context.Context, cfg ConfigUnmarshaler) error) CobraRun {
	return elegantQuit(log, func(ctx context.Context, _ *cobra.Command, _ []string) error {
		return f(ctx, allConfig)
	})
}

func elegantQuit(log Logger, fn ctxRun) CobraRun {
	if log == nil {
		log = &NoLog{}
	}
	ctx, cancel := context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	return func(cmd *cobra.Command, args []string) {
		quit := make(chan error, 1)
		go func() {
			quit <- fn(ctx, cmd, args)
		}()

		var willQuit bool
		for {
			select {
			case err := <-quit:
				if err != nil {
					log.Errorf("stopped: %v", err)
					return
				}

				if willQuit {
					log.Info("stopped")
				}
				return
			case s := <-sig:
				if !willQuit {
					cancel()
					willQuit = true
					log.Infof("receive stop signal %v", s)
					continue
				}

				log.Info("force stopped")
				return
			}
		}
	}
}

type NoLog struct {
}

func (n NoLog) Info(_ ...interface{}) {
}

func (n NoLog) Infof(_ string, _ ...interface{}) {
}

func (n NoLog) Errorf(_ string, _ ...interface{}) {
}
