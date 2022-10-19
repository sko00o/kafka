package kafka

import (
	"time"
)

type ConsumerConfig struct {
	WorkerCnt   uint32   `mapstructure:"worker_cnt"`
	Addresses   []string `mapstructure:"addresses"`
	Topics      []string `mapstructure:"topics"`
	GroupID     string   `mapstructure:"group_id"`
	StartOffset string   `mapstructure:"start_offset"`

	// sarama only
	Version      string `mapstructure:"version"`
	EnableErrors bool   `mapstructure:"enable_errors"`

	MinBytes         int           `mapstructure:"min_bytes"`
	MaxBytes         int           `mapstructure:"max_bytes"`
	CommitSync       bool          `mapstructure:"commit_sync"`
	CommitInterval   time.Duration `mapstructure:"commit_interval"`
	SessionTimeout   time.Duration `mapstructure:"session_timeout"`
	RebalanceTimeout time.Duration `mapstructure:"rebalance_timeout"`
}

type ProducerConfig struct {
	Addresses   []string `mapstructure:"addresses"`
	Async       bool     `mapstructure:"async"`
	Compression string   `mapstructure:"compression"`

	// sarama only
	Version           string        `mapstructure:"version"`
	EnableAsyncErrors bool          `mapstructure:"enable_async_errors"`
	BufferSize        int           `mapstructure:"buffer_size"`
	DialTimeout       time.Duration `mapstructure:"dial_timeout"`

	// kafka-go only
	Balancer           string `mapstructure:"balancer"`
	BalancerConsistent bool   `mapstructure:"balancer_consistent"`
	BatchQueueSize     int    `mapstructure:"batch_queue_size"`

	MaxAttempts  int           `mapstructure:"max_attempts"`
	RequiredAcks int           `mapstructure:"required_acks"`
	BatchSize    int           `mapstructure:"batch_size"`
	BatchBytes   int64         `mapstructure:"batch_bytes"`
	BatchTimeout time.Duration `mapstructure:"batch_timeout"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`

	SASL *struct {
		Mechanism string `mapstructure:"mechanism"`
		Username  string `mapstructure:"username"`
		Password  string `mapstructure:"password"`
	} `mapstructure:"sasl"`
}
