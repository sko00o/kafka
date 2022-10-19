package kafka

type Consumer interface {
	Run() error
	Stop()
	Receive() <-chan Message
}

type Producer interface {
	Stop()
	Send(topic string, value []byte) error
	SendWithKey(topic string, key, value []byte) error
}

type Message interface {
	Value() []byte
	Topic() string
	Partition() int32
	Offset() int64
}
