package sarama

type Logger interface {
	Infof(string, ...interface{})
	Errorf(string, ...interface{})
}
