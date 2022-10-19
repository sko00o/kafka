package kafkago

type Logger interface {
	Infof(string, ...interface{})
	Errorf(string, ...interface{})
}
