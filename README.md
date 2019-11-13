# simple kafka sub/pub example

## Usage

Get command line client

If you have Go 1.11 or higher. enable `go mod` by `export GO111MODULE=on`

Then run following line, tools will be installed in your `$GOPATH/bin` directory.

```sh
go get github.com/sko00o/kafka/example/consumer
go get github.com/sko00o/kafka/example/producer
```

Try other implementations.

```sh
go get github.com/sko00o/kafka/example-goka/consumer
go get github.com/sko00o/kafka/example-goka/producer
go get github.com/sko00o/kafka/example-kafka-go/consumer
go get github.com/sko00o/kafka/example-kafka-go/producer
```

## Dependencies

- [Shopify/sarama](https://github.com/Shopify/sarama)
- [bsm/sarama-cluster](https://github.com/bsm/sarama-cluster)
- [sirupsen/logrus](https://github.com/sirupsen/logrus)
