package sarama

import (
	"bufio"
	"fmt"

	"github.com/Shopify/sarama"
)

type Producer interface {
	Close() error
	SendWithKey(topic string, key, value []byte) error
	SendMessages(msgs []*sarama.ProducerMessage) error
}

type SimpleSyncProducer struct {
	sarama.SyncProducer
	ProducerMessage
}

func (p *SimpleSyncProducer) SendWithKey(topic string, key, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if key != nil {
		msg.Key = sarama.ByteEncoder(key)
	}

	_, _, err := p.SyncProducer.SendMessage(msg)
	return err
}

type SimpleAsyncProducer struct {
	sarama.AsyncProducer
	ProducerMessage
}

func (p *SimpleAsyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	for i := range msgs {
		p.AsyncProducer.Input() <- msgs[i]
	}
	return nil
}

func (p *SimpleAsyncProducer) SendMessage(msg *sarama.ProducerMessage) error {
	p.AsyncProducer.Input() <- msg
	return nil
}

func (p *SimpleAsyncProducer) SendWithKey(topic string, key, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if key != nil {
		msg.Key = sarama.ByteEncoder(key)
	}

	return p.SendMessage(msg)
}

type ProducerMessage struct{}

func (mm ProducerMessage) Messages(scanner *bufio.Scanner, getTopic func([]byte) string, convert func([]byte) []byte) ([]*sarama.ProducerMessage, error) {
	msgs := make([]*sarama.ProducerMessage, 0, 500)
	for scanner.Scan() {
		value := convert(scanner.Bytes())
		if len(value) == 0 {
			continue
		}

		msg := &sarama.ProducerMessage{
			Topic: getTopic(value),
			Value: sarama.ByteEncoder(value),
		}
		msgs = append(msgs, msg)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan: %v", err)
	}
	return msgs, nil
}
