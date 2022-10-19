package kafkago

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Close() error
	SendWithKey(topic string, key, value []byte) error
}

type Writer interface {
	Close() error
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type SimpleKafkaGoProducer struct {
	Writer
	ctx         context.Context
	needCopyMsg bool
}

func (p *SimpleKafkaGoProducer) protectMsg(msg []byte) []byte {
	// NOTE: we need allocate new memory for this msg,
	// in case async producer got ridiculous issue.
	if !p.needCopyMsg {
		return msg
	}
	cpMsg := make([]byte, len(msg))
	copy(cpMsg, msg)
	return cpMsg
}

func (p *SimpleKafkaGoProducer) SendWithKey(topic string, key, value []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Key:   key,
		Value: p.protectMsg(value),
	}
	return p.Writer.WriteMessages(p.ctx, msg)
}

type batchWriter struct {
	*kafka.Writer
	log Logger

	// limit max goroutine number
	sema chan struct{}
	// wait for all goroutine
	wg *sync.WaitGroup
}

func (w *batchWriter) WriteMessages(ctx context.Context, msg ...kafka.Message) error {
	w.wg.Add(1)
	w.sema <- struct{}{}
	go func() {
		defer func() {
			<-w.sema
			w.wg.Done()
		}()

		if err := w.Writer.WriteMessages(ctx, msg...); err != nil {
			if w.log != nil {
				w.log.Errorf("write messages: %v", err)
			}
		}
	}()
	return nil
}

func (w *batchWriter) Close() error {
	w.wg.Wait()
	close(w.sema)
	return w.Writer.Close()
}
