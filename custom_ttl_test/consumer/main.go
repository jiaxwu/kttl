package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/jiaxwu/kttl/custom_ttl_test"
	"log"
	"sync"
	"time"
)

func main() {
	consumerConfig := sarama.NewConfig()
	consumerGroup, err := sarama.NewConsumerGroup(custom_ttl_test.Addrs, custom_ttl_test.Group, consumerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer consumerGroup.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	consumer := NewCustomTTLConsumer()
	go func() {
		var err error
		for {
			if err = consumerGroup.Consume(context.Background(), []string{custom_ttl_test.Topic}, consumer); err != nil {
				break
			}
		}
		defer wg.Done()
	}()
	wg.Wait()
}

type CustomTTLConsumer struct{}

func NewCustomTTLConsumer() *CustomTTLConsumer {
	return &CustomTTLConsumer{}
}

func (c *CustomTTLConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// 判断消息是否过期
		// 从消息的Headers字段获取ttl
		var ttl time.Duration
		for _, header := range message.Headers {
			if bytes.Equal(header.Key, []byte("ttl")) {
				ttl = time.Duration(binary.BigEndian.Uint64(header.Value))
				break
			}
		}
		now := time.Now()
		if now.Sub(message.Timestamp) >= ttl {
			// 过期则丢弃消息
			fmt.Println("消息过期：", message.Value, message.Timestamp)
			session.MarkMessage(message, "")
			continue
		}

		// 有效的消息
		fmt.Println("消息有效：", message.Value, message.Timestamp)
		session.MarkMessage(message, "")
	}
	return nil
}

func (c *CustomTTLConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *CustomTTLConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
