package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/jiaxwu/kttl/fix_ttl_test"
	"log"
	"sync"
	"time"
)

func main() {
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Return.Errors = false
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	consumerConfig.Consumer.Interceptors = append(consumerConfig.Consumer.Interceptors)
	consumerGroup, err := sarama.NewConsumerGroup(fix_ttl_test.Addrs, fix_ttl_test.Group, consumerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer consumerGroup.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	consumer := NewFixTTLConsumer(time.Second * 5)
	go func() {
		var err error
		for {
			if err = consumerGroup.Consume(context.Background(), []string{fix_ttl_test.Topic}, consumer); err != nil {
				break
			}
		}
		defer wg.Done()
	}()
	wg.Wait()
}

type FixTTLConsumer struct {
	ttl time.Duration // 消息过期时间
}

func NewFixTTLConsumer(ttl time.Duration) *FixTTLConsumer {
	return &FixTTLConsumer{ttl: ttl}
}

func (c *FixTTLConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// 判断消息是否过期
		now := time.Now()
		if now.Sub(message.Timestamp) >= c.ttl {
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

func (c *FixTTLConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *FixTTLConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
