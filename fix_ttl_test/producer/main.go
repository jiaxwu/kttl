package main

import (
	"github.com/Shopify/sarama"
	"github.com/jiaxwu/kttl/fix_ttl_test"
	"log"
	"strconv"
	"time"
)

func main() {
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(fix_ttl_test.Addrs, producerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 10; i++ {
		msg := &sarama.ProducerMessage{
			Topic: fix_ttl_test.Topic,
			Value: sarama.ByteEncoder(fix_ttl_test.Topic + strconv.Itoa(i)),
			// 设置为当前时间
			Timestamp: time.Now(),
		}
		if _, _, err := producer.SendMessage(msg); err != nil {
			log.Println(err)
		}
	}
}
