package main

import (
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/jiaxwu/kttl/custom_ttl_test"
	"log"
	"strconv"
	"time"
)

func main() {
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(custom_ttl_test.Addrs, producerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 10; i++ {
		msg := &sarama.ProducerMessage{
			Topic: custom_ttl_test.Topic,
			Value: sarama.ByteEncoder(custom_ttl_test.Topic + strconv.Itoa(i)),
			// 设置为当前时间
			Timestamp: time.Now(),
			// 把消息的过期时间添加到消息的Headers字段里
			Headers: []sarama.RecordHeader{{
				Key:   sarama.ByteEncoder("ttl"),
				Value: int64ToBytes(int64(time.Second * time.Duration(i))),
			}},
		}
		if _, _, err := producer.SendMessage(msg); err != nil {
			log.Println(err)
		}
	}
}

// int64转bytes
func int64ToBytes(n int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(n))
	return bytes
}
