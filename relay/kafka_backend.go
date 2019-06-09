package relay

import (
	"github.com/Shopify/sarama"
	"sync/atomic"
)

type KafkaBackend struct {
	name         string
	topic        string
	cluster      *KafkaCluster
	successCount int64
	dropCount    int64
}

func NewKafkaBackend(cfg *KafkaOutputConfig, cluster *KafkaCluster) (*KafkaBackend, error) {
	if cfg.Name == "" {
		log.Fatal("Name should not be empty")
	}

	return &KafkaBackend{
		name:    cfg.Name,
		cluster: cluster,
		topic:   cfg.Topic,
	}, nil
}

func (b *KafkaBackend) Name() string {
	return b.name
}

func (b *KafkaBackend) Post(buf []byte, query string, auth string) (*ResponseData, error) {
	buf1 := make([]byte, len(buf))
	copy(buf1, buf)
	hdrs := make([]sarama.RecordHeader, 2)
	hdrs[0] = sarama.RecordHeader{
		Key:   []byte("query"),
		Value: []byte(query),
	}
	hdrs[1] = sarama.RecordHeader{
		Key:   []byte("auth"),
		Value: []byte(auth),
	}
	b.cluster.AsyncProduce(b.topic, buf1, nil, hdrs, b)
	return &ResponseData{
		StatusCode: 204,
	}, nil
}

func (b *KafkaBackend) Catch(err *sarama.ProducerError) {
	atomic.AddInt64(&b.dropCount, 1)
	log.Errorw("produce failed", "kafkaBackendName", b.name)
}

func (b *KafkaBackend) Done(message *sarama.ProducerMessage) {
	atomic.AddInt64(&b.successCount, 1)
}
