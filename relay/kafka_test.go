package relay

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func newKafkaClusterForTest() (*KafkaCluster, error) {
	kcc := KafkaClusterConfig{
		Name:            "local",
		Zookeeper:       []string{"127.0.0.1:2181"},
		BootstrapServer: []string{"127.0.0.1:9092"},
	}
	return NewKafkaCluster(kcc)
}

func TestNewKafkaCluster(t *testing.T) {
	kc, err := newKafkaClusterForTest()
	assert.Nil(t, err)
	assert.NotNil(t, kc)
}

func TestKafkaCluster_Open(t *testing.T) {
	kc, _ := newKafkaClusterForTest()
	err := kc.Open()
	assert.Nil(t, err)
	defer kc.Close()
}

func TestKafkaCluster_Close(t *testing.T) {
	kc, _ := newKafkaClusterForTest()
	err := kc.Open()
	assert.Nil(t, err)
	err = kc.Close()
	assert.Nil(t, err)
}

func TestKafkaCluster_Produce(t *testing.T) {
	kc, _ := newKafkaClusterForTest()
	err := kc.Open()
	assert.Nil(t, err)
	defer kc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err = kc.Produce(ctx, "test", []byte("hell o world"), []byte{}, nil)
	assert.Nil(t, err)
}

type simpleCallback string

func (s *simpleCallback) Done(message *sarama.ProducerMessage) {
	fmt.Println(string(*s))
	fmt.Println("done")
}

func (s *simpleCallback) Catch(err *sarama.ProducerError) {
	fmt.Println(string(*s))
	fmt.Println(err.Err)
}

func TestKafkaCluster_AsyncProduce(t *testing.T) {
	kc, _ := newKafkaClusterForTest()
	err := kc.Open()
	assert.Nil(t, err)
	defer kc.Close()
	sc := simpleCallback("test")
	kc.AsyncProduce("test", []byte("hell o world"), []byte{}, nil, &sc)
}
