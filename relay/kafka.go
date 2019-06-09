package relay

import (
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"math/rand"
	"sync"
	"time"
)

type KafkaCluster struct {
	name     string
	hosts    []string
	config   sarama.Config
	producer sarama.AsyncProducer

	wg sync.WaitGroup
}

func kafkaVersion(version string) (sarama.KafkaVersion, error) {
	if version == "" {
		return sarama.V0_11_0_0, nil
	}
	v, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return sarama.V0_11_0_0, err
	}
	return v, nil
}

func newKafkaConfig(cluster KafkaClusterConfig) (*sarama.Config, error) {
	k := sarama.NewConfig()

	timeout := time.Second * 30
	k.Net.DialTimeout = timeout
	k.Net.ReadTimeout = timeout
	k.Net.WriteTimeout = timeout
	k.Net.KeepAlive = 0
	k.Producer.Timeout = 10 * time.Second

	// configure metadata update properties
	k.Metadata.Retry.Max = 3
	k.Metadata.Retry.Backoff = 250 * time.Millisecond
	k.Metadata.RefreshFrequency = 5 * time.Minute

	k.Producer.MaxMessageBytes = 1048576
	k.Producer.RequiredAcks = sarama.WaitForLocal
	k.Producer.Compression = sarama.CompressionSnappy

	k.Producer.Return.Successes = true // enable return channel for signaling
	k.Producer.Return.Errors = true

	k.Producer.Retry.Max = 1000
	k.ChannelBufferSize = 256
	k.ClientID = "influxdb-balancer"

	k.Producer.Flush.Frequency = time.Millisecond * 10
	k.Producer.Flush.MaxMessages = 100

	if err := k.Validate(); err != nil {
		return nil, err
	}
	v, err := kafkaVersion(cluster.Version)
	if err != nil {
		log.Warnw("resolve kafka version failed", "error", err)
	}
	k.Version = v

	partitioner := makePartitioner()
	k.Producer.Partitioner = partitioner

	return k, nil
}

type messagePartitioner struct {
	partitioner
}

type partitioner func(int32) int32

func (p *messagePartitioner) RequiresConsistency() bool { return false }

func (p *messagePartitioner) Partition(
	libMsg *sarama.ProducerMessage,
	numPartitions int32,
) (int32, error) {
	return p.partitioner(numPartitions), nil
}

func makePartitioner() sarama.PartitionerConstructor {
	return func(string) sarama.Partitioner {
		generator := rand.New(rand.NewSource(rand.Int63()))
		N := 1
		count := 1
		partition := int32(0)

		p := func(numPartitions int32) int32 {
			if N == count {
				count = 0
				partition = int32(generator.Intn(int(numPartitions)))
			}
			count++
			return partition
		}

		return &messagePartitioner{p}
	}
}

func NewKafkaCluster(cluster KafkaClusterConfig) (*KafkaCluster, error) {
	libCfg, err := newKafkaConfig(cluster)
	if err != nil {
		return nil, err
	}
	k := &KafkaCluster{
		name:   cluster.Name,
		hosts:  cluster.BootstrapServer,
		config: *libCfg,
	}
	return k, nil
}

func (c *KafkaCluster) Name() string {
	return c.name
}

func (c *KafkaCluster) Open() error {
	log.Debugf("connect: %v", c.hosts)
	// try to connect
	producer, err := sarama.NewAsyncProducer(c.hosts, &c.config)
	if err != nil {
		log.Errorf("Kafka connect fails with: %v", err)
		return err
	}

	c.producer = producer

	c.wg.Add(2)
	go c.successWorker(producer.Successes())
	go c.errorWorker(producer.Errors())

	return nil
}

func (c *KafkaCluster) Close() error {
	log.Info("closed kafka client")

	c.producer.AsyncClose()
	c.wg.Wait()
	c.producer = nil
	return nil
}

func (c *KafkaCluster) Produce(context context.Context, topic string, value []byte, key []byte, headers []sarama.RecordHeader) error {
	cb := newDefaultProduceCallback()
	msg := &sarama.ProducerMessage{
		Metadata:  cb,
		Topic:     topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(value),
		Timestamp: time.Now(),
		Headers:   headers,
	}
	ch := c.producer.Input()
	ch <- msg
	select {
	case err := <-cb.done:
		if err != nil {
			return err.Err
		}
		return nil
	case <-context.Done():
		return errors.New("parent context done")
	}
}

func (c *KafkaCluster) AsyncProduce(topic string, value []byte, key []byte, headers []sarama.RecordHeader, callback ProduceCallback) {
	msg := &sarama.ProducerMessage{
		Metadata:  callback,
		Topic:     topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(value),
		Timestamp: time.Now(),
		Headers:   headers,
	}
	ch := c.producer.Input()
	ch <- msg
}

func (c *KafkaCluster) successWorker(ch <-chan *sarama.ProducerMessage) {
	defer c.wg.Done()
	defer log.Debug("Stop kafka ack worker")

	for libMsg := range ch {
		cb := libMsg.Metadata.(ProduceCallback)
		cb.Done(libMsg)
	}
}

func (c *KafkaCluster) errorWorker(ch <-chan *sarama.ProducerError) {
	defer c.wg.Done()
	defer log.Debug("Stop kafka error handler")

	for errMsg := range ch {
		cb := errMsg.Msg.Metadata.(ProduceCallback)
		cb.Catch(errMsg)
	}
}

type ProduceCallback interface {
	Catch(err *sarama.ProducerError)
	Done(message *sarama.ProducerMessage)
}

type DefaultProduceCallback struct {
	done chan *sarama.ProducerError
}

func newDefaultProduceCallback() *DefaultProduceCallback {
	return &DefaultProduceCallback{
		done: make(chan *sarama.ProducerError),
	}
}

func (cb *DefaultProduceCallback) Catch(err *sarama.ProducerError) {
	cb.done <- err
}

func (cb *DefaultProduceCallback) Done(message *sarama.ProducerMessage) {
	close(cb.done)
}

type KafkaConsumer struct {
	name           string
	group          string
	hosts          []string
	topics         []string
	topicProtocols map[string]Protocol
	consumer       *cluster.Consumer
	version        sarama.KafkaVersion
	ConsumeProcessor
}

func NewKafkaConsumer(group string, cp ConsumeProcessor, config KafkaClusterConfig) (*KafkaConsumer, error) {
	v, err := kafkaVersion(config.Version)
	if err != nil {
		log.Warnw("resolve kafka version failed", "error", err)
	}
	c := &KafkaConsumer{
		name:             config.Name,
		group:            group,
		hosts:            config.BootstrapServer,
		ConsumeProcessor: cp,
		topicProtocols:   make(map[string]Protocol),
		version:          v,
	}
	return c, nil
}

func (c *KafkaConsumer) Name() string {
	return c.name
}

func (c *KafkaConsumer) AddTopic(o KafkaOutputConfig) error {
	var protocol Protocol
	for k, v := range o.Protocol {
		pf, found := protocols[k]
		if !found {
			return fmt.Errorf("unsupported protocol %v", k)
		}
		cfgMap, _ := v.(map[string]interface{})
		p, err := pf(cfgMap)
		if err != nil {
			return err
		}
		protocol = p
		break
	}
	if protocol == nil {
		protocol, _ = NewInfluxdbProtocol(nil)
	}
	c.topicProtocols[o.Topic] = protocol
	c.topics = append(c.topics, o.Topic)
	if c.consumer != nil {
		c.Close()
		return c.Open()
	} else {
		return nil
	}
}

func (c *KafkaConsumer) Open() error {
	if c.consumer == nil {
		config := cluster.NewConfig()
		config.Version = c.version
		config.Consumer.Return.Errors = true
		config.Group.Return.Notifications = true
		consumer, err := cluster.NewConsumer(c.hosts, c.group, c.topics, config)
		if err != nil {
			return err
		}
		c.consumer = consumer

		// consume errors
		go func() {
			for err := range c.consumer.Errors() {
				log.Infof("Error: %s", err.Error())
			}
		}()

		// consume notifications
		go func() {
			for ntf := range consumer.Notifications() {
				log.Infof("Rebalanced: %+v", ntf)
			}
		}()

		// consume messages, watch signals
		go func() {
			for {
				select {
				case msg, ok := <-consumer.Messages():
					if ok {
						for i := 1; ; i++ {
							p, ok := c.topicProtocols[msg.Topic]
							if !ok {
								log.Warnw("not found protocol", "topic", msg.Topic)
								break
							}
							err := c.Process(msg, p)
							if err == nil {
								break
							}
							if !c.ShouldRetry(err, i) {
								break
							}
						}
						c.consumer.MarkOffset(msg, "")
					}
				}
			}
		}()
	}

	return nil
}

func (c *KafkaConsumer) Close() {
	if c.consumer != nil {
		c.consumer.Close()
		c.consumer = nil
	}
}

type ConsumeProcessor interface {
	Process(msg *sarama.ConsumerMessage, protocol Protocol) error
	ShouldRetry(err error, fail int) bool
}
