package relay

import (
	"errors"
	"github.com/influxdata/influxdb/models"
	jsoniter "github.com/json-iterator/go"
	"net/url"
	"time"
	"unsafe"

	"github.com/Shopify/sarama"
)

type Protocol interface {
	Decode(msg *sarama.ConsumerMessage) (query, auth string, body []byte, err error)
}

type ProtocolFactory = func(map[string]interface{}) (Protocol, error)

type InfluxdbProtocol struct{}

func (*InfluxdbProtocol) Decode(msg *sarama.ConsumerMessage) (query, auth string, body []byte, err error) {
	for _, h := range msg.Headers {
		switch string(h.Key) {
		case "query":
			query = string(h.Value)
		case "auth":
			auth = string(h.Value)
		}
	}
	if query == "" {
		err = errors.New("kafka message bad headers, no query head")
		return
	}
	body = msg.Value
	return
}

func NewInfluxdbProtocol(_ map[string]interface{}) (Protocol, error) {
	return new(InfluxdbProtocol), nil
}

type FalconProtocol struct {
	query string
}

type FalconMessage struct {
	Step      int64             `json:"step"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
	Metric    string            `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	OtherTags map[string]string `json:"other_tags"`
}

func DecodeFalconMessage(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	fm := (*FalconMessage)(ptr)
	for {
		field := iter.ReadObject()
		if field == "" {
			return
		}
		switch field {
		case "step":
			fm.Step = iter.ReadInt64()
		case "value":
			fm.Value = iter.ReadFloat64()
		case "tags":
			iter.ReadMapCB(func(iterator *jsoniter.Iterator, s string) bool {
				if fm.Tags == nil {
					fm.Tags = make(map[string]string)
				}
				fm.Tags[s] = iterator.ReadString()
				return true
			})
		case "metric":
			fm.Metric = iter.ReadString()
		case "timestamp":
			fm.Timestamp = iter.ReadInt64()
		default:
			if fm.OtherTags == nil {
				fm.OtherTags = make(map[string]string)
			}
			fm.OtherTags[field] = iter.ReadString()
		}
	}
}

func init()  {
	jsoniter.RegisterTypeDecoderFunc("relay.FalconMessage", DecodeFalconMessage)
}

func (p *FalconProtocol) Decode(msg *sarama.ConsumerMessage) (query, auth string, body []byte, err error) {
	query = p.query
	fm := FalconMessage{}
	if err := jsoniter.Unmarshal(msg.Value, &fm); err != nil {
		return "", "", nil, err
	}
	tags := make(map[string]string)
	for k, v := range fm.Tags {
		tags["tags."+k] = v
	}
	for k, v := range fm.OtherTags {
		tags[k] = v
	}
	point, e := models.NewPoint(fm.Metric, models.NewTags(tags), map[string]interface{}{"value": fm.Value}, time.Unix(fm.Timestamp, 0))
	if e != nil {
		err = e
		return
	}
	body = []byte(point.PrecisionString("s"))
	return
}

func NewFalconProtocol(cfgMap map[string]interface{}) (Protocol, error) {
	db, _ := cfgMap["db"].(string)
	if db == "" {
		db = "falcon"
	}
	values := make(url.Values)
	values.Set("db", db)
	values.Set("precision", "s")
	return &FalconProtocol{query: values.Encode()}, nil
}

var protocols = map[string]ProtocolFactory{
	"influxdb": NewInfluxdbProtocol,
	"falcon":   NewFalconProtocol,
}
