package relay

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDecodeFalconMessage(t *testing.T) {
	now := time.Now()
	str := fmt.Sprintf(`{"metric":"TcpExt.TCPMemoryPressures","endpoint":"kvm-p-127030.hz.td","timestamp":%d,"step":60,"value":1,"tags":{"hello":"world"},"app_name":"kvm","grp_name":"kvm","room":"production","env":"HZ"}`, now.Unix())
	fmt.Println(str)
	fm := FalconMessage{}
	err := jsoniter.UnmarshalFromString(str, &fm)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "TcpExt.TCPMemoryPressures", fm.Metric)
	assert.EqualValues(t, map[string]string{"hello": "world"}, fm.Tags)
	assert.EqualValues(t, map[string]string{"endpoint": "kvm-p-127030.hz.td", "app_name": "kvm", "grp_name": "kvm", "room": "production", "env": "HZ"}, fm.OtherTags)
	assert.EqualValues(t, now.Unix(), fm.Timestamp)
	assert.EqualValues(t, 60, fm.Step)
	assert.EqualValues(t, 1, fm.Value)
}
