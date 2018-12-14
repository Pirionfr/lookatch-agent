package control

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Pirionfr/lookatch-agent/rpc"
	"github.com/stretchr/testify/assert"
)

//emulation of sink decoding message
func decodeSinkMessage(message rpc.Message) *Sink {

	switch message.Type {
	case TypeAgent:
		break
	case TypeSink:
		c := &Sink{}
		json.Unmarshal(message.Payload, c)
		fmt.Println(c.GetAction())
		return c
	case TypeSource:
		break
	}
	return nil
}

//emulation of source decoding message
func decodeSourceMessage(message rpc.Message) *Source {

	switch message.Type {
	case TypeAgent:
		break
	case TypeSink:
		break
	case TypeSource:
		c := &Source{}
		json.Unmarshal(message.Payload, c)
		fmt.Println(c.GetAction())
		return c
	}
	return nil
}

//emulation of Agent decoding message
func decodeAgentMessage(message rpc.Message) *Agent {

	switch message.Type {
	case TypeAgent:
		c := &Agent{}
		json.Unmarshal(message.Payload, c)
		fmt.Println(c.GetAction())
		return c
	case TypeSink:
		break
	case TypeSource:
		break
	}
	return nil
}

// Test Agent controls messages encoding and decoding
func TestSendAgentControlMessage(t *testing.T) {
	agent := &Agent{}
	actions := [...]string{AgentConfigure, AgentRestart, AgentStart, AgentStatus, AgentStop}
	for aIt := range actions {
		agent = agent.NewMessage("uuid-value", "tenant-value", actions[aIt])
		bytes, _ := json.Marshal(agent)
		decoded := decodeAgentMessage(rpc.Message{Type: TypeAgent, Payload: bytes})
		assert.EqualValuesf(t, agent, decoded, "[Control Events] [Agent] Testing ", actions[aIt], " build, Send, Decode failed")
	}
}

// Test Sink controls messages encoding and decoding
func TestSendSinkControlMessage(t *testing.T) {
	sink := &Sink{}
	actions := [...]string{
		SinkStart,
		SinkStop,
		SinkRestart,
		SinkStatus,
		SinkMeta,
		SinkSchema,
	}
	for aIt := range actions {
		sink = sink.NewMessage("uuid-value", "tenant-value", actions[aIt])
		bytes, _ := json.Marshal(sink)
		decoded := decodeSinkMessage(rpc.Message{Type: TypeSink, Payload: bytes})
		assert.EqualValuesf(t, sink, decoded, "[Control Events] [Sink] Testing ", actions[aIt], " build, Send, Decode failed")
	}
}

// Test Source controls messages encoding and decoding
func TestSendSourceControlMessage(t *testing.T) {

	source := &Source{}
	actions := [...]string{
		SourceStart,
		SourceStop,
		SourceRestart,
		SourceStatus,
		SourceMeta,
		SourceSchema,
	}
	for aIt := range actions {
		source = source.NewMessage("uuid-value", "tenant-value", actions[aIt])
		bytes, _ := json.Marshal(source)
		decoded := decodeSourceMessage(rpc.Message{Type: TypeSource, Payload: bytes})
		assert.EqualValuesf(t, source, decoded, "[Control Events] [Source] Testing ", actions[aIt], " build, Send, Decode failed")
	}
}

// Test send config
func TestSendConfig(t *testing.T) {
	agent := &Agent{}
	conf := make(map[string]interface{})
	conf["test"] = "toto"
	agent = agent.NewMessage("uuid-value", "tenant-value", AgentConfigure).WithPayload(conf)
	bytes, _ := json.Marshal(agent)
	decoded := decodeAgentMessage(rpc.Message{Type: TypeAgent, Payload: bytes})
	assert.EqualValuesf(t, agent, decoded, "[Control Events] [Agent] Testing ", AgentConfigure, " with payload failed")
}

// test get schema
func TestGetSchema(t *testing.T) {
	source := &Source{}
	conf := make(map[string]interface{})
	conf["test"] = "toto"
	source = source.NewMessage("uuid-value", "tenant-value", SourceSchema).WithPayload(conf)
	bytes, _ := json.Marshal(source)
	decoded := decodeSourceMessage(rpc.Message{Type: TypeSource, Payload: bytes})
	assert.EqualValuesf(t, source, decoded, "[Control Events] [Source] Testing ", SourceSchema, " with payload failed")
}
