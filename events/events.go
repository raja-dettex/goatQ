package events

import (
	"fmt"
	"strings"
)

type EVENTMessage string

const (
	EVENTRead  EVENTMessage = "READ"
	EVENTWrite EVENTMessage = "WRITE"
)

type Event struct {
	Command EVENTMessage
	Value   []byte
}

func ParseEvent(rawMessage []byte) (*Event, error) {
	if rawMessage == nil {
		return nil, fmt.Errorf("message can not be nil")
	}
	message := strings.Split(string(rawMessage), " ")
	if len(message) < 1 {
		return nil, fmt.Errorf("invalid message type")
	}
	event := &Event{
		Command: EVENTMessage(message[0]),
	}
	if event.Command == EVENTWrite {
		event.Value = []byte(message[1])
	}
	return event, nil
}
