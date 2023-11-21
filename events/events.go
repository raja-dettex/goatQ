package events

import (
	"fmt"
	"strings"
)

type EVENTMessage string

const (
	EVENTRead  EVENTMessage = "READ"
	EVENTWrite EVENTMessage = "WRITE"
	EVENTJoin  EVENTMessage = "JOIN"
)

type Event struct {
	Command    EVENTMessage
	Value      []byte
	RemoteAddr string
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
	if event.Command == EVENTJoin {
		event.RemoteAddr = message[1]
	}
	return event, nil
}
