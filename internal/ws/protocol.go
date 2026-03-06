package ws

import (
	"time"

	"pubsub-system/internal/broker"
)

// ClientMessage is a message sent from a WebSocket client to the broker.
type ClientMessage struct {
	Type      string          `json:"type"`
	Topic     string          `json:"topic,omitempty"`
	Message   *broker.Message `json:"message,omitempty"`
	ClientID  string          `json:"client_id,omitempty"`
	LastN     int             `json:"last_n,omitempty"`
	RequestID string          `json:"request_id,omitempty"`
}

// ServerMessage is a message sent from the broker to a WebSocket client.
// Re-exported from broker for use in the ws package.
type ServerMessage = broker.ServerMessage

// ErrorPayload carries a machine-readable error code and human message.
// Re-exported from broker for use in the ws package.
type ErrorPayload = broker.ErrorPayload

// AckMessage constructs an acknowledgement response.
func AckMessage(requestID, topic string) ServerMessage {
	return ServerMessage{
		Type:      "ack",
		RequestID: requestID,
		Topic:     topic,
		Status:    "ok",
		Ts:        time.Now(),
	}
}

// ErrorMessage constructs an error response.
func ErrorMessage(requestID, code, message string) ServerMessage {
	return ServerMessage{
		Type:      "error",
		RequestID: requestID,
		Error: &ErrorPayload{
			Code:    code,
			Message: message,
		},
		Ts: time.Now(),
	}
}

// PongMessage constructs a pong response matching the given request_id.
func PongMessage(requestID string) ServerMessage {
	return ServerMessage{
		Type:      "pong",
		RequestID: requestID,
		Ts:        time.Now(),
	}
}

// InfoMessage constructs a server-initiated info message.
func InfoMessage(topic, msg string) ServerMessage {
	return ServerMessage{
		Type:  "info",
		Topic: topic,
		Msg:   msg,
		Ts:    time.Now(),
	}
}

// DeliveryMessage constructs a message delivery envelope for a subscriber.
func DeliveryMessage(topic string, msg *broker.Message) ServerMessage {
	return ServerMessage{
		Type:    "message",
		Topic:   topic,
		Message: msg,
		Ts:      time.Now(),
	}
}
