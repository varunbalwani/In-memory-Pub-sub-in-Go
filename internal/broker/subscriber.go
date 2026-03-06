package broker

// BackpressurePolicy controls what happens when a subscriber's send queue is full.
type BackpressurePolicy string

const (
	PolicyDrop       BackpressurePolicy = "drop"
	PolicyDisconnect BackpressurePolicy = "disconnect"
)

// Connection is a forward-declared interface representing a WebSocket connection.
// The concrete type lives in the ws package; broker depends only on this interface.
type Connection interface {
	// SendCh returns the buffered channel used to deliver messages to this connection.
	// The broker writes to this channel; the connection's write loop drains it.
	SendCh() chan ServerMessage
	// Enqueue adds a message to the connection's send channel without blocking.
	// Returns false if the channel is full.
	Enqueue(msg ServerMessage) bool
	// Close tears down the connection.
	Close()
}

// Subscriber represents a single client subscribed to a topic.
type Subscriber struct {
	clientID string
	conn     Connection
	send     chan ServerMessage
}
