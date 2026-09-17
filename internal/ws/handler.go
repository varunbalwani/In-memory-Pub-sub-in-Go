package ws

import (
	"encoding/json"
	"net/http"
	"regexp"
	"sync"

	"pubsub-system/internal/broker"

	"github.com/gorilla/websocket"
)

// uuidRE is a simple UUID v4 pattern validator.
var uuidRE = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Handler upgrades HTTP connections to WebSocket and wires them to the broker.
type Handler struct {
	Broker    *broker.Broker
	QueueSize int
}

// ServeHTTP upgrades the connection, registers it, and runs the read/write loops.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	conn := &Connection{
		ws:        ws,
		send:      make(chan ServerMessage, h.QueueSize),
		control:   make(chan ServerMessage, h.QueueSize),
		broker:    h.Broker,
		done:      make(chan struct{}),
		writeDone: make(chan struct{}),
	}

	h.Broker.RegisterConnection(conn)
	go conn.writeLoop()
	conn.readLoop()
	// readLoop returned (client disconnected or error).
	// Signal writeLoop to flush and exit, then wait for it.
	conn.Close()
	<-conn.writeDone
	h.Broker.UnsubscribeAll(conn)
	h.Broker.DeregisterConnection(conn)
}

// Connection represents a single WebSocket client connection.
//
// Outbound messages travel over two channels: send carries topic-message
// deliveries, the only messages the broker's backpressure policy is allowed
// to evict; control carries acks, errors, pongs, and info notices, which are
// never evicted to make room for a topic delivery.
type Connection struct {
	ws        *websocket.Conn
	send      chan ServerMessage
	control   chan ServerMessage
	broker    *broker.Broker
	done      chan struct{}
	writeDone chan struct{}
	once      sync.Once
}

// SendCh implements broker.Connection. It returns the data channel used for
// topic-message deliveries and is the queue backpressure policies act on.
func (c *Connection) SendCh() chan ServerMessage {
	return c.send
}

// Enqueue implements broker.Connection — non-blocking send of a control/info
// message (e.g. heartbeat pings, topic_deleted notices). Never evicted.
func (c *Connection) Enqueue(msg ServerMessage) bool {
	select {
	case c.control <- msg:
		return true
	default:
		return false
	}
}

// Close implements broker.Connection — idempotent teardown.
// Closes done (signals writeLoop to flush) and the WebSocket (unblocks any pending ReadMessage).
func (c *Connection) Close() {
	c.once.Do(func() {
		close(c.done)
		c.ws.Close()
	})
}

// readLoop decodes incoming JSON frames and dispatches them.
func (c *Connection) readLoop() {
	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			return
		}

		var cm ClientMessage
		if err := json.Unmarshal(data, &cm); err != nil {
			c.enqueueOrClose(ErrorMessage("", "BAD_REQUEST", "malformed JSON"))
			continue
		}

		c.dispatch(cm)
	}
}

// writeLoop drains the send and control channels and writes JSON frames to
// the WebSocket. It owns the WebSocket close so it can flush pending
// messages before tearing down.
func (c *Connection) writeLoop() {
	defer func() {
		c.ws.Close()
		close(c.writeDone)
	}()
	for {
		select {
		case msg, ok := <-c.control:
			if !ok {
				return
			}
			if err := c.ws.WriteJSON(msg); err != nil {
				return
			}
		case msg, ok := <-c.send:
			if !ok {
				return
			}
			if err := c.ws.WriteJSON(msg); err != nil {
				return
			}
		case <-c.done:
			// Drain any messages already queued on either channel before exiting.
			for {
				select {
				case msg, ok := <-c.control:
					if !ok {
						return
					}
					_ = c.ws.WriteJSON(msg) // best-effort flush; ignore write errors
				case msg, ok := <-c.send:
					if !ok {
						return
					}
					_ = c.ws.WriteJSON(msg) // best-effort flush; ignore write errors
				default:
					return
				}
			}
		}
	}
}

// dispatch routes a validated ClientMessage to the appropriate broker operation.
func (c *Connection) dispatch(cm ClientMessage) {
	switch cm.Type {
	case "ping":
		c.enqueueOrClose(PongMessage(cm.RequestID))

	case "subscribe":
		if cm.ClientID == "" {
			c.enqueueOrClose(ErrorMessage(cm.RequestID, "BAD_REQUEST", "missing client_id"))
			return
		}
		if cm.Topic == "" {
			c.enqueueOrClose(ErrorMessage(cm.RequestID, "BAD_REQUEST", "missing topic"))
			return
		}
		history, err := c.broker.Subscribe(cm.Topic, cm.ClientID, c, cm.LastN)
		if err != nil {
			c.enqueueOrClose(ErrorMessage(cm.RequestID, "TOPIC_NOT_FOUND", err.Error()))
			return
		}
		c.enqueueOrClose(AckMessage(cm.RequestID, cm.Topic))
		for i := range history {
			c.enqueueOrClose(DeliveryMessage(cm.Topic, &history[i]))
		}

	case "unsubscribe":
		if cm.Topic == "" {
			c.enqueueOrClose(ErrorMessage(cm.RequestID, "BAD_REQUEST", "missing topic"))
			return
		}
		// Idempotent: ignore TOPIC_NOT_FOUND
		_ = c.broker.Unsubscribe(cm.Topic, c)
		c.enqueueOrClose(AckMessage(cm.RequestID, cm.Topic))

	case "publish":
		if cm.Topic == "" {
			c.enqueueOrClose(ErrorMessage(cm.RequestID, "BAD_REQUEST", "missing topic"))
			return
		}
		if cm.Message == nil {
			c.enqueueOrClose(ErrorMessage(cm.RequestID, "BAD_REQUEST", "missing message"))
			return
		}
		if !uuidRE.MatchString(cm.Message.ID) {
			c.enqueueOrClose(ErrorMessage(cm.RequestID, "BAD_REQUEST", "message.id must be a valid UUID"))
			return
		}
		if err := c.broker.Publish(cm.Topic, *cm.Message); err != nil {
			c.enqueueOrClose(ErrorMessage(cm.RequestID, "TOPIC_NOT_FOUND", err.Error()))
			return
		}
		c.enqueueOrClose(AckMessage(cm.RequestID, cm.Topic))

	default:
		c.enqueueOrClose(ErrorMessage(cm.RequestID, "BAD_REQUEST", "unknown message type"))
	}
}

// enqueueOrClose sends msg to the appropriate channel — the data channel for
// topic-message deliveries (history replay), the control channel for
// everything else — blocking only until the connection is torn down.
func (c *Connection) enqueueOrClose(msg ServerMessage) {
	ch := c.control
	if msg.Type == "message" {
		ch = c.send
	}
	select {
	case ch <- msg:
	case <-c.done:
	}
}
