package broker

import (
	"sync"
	"sync/atomic"
)

// Topic holds subscribers and a ring-buffer message history.
type Topic struct {
	name        string
	mu          sync.RWMutex
	subscribers map[Connection]*Subscriber
	history     []Message // ring buffer, capacity = cfg.HistorySize
	historyHead int       // index of the oldest slot (next write position)
	historyLen  int       // number of valid entries (≤ cap(history))
	msgCount    int64     // total messages published (atomic)
}

func newTopic(name string, historySize int) *Topic {
	return &Topic{
		name:        name,
		subscribers: make(map[Connection]*Subscriber),
		history:     make([]Message, 0, historySize),
	}
}

func (t *Topic) addSubscriber(sub *Subscriber) {
	t.mu.Lock()
	t.subscribers[sub.conn] = sub
	t.mu.Unlock()
}

func (t *Topic) removeSubscriber(conn Connection) {
	t.mu.Lock()
	delete(t.subscribers, conn)
	t.mu.Unlock()
}

func (t *Topic) subscriberCount() int {
	t.mu.RLock()
	n := len(t.subscribers)
	t.mu.RUnlock()
	return n
}

// lastN returns the most recent n messages in chronological (oldest-first) order.
func (t *Topic) lastN(n int) []Message {
	t.mu.RLock()
	defer t.mu.RUnlock()

	cap := cap(t.history)
	if cap == 0 || n == 0 {
		return nil
	}

	total := t.historyLen
	if n > total {
		n = total
	}
	if n == 0 {
		return nil
	}

	result := make([]Message, n)
	// oldest of the n messages is at position (historyHead - total + (total - n)) mod cap
	// = (historyHead - n) mod cap
	start := (t.historyHead - n + cap*2) % cap
	for i := 0; i < n; i++ {
		result[i] = t.history[(start+i)%cap]
	}
	return result
}

// publish fans out msg to all subscribers, appends to history, and applies backpressure.
// Must be called with t.mu held (write lock).
func (t *Topic) publish(msg Message, policy BackpressurePolicy) {
	// Append to ring buffer.
	cap := cap(t.history)
	if cap > 0 {
		if t.historyLen < cap {
			t.history = append(t.history, msg)
			t.historyLen++
		} else {
			t.history[t.historyHead] = msg
		}
		t.historyHead = (t.historyHead + 1) % cap
	}

	atomic.AddInt64(&t.msgCount, 1)

	// Build the delivery message once.
	delivery := ServerMessage{
		Type:    "message",
		Topic:   t.name,
		Message: &msg,
	}

	for _, sub := range t.subscribers {
		select {
		case sub.send <- delivery:
			// delivered
		default:
			// queue full — apply backpressure
			switch policy {
			case PolicyDrop:
				// drain oldest, enqueue newest
				select {
				case <-sub.send:
				default:
				}
				select {
				case sub.send <- delivery:
				default:
				}
			case PolicyDisconnect:
				errMsg := ServerMessage{
					Type: "error",
					Error: &ErrorPayload{
						Code:    "SLOW_CONSUMER",
						Message: "subscriber queue overflow",
					},
				}
				// best-effort send of error, then close
				select {
				case sub.send <- errMsg:
				default:
				}
				sub.conn.Close()
			}
		}
	}
}
