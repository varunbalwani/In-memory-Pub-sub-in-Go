package broker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"pubsub-system/internal/config"
)

// Sentinel errors.
var (
	ErrTopicExists   = errors.New("topic already exists")
	ErrTopicNotFound = errors.New("topic not found")
)

// Message is a unit of data published to a topic.
type Message struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

// ServerMessage is sent from the broker to a client.
type ServerMessage struct {
	Type      string        `json:"type"`
	RequestID string        `json:"request_id,omitempty"`
	Topic     string        `json:"topic,omitempty"`
	Message   *Message      `json:"message,omitempty"`
	Status    string        `json:"status,omitempty"`
	Error     *ErrorPayload `json:"error,omitempty"`
	Msg       string        `json:"msg,omitempty"`
	Ts        time.Time     `json:"ts"`
}

// ErrorPayload carries a machine-readable error code and human message.
type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// TopicInfo is returned by GetTopics.
type TopicInfo struct {
	Name            string `json:"name"`
	SubscriberCount int    `json:"subscriber_count"`
}

// TopicStats is per-topic data returned by Stats.
type TopicStats struct {
	Name            string `json:"name"`
	MsgCount        int64  `json:"msg_count"`
	SubscriberCount int    `json:"subscriber_count"`
}

// StatsSnapshot is the payload for GET /stats.
type StatsSnapshot struct {
	Topics []TopicStats `json:"topics"`
}

// HealthSnapshot is the payload for GET /health.
type HealthSnapshot struct {
	UptimeSec   int64 `json:"uptime_sec"`
	Topics      int   `json:"topics"`
	Subscribers int   `json:"subscribers"`
}

// Broker is the central component managing topics, subscribers, and message routing.
type Broker struct {
	mu          sync.RWMutex
	topics      map[string]*Topic
	connections sync.Map // map[Connection]struct{}
	startTime   time.Time
	cfg         *config.Config
	shutdownCh  chan struct{}
}

// NewBroker creates and returns a new Broker. Call StartHeartbeat separately.
func NewBroker(cfg *config.Config) *Broker {
	return &Broker{
		topics:     make(map[string]*Topic),
		startTime:  time.Now(),
		cfg:        cfg,
		shutdownCh: make(chan struct{}),
	}
}

// RegisterConnection adds a connection to the broker's connection set.
func (b *Broker) RegisterConnection(conn Connection) {
	b.connections.Store(conn, struct{}{})
}

// DeregisterConnection removes a connection from the broker's connection set.
func (b *Broker) DeregisterConnection(conn Connection) {
	b.connections.Delete(conn)
}

// CreateTopic creates a new topic. Returns ErrTopicExists if it already exists.
func (b *Broker) CreateTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[name]; ok {
		return ErrTopicExists
	}
	b.topics[name] = newTopic(name, b.cfg.HistorySize)
	return nil
}

// DeleteTopic deletes a topic, notifying all subscribers with a topic_deleted info message.
// Returns ErrTopicNotFound if the topic does not exist.
func (b *Broker) DeleteTopic(name string) error {
	b.mu.Lock()
	topic, ok := b.topics[name]
	if !ok {
		b.mu.Unlock()
		return ErrTopicNotFound
	}
	delete(b.topics, name)
	b.mu.Unlock()

	// Notify subscribers outside the broker lock.
	info := ServerMessage{
		Type:  "info",
		Topic: name,
		Msg:   "topic_deleted",
		Ts:    time.Now(),
	}
	topic.mu.RLock()
	for _, sub := range topic.subscribers {
		select {
		case sub.send <- info:
		default:
		}
	}
	topic.mu.RUnlock()
	return nil
}

// GetTopics returns a snapshot of all topics and their subscriber counts.
func (b *Broker) GetTopics() []TopicInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]TopicInfo, 0, len(b.topics))
	for name, t := range b.topics {
		out = append(out, TopicInfo{Name: name, SubscriberCount: t.subscriberCount()})
	}
	return out
}

// Publish fans out msg to all subscribers of topicName.
func (b *Broker) Publish(topicName string, msg Message) error {
	b.mu.RLock()
	topic, ok := b.topics[topicName]
	b.mu.RUnlock()
	if !ok {
		return ErrTopicNotFound
	}

	policy := BackpressurePolicy(b.cfg.BackpressurePolicy)

	topic.mu.Lock()
	topic.publish(msg, policy, b.cfg.QueueSize)
	topic.mu.Unlock()
	return nil
}

// Subscribe registers conn as a subscriber of topicName.
// If lastN > 0, the most recent lastN messages are replayed immediately after the ack.
func (b *Broker) Subscribe(topicName, clientID string, conn Connection, lastN int) ([]Message, error) {
	b.mu.RLock()
	topic, ok := b.topics[topicName]
	b.mu.RUnlock()
	if !ok {
		return nil, ErrTopicNotFound
	}

	sub := &Subscriber{
		clientID: clientID,
		conn:     conn,
		send:     conn.SendCh(),
	}

	topic.addSubscriber(sub)

	var history []Message
	if lastN > 0 {
		history = topic.lastN(lastN)
	}
	return history, nil
}

// Unsubscribe removes conn from topicName's subscriber list.
func (b *Broker) Unsubscribe(topicName string, conn Connection) error {
	b.mu.RLock()
	topic, ok := b.topics[topicName]
	b.mu.RUnlock()
	if !ok {
		return ErrTopicNotFound
	}
	topic.removeSubscriber(conn)
	return nil
}

// UnsubscribeAll removes conn from every topic. Safe to call multiple times (idempotent).
func (b *Broker) UnsubscribeAll(conn Connection) {
	b.mu.RLock()
	topics := make([]*Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	b.mu.RUnlock()

	for _, t := range topics {
		t.removeSubscriber(conn)
	}
}

// Stats returns a snapshot of per-topic message and subscriber counts.
func (b *Broker) Stats() StatsSnapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()
	stats := make([]TopicStats, 0, len(b.topics))
	for name, t := range b.topics {
		stats = append(stats, TopicStats{
			Name:            name,
			MsgCount:        t.msgCount,
			SubscriberCount: t.subscriberCount(),
		})
	}
	return StatsSnapshot{Topics: stats}
}

// Health returns a snapshot of overall broker health.
func (b *Broker) Health() HealthSnapshot {
	b.mu.RLock()
	topicCount := len(b.topics)
	totalSubs := 0
	for _, t := range b.topics {
		totalSubs += t.subscriberCount()
	}
	b.mu.RUnlock()

	return HealthSnapshot{
		UptimeSec:   int64(time.Since(b.startTime).Seconds()),
		Topics:      topicCount,
		Subscribers: totalSubs,
	}
}

// StartHeartbeat starts the heartbeat goroutine. Call once after NewBroker.
func (b *Broker) StartHeartbeat() {
	go b.heartbeatLoop()
}

func (b *Broker) heartbeatLoop() {
	ticker := time.NewTicker(b.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.broadcastInfo("ping")
		case <-b.shutdownCh:
			return
		}
	}
}

func (b *Broker) broadcastInfo(msg string) {
	info := ServerMessage{
		Type: "info",
		Msg:  msg,
		Ts:   time.Now(),
	}
	b.connections.Range(func(key, _ any) bool {
		conn := key.(Connection)
		conn.Enqueue(info)
		return true
	})
}

// Shutdown gracefully closes all connections and stops background goroutines.
func (b *Broker) Shutdown(ctx context.Context) {
	close(b.shutdownCh)

	// Signal all connections to close.
	b.connections.Range(func(key, _ any) bool {
		conn := key.(Connection)
		conn.Close()
		return true
	})
}
