package broker

import (
	"fmt"
	"testing"

	"pgregory.net/rapid"

	"pubsub-system/internal/config"
)

// mockConn is a minimal Connection implementation for testing.
type mockConn struct {
	ch chan ServerMessage
}

func newMockConn(bufSize int) *mockConn {
	return &mockConn{ch: make(chan ServerMessage, bufSize)}
}

func (m *mockConn) SendCh() chan ServerMessage { return m.ch }
func (m *mockConn) Enqueue(msg ServerMessage) bool {
	select {
	case m.ch <- msg:
		return true
	default:
		return false
	}
}
func (m *mockConn) Close() {}

func defaultConfig() *config.Config {
	return &config.Config{
		QueueSize:          256,
		HistorySize:        100,
		BackpressurePolicy: "drop",
	}
}

// Feature: pubsub-system, Property 1: Fan-out completeness
// For any topic with N subscribers and any message, every subscriber's send channel
// contains that message exactly once after publish.
// Validates: Requirements 4.1, 4.5
func TestFanOutCompleteness(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		numSubscribers := rapid.IntRange(1, 20).Draw(t, "numSubscribers")
		msgID := fmt.Sprintf("%08d-0000-0000-0000-000000000000", rapid.IntRange(0, 99999999).Draw(t, "msgIDNum"))
		payload := rapid.SliceOf(rapid.Byte()).Draw(t, "payload")

		cfg := defaultConfig()
		b := NewBroker(cfg)
		if err := b.CreateTopic("test-topic"); err != nil {
			t.Fatal(err)
		}

		// Subscribe N clients.
		conns := make([]*mockConn, numSubscribers)
		for i := 0; i < numSubscribers; i++ {
			conns[i] = newMockConn(cfg.QueueSize)
			if _, err := b.Subscribe("test-topic", fmt.Sprintf("client-%d", i), conns[i], 0); err != nil {
				t.Fatal(err)
			}
		}

		msg := Message{ID: msgID, Payload: payload}
		if err := b.Publish("test-topic", msg); err != nil {
			t.Fatal(err)
		}

		// Every subscriber must have received the message exactly once.
		for i, conn := range conns {
			if len(conn.ch) != 1 {
				t.Fatalf("subscriber %d: expected 1 message in channel, got %d", i, len(conn.ch))
			}
			received := <-conn.ch
			if received.Type != "message" {
				t.Fatalf("subscriber %d: expected type 'message', got %q", i, received.Type)
			}
			if received.Message == nil {
				t.Fatalf("subscriber %d: received nil message", i)
			}
			if received.Message.ID != msg.ID {
				t.Fatalf("subscriber %d: expected message ID %q, got %q", i, msg.ID, received.Message.ID)
			}
		}
	})
}

// Feature: pubsub-system, Property 3: Unsubscribe stops delivery
// For any subscriber that unsubscribes, messages published after unsubscribe are
// not delivered to that subscriber.
// Validates: Requirements 3.1
func TestUnsubscribeStopsDelivery(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		msgBeforeID := fmt.Sprintf("%08d-0000-0000-0000-000000000000", rapid.IntRange(0, 49999999).Draw(t, "msgBeforeIDNum"))
		msgAfterID := fmt.Sprintf("%08d-0000-0000-0000-000000000000", rapid.IntRange(50000000, 99999999).Draw(t, "msgAfterIDNum"))

		cfg := defaultConfig()
		b := NewBroker(cfg)
		if err := b.CreateTopic("test-topic"); err != nil {
			t.Fatal(err)
		}

		conn := newMockConn(cfg.QueueSize)
		if _, err := b.Subscribe("test-topic", "client-1", conn, 0); err != nil {
			t.Fatal(err)
		}

		// Publish a message before unsubscribe — should be delivered.
		msgBefore := Message{ID: msgBeforeID, Payload: []byte(`"before"`)}
		if err := b.Publish("test-topic", msgBefore); err != nil {
			t.Fatal(err)
		}

		// Unsubscribe.
		if err := b.Unsubscribe("test-topic", conn); err != nil {
			t.Fatal(err)
		}

		// Drain the channel so we start from a clean state.
		for len(conn.ch) > 0 {
			<-conn.ch
		}

		// Publish a message after unsubscribe — must NOT be delivered.
		msgAfter := Message{ID: msgAfterID, Payload: []byte(`"after"`)}
		if err := b.Publish("test-topic", msgAfter); err != nil {
			t.Fatal(err)
		}

		if len(conn.ch) != 0 {
			t.Fatalf("unsubscribed client received %d message(s) after unsubscribe", len(conn.ch))
		}
	})
}

// Feature: pubsub-system, Property 6: Backpressure — drop policy preserves newest
// For any full queue of capacity Q and new message M, after drop policy the queue
// contains M and not the former head.
// Validates: Requirements 6.2, 6.3
func TestBackpressureDropPolicyPreservesNewest(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		queueSize := rapid.IntRange(1, 16).Draw(t, "queueSize")

		cfg := &config.Config{
			QueueSize:          queueSize,
			HistorySize:        100,
			BackpressurePolicy: "drop",
		}
		b := NewBroker(cfg)
		if err := b.CreateTopic("test-topic"); err != nil {
			t.Fatal(err)
		}

		conn := newMockConn(queueSize)
		if _, err := b.Subscribe("test-topic", "client-1", conn, 0); err != nil {
			t.Fatal(err)
		}

		// Fill the queue completely with messages whose IDs we track.
		firstMsgID := "00000000-0000-0000-0000-000000000001"
		for i := 0; i < queueSize; i++ {
			id := fmt.Sprintf("%08d-0000-0000-0000-000000000000", i+1)
			if i == 0 {
				firstMsgID = id
			}
			msg := Message{ID: id, Payload: []byte(`"fill"`)}
			if err := b.Publish("test-topic", msg); err != nil {
				t.Fatal(err)
			}
		}

		if len(conn.ch) != queueSize {
			t.Fatalf("expected queue full (%d), got %d", queueSize, len(conn.ch))
		}

		// Publish one more message — drop policy should drop the oldest and enqueue this one.
		newMsgID := "99999999-0000-0000-0000-000000000000"
		newMsg := Message{ID: newMsgID, Payload: []byte(`"newest"`)}
		if err := b.Publish("test-topic", newMsg); err != nil {
			t.Fatal(err)
		}

		// Queue should still be at capacity.
		if len(conn.ch) != queueSize {
			t.Fatalf("expected queue size %d after drop, got %d", queueSize, len(conn.ch))
		}

		// Drain all messages and collect IDs.
		received := make([]string, 0, queueSize)
		for len(conn.ch) > 0 {
			m := <-conn.ch
			if m.Message != nil {
				received = append(received, m.Message.ID)
			}
		}

		// The former head (firstMsgID) must NOT be present.
		for _, id := range received {
			if id == firstMsgID {
				t.Fatalf("drop policy failed: oldest message %q still present in queue", firstMsgID)
			}
		}

		// The new message (newMsgID) MUST be present.
		found := false
		for _, id := range received {
			if id == newMsgID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("drop policy failed: newest message %q not found in queue", newMsgID)
		}
	})
}

// Feature: pubsub-system, Property 7: Unsubscribe idempotence
// Calling UnsubscribeAll multiple times produces the same result as calling it once —
// the subscriber list for every topic must not contain that connection.
// Validates: Requirements 3.2, 1.5
func TestUnsubscribeIdempotence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		numTopics := rapid.IntRange(1, 5).Draw(t, "numTopics")
		extraCalls := rapid.IntRange(1, 5).Draw(t, "extraCalls")

		cfg := defaultConfig()
		b := NewBroker(cfg)

		// Create topics and subscribe the same connection to all of them.
		topicNames := make([]string, numTopics)
		for i := 0; i < numTopics; i++ {
			topicNames[i] = fmt.Sprintf("topic-%d", i)
			if err := b.CreateTopic(topicNames[i]); err != nil {
				t.Fatal(err)
			}
		}

		conn := newMockConn(cfg.QueueSize)
		for i, name := range topicNames {
			if _, err := b.Subscribe(name, fmt.Sprintf("client-%d", i), conn, 0); err != nil {
				t.Fatal(err)
			}
		}

		// First UnsubscribeAll — removes conn from all topics.
		b.UnsubscribeAll(conn)

		// Verify conn is gone from every topic after the first call.
		for _, name := range topicNames {
			b.mu.RLock()
			topic := b.topics[name]
			b.mu.RUnlock()
			topic.mu.RLock()
			_, present := topic.subscribers[conn]
			topic.mu.RUnlock()
			if present {
				t.Fatalf("topic %q still contains conn after first UnsubscribeAll", name)
			}
		}

		// Call UnsubscribeAll additional times — must not panic or change state.
		for i := 0; i < extraCalls; i++ {
			b.UnsubscribeAll(conn)
		}

		// Verify conn is still absent from every topic.
		for _, name := range topicNames {
			b.mu.RLock()
			topic := b.topics[name]
			b.mu.RUnlock()
			topic.mu.RLock()
			_, present := topic.subscribers[conn]
			topic.mu.RUnlock()
			if present {
				t.Fatalf("topic %q contains conn after repeated UnsubscribeAll calls", name)
			}
		}
	})
}

// Feature: pubsub-system, Property 2: Topic isolation
// For any two distinct topics A and B, a message published to A never appears in
// a subscriber registered only to B.
// Validates: Requirements 4.6
func TestTopicIsolation(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		msgID := fmt.Sprintf("%08d-0000-0000-0000-000000000000", rapid.IntRange(0, 99999999).Draw(t, "msgIDNum"))
		payload := rapid.SliceOf(rapid.Byte()).Draw(t, "payload")

		cfg := defaultConfig()
		b := NewBroker(cfg)

		if err := b.CreateTopic("topic-a"); err != nil {
			t.Fatal(err)
		}
		if err := b.CreateTopic("topic-b"); err != nil {
			t.Fatal(err)
		}

		// Subscribe one client only to topic-b.
		connB := newMockConn(cfg.QueueSize)
		if _, err := b.Subscribe("topic-b", "client-b", connB, 0); err != nil {
			t.Fatal(err)
		}

		// Publish a message only to topic-a.
		msg := Message{ID: msgID, Payload: payload}
		if err := b.Publish("topic-a", msg); err != nil {
			t.Fatal(err)
		}

		// The topic-b subscriber must not have received anything.
		if len(connB.ch) != 0 {
			t.Fatalf("topic isolation violated: subscriber of topic-b received %d message(s) from topic-a", len(connB.ch))
		}
	})
}

// Feature: pubsub-system, Property 9: Stats consistency
// For any sequence of publish operations, msgCount in stats equals the number of
// successful publish calls to that topic.
// Validates: Requirements 8.2
func TestStatsConsistency(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		numPublishes := rapid.IntRange(1, 50).Draw(t, "numPublishes")

		cfg := defaultConfig()
		b := NewBroker(cfg)
		if err := b.CreateTopic("test-topic"); err != nil {
			t.Fatal(err)
		}

		// Perform numPublishes successful publish calls.
		for i := 0; i < numPublishes; i++ {
			msgID := fmt.Sprintf("%08d-0000-0000-0000-000000000000", i+1)
			msg := Message{ID: msgID, Payload: []byte(`"data"`)}
			if err := b.Publish("test-topic", msg); err != nil {
				t.Fatal(err)
			}
		}

		// Stats must report msgCount == numPublishes.
		stats := b.Stats()
		if len(stats.Topics) != 1 {
			t.Fatalf("expected 1 topic in stats, got %d", len(stats.Topics))
		}
		if stats.Topics[0].MsgCount != int64(numPublishes) {
			t.Fatalf("stats msgCount = %d, want %d", stats.Topics[0].MsgCount, numPublishes)
		}
	})
}

// Feature: pubsub-system, Property 10: Subscriber count accuracy
// For any sequence of subscribe/unsubscribe operations, subscriber count equals
// the number of currently active subscribers.
// Validates: Requirements 7.5
func TestSubscriberCountAccuracy(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		numOps := rapid.IntRange(1, 30).Draw(t, "numOps")

		cfg := defaultConfig()
		b := NewBroker(cfg)
		if err := b.CreateTopic("test-topic"); err != nil {
			t.Fatal(err)
		}

		// Track which conns are currently subscribed.
		conns := make([]*mockConn, numOps)
		for i := range conns {
			conns[i] = newMockConn(cfg.QueueSize)
		}

		active := make(map[int]bool)

		for i := 0; i < numOps; i++ {
			idx := rapid.IntRange(0, numOps-1).Draw(t, fmt.Sprintf("conn_idx_%d", i))
			conn := conns[idx]

			if active[idx] {
				// Unsubscribe
				if err := b.Unsubscribe("test-topic", conn); err != nil {
					t.Fatal(err)
				}
				delete(active, idx)
			} else {
				// Subscribe
				if _, err := b.Subscribe("test-topic", fmt.Sprintf("client-%d", idx), conn, 0); err != nil {
					t.Fatal(err)
				}
				active[idx] = true
			}

			// After each operation, subscriber count must match active set size.
			topics := b.GetTopics()
			if len(topics) != 1 {
				t.Fatalf("expected 1 topic, got %d", len(topics))
			}
			if topics[0].SubscriberCount != len(active) {
				t.Fatalf("op %d: subscriber count = %d, want %d", i, topics[0].SubscriberCount, len(active))
			}
		}
	})
}

// --- Unit Tests (task 2.13) ---

// Test CreateTopic returns ErrTopicExists on duplicate.
// Validates: Requirements 2.2
func TestCreateTopicDuplicate(t *testing.T) {
	b := NewBroker(defaultConfig())
	if err := b.CreateTopic("dup"); err != nil {
		t.Fatalf("first CreateTopic: %v", err)
	}
	if err := b.CreateTopic("dup"); err != ErrTopicExists {
		t.Fatalf("expected ErrTopicExists, got %v", err)
	}
}

// Test DeleteTopic returns ErrTopicNotFound for a missing topic.
// Validates: Requirements 7.4
func TestDeleteTopicNotFound(t *testing.T) {
	b := NewBroker(defaultConfig())
	if err := b.DeleteTopic("ghost"); err != ErrTopicNotFound {
		t.Fatalf("expected ErrTopicNotFound, got %v", err)
	}
}

// Test Publish returns ErrTopicNotFound for a missing topic.
// Validates: Requirements 4.2
func TestPublishTopicNotFound(t *testing.T) {
	b := NewBroker(defaultConfig())
	msg := Message{ID: "00000000-0000-0000-0000-000000000001", Payload: []byte(`"x"`)}
	if err := b.Publish("ghost", msg); err != ErrTopicNotFound {
		t.Fatalf("expected ErrTopicNotFound, got %v", err)
	}
}

// Test Subscribe returns ErrTopicNotFound for a missing topic.
// Validates: Requirements 2.2
func TestSubscribeTopicNotFound(t *testing.T) {
	b := NewBroker(defaultConfig())
	conn := newMockConn(256)
	if _, err := b.Subscribe("ghost", "client-1", conn, 0); err != ErrTopicNotFound {
		t.Fatalf("expected ErrTopicNotFound, got %v", err)
	}
}

// Test Subscribe with last_n = 0 delivers no history.
// Validates: Requirements 2.3
func TestSubscribeLastNZeroNoHistory(t *testing.T) {
	b := NewBroker(defaultConfig())
	if err := b.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}

	// Publish a few messages before subscribing.
	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("%08d-0000-0000-0000-000000000000", i)
		if err := b.Publish("t", Message{ID: id, Payload: []byte(`1`)}); err != nil {
			t.Fatal(err)
		}
	}

	conn := newMockConn(256)
	history, err := b.Subscribe("t", "client-1", conn, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("expected 0 history messages with last_n=0, got %d", len(history))
	}
}

// Test DeleteTopic sends topic_deleted info to all subscribers.
// Validates: Requirements 7.3
func TestDeleteTopicNotifiesSubscribers(t *testing.T) {
	b := NewBroker(defaultConfig())
	if err := b.CreateTopic("t"); err != nil {
		t.Fatal(err)
	}

	conn1 := newMockConn(256)
	conn2 := newMockConn(256)
	if _, err := b.Subscribe("t", "c1", conn1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Subscribe("t", "c2", conn2, 0); err != nil {
		t.Fatal(err)
	}

	if err := b.DeleteTopic("t"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}

	for i, conn := range []*mockConn{conn1, conn2} {
		if len(conn.ch) != 1 {
			t.Fatalf("subscriber %d: expected 1 notification, got %d", i, len(conn.ch))
		}
		msg := <-conn.ch
		if msg.Type != "info" || msg.Msg != "topic_deleted" {
			t.Fatalf("subscriber %d: expected info/topic_deleted, got type=%q msg=%q", i, msg.Type, msg.Msg)
		}
	}
}
