package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"pubsub-system/internal/broker"
)

// Handler holds the broker and exposes HTTP handlers.
type Handler struct {
	Broker *broker.Broker
}

// writeJSON encodes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeError writes a consistent {"error": "..."} envelope.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// CreateTopicHandler handles POST /topics.
// Expects body: {"name": "..."}
// Returns 201 on success, 409 if topic already exists.
func (h *Handler) CreateTopicHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Name == "" {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := h.Broker.CreateTopic(body.Name); err != nil {
		if errors.Is(err, broker.ErrTopicExists) {
			writeError(w, http.StatusConflict, "topic already exists")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"status": "created",
		"topic":  body.Name,
	})
}

// DeleteTopicHandler handles DELETE /topics/{name}.
// Returns 200 on success, 404 if topic not found.
func (h *Handler) DeleteTopicHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "missing topic name")
		return
	}

	if err := h.Broker.DeleteTopic(name); err != nil {
		if errors.Is(err, broker.ErrTopicNotFound) {
			writeError(w, http.StatusNotFound, "topic not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "deleted",
		"topic":  name,
	})
}

// ListTopicsHandler handles GET /topics.
// Returns 200 with JSON array of {name, subscriber_count}.
func (h *Handler) ListTopicsHandler(w http.ResponseWriter, r *http.Request) {
	topics := h.Broker.GetTopics()
	writeJSON(w, http.StatusOK, topics)
}

// HealthHandler handles GET /health.
// Returns 200 with {uptime_sec, topics, subscribers}.
func (h *Handler) HealthHandler(w http.ResponseWriter, r *http.Request) {
	health := h.Broker.Health()
	writeJSON(w, http.StatusOK, health)
}

// StatsHandler handles GET /stats.
// Returns 200 with per-topic {name, msg_count, subscriber_count}.
func (h *Handler) StatsHandler(w http.ResponseWriter, r *http.Request) {
	stats := h.Broker.Stats()
	writeJSON(w, http.StatusOK, stats)
}
