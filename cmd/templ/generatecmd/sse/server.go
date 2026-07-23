package sse

import (
	_ "embed"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type client struct {
	events chan event
	done   chan struct{}
}

func New() *Handler {
	return &Handler{
		m:        new(sync.Mutex),
		requests: map[int64]client{},
	}
}

type Handler struct {
	m        *sync.Mutex
	counter  int64
	requests map[int64]client
}

type event struct {
	Type string
	Data string
}

// Send an event to all connected clients.
func (s *Handler) Send(eventType string, data string) {
	s.m.Lock()
	defer s.m.Unlock()
	for _, c := range s.requests {
		c := c
		go func(c client) {
			select {
			case c.events <- event{Type: eventType, Data: data}:
			case <-c.done:
			}
		}(c)
	}
}

func (s *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	id := atomic.AddInt64(&s.counter, 1)
	events := make(chan event)
	done := make(chan struct{})
	s.m.Lock()
	s.requests[id] = client{events: events, done: done}
	s.m.Unlock()
	defer func() {
		s.m.Lock()
		delete(s.requests, id)
		s.m.Unlock()
		close(done)
	}()

	timer := time.NewTimer(0)
loop:
	for {
		select {
		case <-timer.C:
			if _, err := fmt.Fprintf(w, "event: message\ndata: ping\n\n"); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			timer.Reset(time.Second * 5)
		case e := <-events:
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Type, e.Data); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		case <-r.Context().Done():
			break loop
		}
		w.(http.Flusher).Flush()
	}
}
