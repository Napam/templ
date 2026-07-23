package sse

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestSendDoesNotPanicOnDisconnect(t *testing.T) {
	for round := range 200 {
		h := New()
		srv := httptest.NewServer(h)

		// Client connects and reads one event.
		resp, err := http.Get(srv.URL)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}

		// Flood reload events from multiple goroutines (like the three
		// notify sources in a real dev session).
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						h.Send("message", "reload")
					}
				}
			}()
		}

		// Read one event (like the browser receiving "reload") then
		// disconnect immediately (like onbeforeunload closing EventSource).
		buf := make([]byte, 64)
		_, _ = resp.Body.Read(buf)
		_ = resp.Body.Close()

		time.Sleep(2 * time.Millisecond)
		close(stop)
		wg.Wait()
		srv.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
	}
}
