package main

import (
	"container/list"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Queue struct {
	mu       sync.Mutex
	messages *list.List
	waiters  *list.List
}

type Broker struct {
	mu     sync.RWMutex
	queues map[string]*Queue
}

func NewBroker() *Broker {
	return &Broker{queues: make(map[string]*Queue)}
}

func (b *Broker) getOrCreateQueue(name string) *Queue {
	b.mu.RLock()
	q, ok := b.queues[name] // check exists channel
	b.mu.RUnlock()
	if ok {
		return q
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// double check Lock
	q, ok = b.queues[name]
	if ok {
		return q
	}

	q = &Queue{
		messages: list.New(),
		waiters:  list.New(),
	}
	b.queues[name] = q
	return q
}

// slice make garbage [1:] :)

func main() {
	defaultPort := "3000"
	portFlag := flag.String("port", defaultPort, "Port to run server")
	flag.Parse()

	port := ":" + *portFlag

	broker := NewBroker()

	mux := http.NewServeMux()

	mux.HandleFunc("PUT /{queue}", func(w http.ResponseWriter, r *http.Request) {
		queueName := r.PathValue("queue")
		message := r.FormValue("v")

		if message == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		q := broker.getOrCreateQueue(queueName)

		q.mu.Lock()

		if q.waiters.Len() > 0 {
			frontElement := q.waiters.Front()
			ch := frontElement.Value.(chan string)
			q.waiters.Remove(frontElement)
			q.mu.Unlock()

			ch <- message
			w.WriteHeader(http.StatusOK)
			return
		}
		if q.messages.Len() >= 10000 {
			q.mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		q.messages.PushBack(message)
		q.mu.Unlock()
	})
	mux.HandleFunc("GET /{queue}", func(w http.ResponseWriter, r *http.Request) {
		queueName := r.PathValue("queue")
		timeoutValue := r.FormValue("timeout")

		if timeoutValue == "" {
			timeoutValue = "0"
		}

		timeout, err := strconv.Atoi(timeoutValue)
		if err != nil {
			http.Error(w, "invalid timeout", http.StatusBadRequest)
			return
		}

		q := broker.getOrCreateQueue(queueName)

		q.mu.Lock()

		if q.messages.Len() > 0 {
			frontElement := q.messages.Front()
			message := frontElement.Value.(string)
			q.messages.Remove(frontElement)
			q.mu.Unlock()

			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(message))
			return
		}

		if timeout <= 0 {
			q.mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
			return
		}

		ch := make(chan string, 1)
		elem := q.waiters.PushBack(ch)
		q.mu.Unlock()

		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeout)*time.Second)
		defer cancel()

		select {
		case message := <-ch:
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(message))
		case <-ctx.Done():
			q.mu.Lock()
			q.waiters.Remove(elem)
			q.mu.Unlock()

			w.WriteHeader(http.StatusNotFound)
		}

	})

	fmt.Printf("Server starting on %s\n", port)
	err := http.ListenAndServe(port, mux)
	if err != nil {
		slog.Error("starting server", "err", err)
	}
}

// test terminal
// curl -i -X PUT "http://localhost:7777/pet?v=snake"
// curl -i -X PUT "http://localhost:7777/pet?v=dog"
// curl -i -X PUT "http://localhost:7777/role?v=admin"
// curl -i -X PUT "http://localhost:7777/role?v=manager"
// curl -i -X GET "http://localhost:7777/pet"
// curl -i -X GET "http://localhost:7777/role"
