package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Broker struct {
	mu     sync.RWMutex
	queues map[string]chan string
}

func NewBroker() *Broker {
	return &Broker{queues: make(map[string]chan string)}
}

func (b *Broker) getCreateQueue(name string) chan string {
	b.mu.RLock()
	ch, ok := b.queues[name] // check exists channel
	b.mu.RUnlock()
	if ok {
		return ch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	ch = make(chan string, 10) // for example 10
	b.queues[name] = ch
	return ch
}

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

		ch := broker.getCreateQueue(queueName)

		select {
		case ch <- message:
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "queue is full", http.StatusServiceUnavailable)
		}
	})
	mux.HandleFunc("GET /{queue}", func(w http.ResponseWriter, r *http.Request) {
		queueName := r.PathValue("queue")
		timeoutValue := r.FormValue("timeout")

		ch := broker.getCreateQueue(queueName)

		if timeoutValue == "" {
			select {
			case message := <-ch:
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(message))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}

		timeout, err := strconv.Atoi(timeoutValue)
		if err != nil {
			slog.Error("convert", "err", err)
			http.Error(w, "invalid timeout", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeout)*time.Second)
		defer cancel()

		select {
		case message := <-ch:
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(message))
		case <-ctx.Done():
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
