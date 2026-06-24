package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
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

	ch = make(chan string, 100) // for example 100
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
		ch <- message

		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /{queue}", func(w http.ResponseWriter, r *http.Request) {
		queueName := r.PathValue("queue")

		ch := broker.getCreateQueue(queueName)

		message := <-ch

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(message))

	})

	fmt.Printf("Server starting on %s\n", port)
	err := http.ListenAndServe(port, mux)
	if err != nil {
		slog.Error("starting server", "err", err)
	}
}
