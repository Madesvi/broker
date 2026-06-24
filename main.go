package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
)

func putMessageToBroker(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	message := r.FormValue("v")

	slog.Info("Path", "value", queue)

	if message == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func getMessageFromBroker(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queue")

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(queueName))

}

func main() {
	defaultPort := "3000"
	portFlag := flag.String("port", defaultPort, "Port to run server")
	flag.Parse()

	port := ":" + *portFlag

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /{queue}", putMessageToBroker)
	mux.HandleFunc("GET /{queue}", getMessageFromBroker)

	fmt.Printf("Server starting on %s\n", port)
	err := http.ListenAndServe(port, mux)
	if err != nil {
		slog.Error("starting server", "err", err)
	}
}
