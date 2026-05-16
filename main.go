package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Task struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type Response struct {
	Status    string `json:"status"`
	Message   string `json:"message,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(Response{Status: "ok", Timestamp: time.Now().Unix()})
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"ping": "pong"})
}

func dispatchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var task Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Status: "error", Message: err.Error()})
		return
	}
	resp := Response{Status: "queued", Message: task.Type, Timestamp: time.Now().Unix()}
	json.NewEncoder(w).Encode(resp)
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade:", err)
		return
	}
	defer conn.Close()
	for {
		mt, msg, err := conn.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			return
		}
		if err := conn.WriteMessage(mt, msg); err != nil {
			log.Println("write:", err)
			return
		}
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/ping", pingHandler)
	mux.HandleFunc("/dispatch", dispatchHandler)
	mux.HandleFunc("/ws", wsHandler)
	
	log.Printf("relay-go listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
