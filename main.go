package main

import (
	"log"
	"net/http"
	//"time"
)

func main() {
	hub := NewHub()
	go hub.Run()

	upstream := NewTwitchUpstream(hub)
	upstream.dialTwitch()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})
	log.Println("listening on :8080")
	log.Println("join this: ws://localhost:8080/ws")
	log.Fatal(http.ListenAndServe(":8080", nil))
}