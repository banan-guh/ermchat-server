package main

// TODO: future things to do:
/*
- shrink tags (diff, send hashes), data saver
- pings (later)
*/

import (
	"log"
	"net/http"
	//"time"
)

func main() {
	hub := NewHub()
	go hub.Run()

	upstream := NewTwitchUpstream(hub)
	hub.upstream = upstream
	upstream.dialTwitch()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})
	log.Println("listening on :8080")
	log.Println("join this: ws://localhost:8080/ws")
	log.Fatal(http.ListenAndServe(":8080", nil))
}