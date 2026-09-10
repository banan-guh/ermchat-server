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
	go hub.SaveLoop()

	upstream := NewTwitchUpstream(hub)
	hub.limiter = NewRateLimiter(upstream.send)
	hub.upstream = upstream
	upstream.dialTwitch()

	cached := LoadChannels(hub.jsonpath)
	for ch, ts := range cached { // for ch, ts in cache:
		hub.lastseen[ch] = ts // if fails to load, it doesn't nuke the json
	}
	for _, ch := range FilterStale(cached) { // for everything in cache:
		hub.limiter.Enqueue("JOIN " + ch + "\r\n")
	}

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})
	log.Println("listening on :8080")
	log.Println("join this: ws://localhost:8080/ws")
	log.Fatal(http.ListenAndServe(":8080", nil))
}