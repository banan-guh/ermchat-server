package main

import (
	"log"
	"net/http"
	"time"
)

func main() {
	hub := NewHub()
	go hub.Run()

	go func() {
		for {
			time.Sleep(5 * time.Second)
			hub.broadcast <- Message{Channel: "#test", Data: []byte("...PRIVMSG #test :hello\r\n")}
		}
	}()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})
	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}