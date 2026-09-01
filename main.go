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
			hub.broadcast <- []byte("@badge-info=;badges=;color=#FF0000;display-name=TestBot;... :testbot!testbot@testbot.tmi.twitch.tv PRIVMSG #test :hello from dummy\r\n")
		}
	}()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})
	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}