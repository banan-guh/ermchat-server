package main

// twitch file

import (
	"strings"
	"log"
	//"net/http"
	//"net/url"
	"github.com/gorilla/websocket"
)

type TwitchUpstream struct {
	conn *websocket.Conn
	send chan string
	hub *Hub
}

var dialer = websocket.Dialer{
	ReadBufferSize: 4096,
	WriteBufferSize: 4096,
}

// helper
func hasPrefixes(line string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

func (upstream *TwitchUpstream) readPump() {
	defer func() {
		log.Println("error in upstream readPump!")
		return
	}()
	for {
		_, msg, err := upstream.conn.ReadMessage()
		if err != nil {
			return
		}
		line := string(msg)
		log.Println(line)
		switch {
		case strings.HasPrefix(line, "PING"):
			upstream.send <- "PONG :tmi.twitch.tv\r\n"
		case hasPrefixes(line, []string{
			"PRIVMSG", 
			"USERNOTICE", 
			"CLEARCHAT", 
			"CLEARMSG", 
			"ROOMSTATE", 
			"NOTICE"}):
			upstream.hub.broadcast <- Message{Channel: "#test", Data: msg} // change to real channel later
		}
	}
}

func (upstream *TwitchUpstream) writePump() {
	for {
		msg := <-upstream.send
		if err := upstream.conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
			return
		}
	}
}

func (upstream *TwitchUpstream) dialTwitch() {
	url := "wss://irc-ws.chat.twitch.tv:443"
	conn, _, err := dialer.Dial(url, nil)
	if err != nil { return }
	if err := conn.WriteMessage(websocket.TextMessage, []byte("PASS pass\r\n")); err != nil { return }
	if err := conn.WriteMessage(websocket.TextMessage, []byte("NICK justinfan16845\r\n")); err != nil { return }
	if err := conn.WriteMessage(websocket.TextMessage, []byte("CAP REQ :twitch.tv/tags twitch.tv/commands\r\n")); err != nil { return }
	upstream.conn = conn
	go upstream.writePump()
	go upstream.readPump()
}



func NewTwitchUpstream(hub *Hub) *TwitchUpstream {
	return &TwitchUpstream{
		send: make(chan string, 256),
		hub:  hub,
		// conn is nil until written to by dialTwitch()
	}
}