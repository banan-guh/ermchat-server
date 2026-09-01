package main

import (
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

// connect to clients and serve what upstream picks up

type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
	channels map[string]bool
}

var upgrader = websocket.Upgrader{
	ReadBufferSize: 4096,
	WriteBufferSize: 4096,
}

// func handler(w http.ResponseWriter, r *http.Request) {
// 	conn, err := upgrader.Upgrade(w, r, nil)
// 	if err != nil {
// 		log.Println(err)
// 		return
// 	}
// 	for {
// 		messageType, p, err := conn.ReadMessage()
// 		if err != nil {
// 			log.Println(err)
// 			return
// 		}
// 		line := string(p)
// 		if !strings.HasPrefix(line, "PING") {
			
// 		}
// 		if err := conn.WriteMessage(messageType, p); err != nil {
// 			log.Println(err)
// 			return
// 		}
// 	}
// }

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		line := string(msg)
		switch {
		case strings.HasPrefix(line, "CAP REQ "):
			req := strings.TrimSpace(strings.TrimPrefix(line, "CAP REQ "))
			c.send <- []byte(":tmi.twitch.tv CAP * ACK " + req + "\r\n")

		case strings.HasPrefix(line, "PASS"):
			// ignore
		case strings.HasPrefix(line, "NICK"):
			nick := strings.TrimSpace(strings.TrimPrefix(line, "NICK "))
			if nick == "" { nick = "justinfan12345"}
			c.sendWelcome(nick)
		case strings.HasPrefix(line, "JOIN "):
			channel := strings.TrimSpace(strings.TrimPrefix(line, "JOIN "))
			c.hub.Join(channel, c)
			c.channels[channel] = true

		case strings.HasPrefix(line, "PART "):
			channel := strings.TrimSpace(strings.TrimPrefix(line, "PART "))
			c.hub.Leave(channel, c)
			delete(c.channels, channel)
		}
	}
}

// header twitch sends (we relay so we need it too)
func (c *Client) sendWelcome(nick string) {
    //nick := "justinfan12345"
    c.send <- []byte(":tmi.twitch.tv 001 " + nick + " :Welcome, GLHF!\r\n")
    c.send <- []byte(":tmi.twitch.tv 002 " + nick + " :Your host is tmi.twitch.tv\r\n")
    c.send <- []byte(":tmi.twitch.tv 003 " + nick + " :This server is rather new\r\n")
    c.send <- []byte(":tmi.twitch.tv 004 " + nick + " :-\r\n")
    c.send <- []byte(":tmi.twitch.tv 375 " + nick + " :-\r\n")
	c.send <- []byte(":tmi.twitch.tv 372 " + nick + " :You are in a maze of twisty passages, all alike.\r\n")
    c.send <- []byte(":tmi.twitch.tv 376 " + nick + " :>\r\n")
}

func (c *Client) writePump() {
	for {
		msg := <-c.send
		if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}
	}
}

func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	client := &Client{
		hub: hub, 
		conn: conn, 
		send: make(chan []byte, 256),
		channels: make(map[string]bool),
	}
	client.hub.register <- client
	go client.writePump()
	go client.readPump()
}