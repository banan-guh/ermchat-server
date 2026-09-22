package main

import (
	"log"
	"net/http"
	"strings"
	"time"

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
		switch { // future: add ping to app (from server to app), currently none
		case strings.HasPrefix(line, "PING"):
			token := strings.TrimSpace(strings.TrimPrefix(line, "PING"))
			c.send <- []byte("PONG " + token + "\r\n")
		
		// pass, nick moved to serveWs, cap removed here because it's not supposed to be
		case strings.HasPrefix(line, "JOIN "):
			list := strings.TrimSpace(strings.TrimPrefix(line, "JOIN "))
			for _, channel := range strings.Split(list, ",") {
				channel = strings.TrimSpace(channel)
				if channel == "" {
					continue
				}
				c.hub.Join(channel, c)
				c.channels[channel] = true
			}

		case strings.HasPrefix(line, "PART "):
			channel := strings.TrimSpace(strings.TrimPrefix(line, "PART "))
			c.hub.Leave(channel, c)
			delete(c.channels, channel)
		}
	}
}

// header twitch sends (we relay so we need it too) <- not actually, ngl we could remove
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

func serveWs(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	// if can't get nick in <10s, or if pass is wrong, close connection
	// (different from official twitch)
	nick := ""
	token := ""
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	for nick == "" || token == "" {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("limbo read err: %v", err)
			conn.Close()
			return
		}
		line := string(msg)
		log.Printf("limbo got: %q", line)
		switch {
		case strings.HasPrefix(line, "NICK"): // real creds, need to parse (req nick AND pass)
			n := strings.TrimSpace(strings.TrimPrefix(line, "NICK "))
			if strings.HasPrefix(n, "justinfan") { // we don't support justinfan here
				conn.Close()
				return
			}
			if n != "" {
				nick = n
			}
		case strings.HasPrefix(line, "PASS"): // pass (token)
			t := strings.TrimSpace(strings.TrimPrefix(line, "PASS "))
			t = strings.TrimPrefix(t, "oauth:")
			if t != "" {
				token = t
			}
		case strings.HasPrefix(line, "CAP REQ "):
			// hardcoded in upstream, I'm not making extra compat
			// for what I don't need (purely for app, not anyone else)
			// flow: cap req ALWAYS FIRST! then, nick and pass.
			// otherwise cap req not seen by readpump
			req := strings.TrimSpace(strings.TrimPrefix(line, "CAP REQ "))
			conn.WriteMessage(websocket.TextMessage, []byte(":tmi.twitch.tv CAP * ACK "+req+"\r\n"))
		}
	}
	conn.SetReadDeadline(time.Time{})
	target := routeHub(nick, token)
	client := &Client{
		hub: target,
		conn: conn,
		send: make(chan []byte, 256),
		channels: make(map[string]bool),
	}
	target.register <- client
	go client.writePump()
	go client.readPump()

	client.sendWelcome(nick)
}