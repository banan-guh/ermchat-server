package main

// twitch file

import (
	"strings"
	"log"
	//"net/http"
	//"net/url"
	"math/rand"
	"time"

	"github.com/gorilla/websocket"
)

type TwitchUpstream struct {
	send chan string
	hub *Hub
}

var dialer = websocket.Dialer{
	ReadBufferSize: 4096,
	WriteBufferSize: 4096,
}

// another helper (nuh uh nesting :{} )
// splits, finds the one with # start and uses it
func getChannelFromIRC(line string) string {
	for _, field := range strings.Fields(line) {
		if strings.HasPrefix(field, "#") {
			return field
		}
	}
	return ""
}

// another another helper (parse IRC to find cmd)
func getCommand(line string) string {
    // strip tags
    if strings.HasPrefix(line, "@") {
        if i := strings.IndexByte(line, ' '); i != -1 {
            line = line[i+1:]
        }
    }
    // strip sender
    if strings.HasPrefix(line, ":") {
        if i := strings.IndexByte(line, ' '); i != -1 {
            line = line[i+1:]
        }
    }
    if i := strings.IndexByte(line, ' '); i != -1 {
        return line[:i]
    }
    return line
}

func (upstream *TwitchUpstream) readPump(conn *websocket.Conn, dead chan bool) {
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Println("error in upstream readPump!")
			dead <- true // channel to report "network's dead!"
			return
		}
		for _, line := range strings.Split(string(msg), "\r\n") {
			if line == "" {
				continue
			}
			//log.Println(line) // rm for spam
			cmd := getCommand(line)
			switch cmd {
			case "PING":
				upstream.send <- "PONG :tmi.twitch.tv\r\n"
			case "PRIVMSG", "USERNOTICE", "CLEARCHAT", "CLEARMSG", "ROOMSTATE", "NOTICE":
				upstream.hub.broadcast <- Message{Channel: getChannelFromIRC(line), Data: []byte(line + "\r\n"), RoomState: cmd == "ROOMSTATE"}
			case "USERSTATE", "GLOBALUSERSTATE":
				upstream.hub.broadcast <- Message{Channel: getChannelFromIRC(line), Data: []byte(line + "\r\n")}
			default:
				// no action
			}
		}
	}
}

func (upstream *TwitchUpstream) writePump(conn *websocket.Conn, dead chan bool) {
	for {
		msg := <-upstream.send
		if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
			dead <- true
			return
		}
	}
}

func (upstream *TwitchUpstream) dialTwitch(dead chan bool) (*websocket.Conn, bool) {
	url := "wss://irc-ws.chat.twitch.tv:443"
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		log.Println("upstream dial:", err)
		return nil, false
	}
	
	nick := upstream.hub.userNick
	pass := upstream.hub.userToken
	if nick == "" || pass == "" {
		conn.Close()
		log.Println("upstream: missing creds")
		return nil, false
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("PASS oauth:"+pass+"\r\n")); err != nil {
		conn.Close()
		log.Println("upstream PASS:", err)
		return nil, false
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("NICK "+nick+"\r\n")); err != nil {
		conn.Close()
		log.Println("upstream NICK:", err)
		return nil, false
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("CAP REQ :twitch.tv/tags twitch.tv/commands\r\n")); err != nil {
		conn.Close()
		log.Println("upstream CAP:", err)
		return nil, false
	}
	go upstream.writePump(conn, dead)
	go upstream.readPump(conn, dead)
	return conn, true
}

func (upstream *TwitchUpstream) maintainUpstream() {
	backoff := time.Second
	var cur *websocket.Conn
	for {
		dead := make(chan bool, 2)
		if conn, ok := upstream.dialTwitch(dead); ok {
			backoff = time.Second
			if cur != nil {
				cur.Close()
			}
			cur = conn
			upstream.rejoinAll()
			<-dead
		} else {
			// duration in nanoseconds??? weird ass go,
			// int64 to format for rand func
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			time.Sleep(backoff + jitter)
			backoff = min(backoff*2, time.Minute)
		}
	}
}

func (upstream *TwitchUpstream) rejoinAll() {
	upstream.hub.mu.Lock()
	defer upstream.hub.mu.Unlock()
	for ch := range upstream.hub.upstreamJoined {
		upstream.hub.limiter.Enqueue("JOIN " + ch + "\r\n")
	}
}

func NewTwitchUpstream(hub *Hub) *TwitchUpstream {
	return &TwitchUpstream{
		send: make(chan string, 256),
		hub:  hub,
	}
}