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
		for _, line := range strings.Split(string(msg), "\r\n") {
			if line == "" {
				continue
			}
			//log.Println(line)
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
//
	nick := upstream.hub.userNick
	if nick == "" {
		nick = "justinfan16845"
	}
	pass := "PASS pass\r\n"
	if upstream.hub.userToken != "" {
		pass = "PASS oauth:" + upstream.hub.userToken + "\r\n"
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte(pass)); err != nil { return }
	if err := conn.WriteMessage(websocket.TextMessage, []byte("NICK "+nick+"\r\n")); err != nil { return }
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