package main

import (
	"log"
	//"strings"
	"sync"
	"time"
)

type Message struct {
	Channel   string
	Data      []byte
	RoomState bool
}

type Hub struct {
	mu             sync.Mutex
	userNick       string
	userToken      string
	channels       map[string]map[*Client]bool
	lastseen       map[string]int64
	upstreamJoined map[string]bool
	roomstate      map[string][]byte
	broadcast      chan Message
	register       chan *Client
	unregister     chan *Client
	upstream       *TwitchUpstream
	limiter        *RateLimiter
	jsonpath       string
}

func (h *Hub) Run() {
	for {
		select {
		case <-h.register:
			//h.clients[client] = true
			log.Printf("client joined")
		case client := <-h.unregister:
			h.mu.Lock()
			for ch, clients := range h.channels {
				delete(clients, client)
				if len(clients) == 0 {
					delete(h.channels, ch)
				}
			}
			h.mu.Unlock()
			log.Printf("client left")
		case msg := <-h.broadcast:
			h.mu.Lock()
			if msg.RoomState {
				h.roomstate[msg.Channel] = msg.Data
				h.upstreamJoined[msg.Channel] = true
			}
			if msg.Channel == "" {
				seen := make(map[*Client]bool)
				for _, clients := range h.channels {
					for client := range clients {
						if seen[client] {
							continue
						}
						seen[client] = true
						select {
						case client.send <- msg.Data:
						default:
						}
					}
				}
			} else if clients, ok := h.channels[msg.Channel]; ok {
				for client := range clients {
					select {
					case client.send <- msg.Data:
					default:
						// client too slow
						close(client.send)
						delete(clients, client)
					}
				}
			}
			h.mu.Unlock()
		}
	}
}

func (h *Hub) Join(channel string, c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastseen[channel] = time.Now().Unix() // refresh every call
	if h.channels[channel] == nil {
		h.channels[channel] = make(map[*Client]bool)
	}
	h.channels[channel][c] = true
	if h.upstreamJoined[channel] {
		// if on twitch, don't rejoin
		if cached, ok := h.roomstate[channel]; ok {
			select {
			case c.send <- cached:
			default:
			}
			return
		}
	}
	if h.upstream != nil {
		h.upstreamJoined[channel] = true
		h.limiter.Enqueue("JOIN " + channel + "\r\n")
	}
}

func (h *Hub) Leave(channel string, c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if clients, ok := h.channels[channel]; ok {
		delete(clients, c)
		if len(clients) == 0 {
			delete(h.channels, channel) // no one watching, clean up
			delete(h.upstreamJoined, channel)
			delete(h.roomstate, channel)
			if h.upstream != nil {
				h.upstream.send <- "PART " + channel + "\r\n"
			}
		}
	}
}

func (h *Hub) SaveLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		h.mu.Lock()
		snapshot := make(map[string]int64, len(h.lastseen))
		for ch, ts := range h.lastseen {
			snapshot[ch] = ts
		}
		h.mu.Unlock()
		SaveChannels(snapshot, h.jsonpath)
	}
}

func (h *Hub) GC() {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		h.mu.Lock()
		_, stale := FilterStale(h.lastseen)
		var removed []string // stale is candidates, removed is actual
		for _, ch := range stale {
			if len(h.channels[ch]) > 0 {
				h.lastseen[ch] = time.Now().Unix()
				continue
			}
			delete(h.lastseen, ch)
			delete(h.channels, ch)
			delete(h.upstreamJoined, ch)
			delete(h.roomstate, ch)
			removed = append(removed, ch)
		}
		snapshot := make(map[string]int64, len(h.lastseen))
		for ch, ts := range h.lastseen {
			snapshot[ch] = ts
		}
		h.mu.Unlock()
		for _, ch := range removed {
			if h.upstream != nil {
				h.limiter.Enqueue("PART " + ch + "\r\n")
			}
		}
		SaveChannels(snapshot, h.jsonpath)
		log.Printf("GC: removed %d stale channels", len(removed))
	}
}

type HubRegistry struct {
	mu   sync.Mutex
	hubs map[string]*Hub
}

func NewHubRegistry() *HubRegistry {
	return &HubRegistry{hubs: make(map[string]*Hub)}
}

var userHubs = NewHubRegistry()

func routeHub(nick, token string) *Hub {
	userHubs.mu.Lock()
	defer userHubs.mu.Unlock()
	h, ok := userHubs.hubs[token]
	if ok {
		return h
	}
	// else, make a new hub
	h = NewHub()
	h.jsonpath = "users/" + nick + ".json"
	h.userNick = nick
	h.userToken = token
	upstream := NewTwitchUpstream(h)
	h.limiter = NewRateLimiter(upstream.send)
	h.upstream = upstream
	saved := LoadChannels(h.jsonpath)
	for ch, ts := range saved {
		h.lastseen[ch] = ts
	}
	go h.Run()
	go h.SaveLoop()
	go h.GC()
	go upstream.maintainUpstream()
	userHubs.hubs[token] = h
	return h
}

func NewHub() *Hub {
	return &Hub{
		channels:       make(map[string]map[*Client]bool),
		lastseen:       make(map[string]int64),
		upstreamJoined: make(map[string]bool),
		roomstate:      make(map[string][]byte),
		broadcast:      make(chan Message, 256),
		register:       make(chan *Client),
		unregister:     make(chan *Client),
		jsonpath: "channels.json",
	}
}