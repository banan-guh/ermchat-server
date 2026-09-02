package main

import (
	"log"
	//"strings"
	"sync"
)

type Message struct {
	Channel string
	Data    []byte
}

type Hub struct {
	mu         sync.Mutex
	channels   map[string]map[*Client]bool
	broadcast  chan Message
	register   chan *Client
	unregister chan *Client
	upstream   *TwitchUpstream
	limiter    *RateLimiter
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
			if clients, ok := h.channels[msg.Channel]; ok {
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
	if h.channels[channel] == nil {
		h.channels[channel] = make(map[*Client]bool)
		if h.upstream != nil {
			h.limiter.Enqueue("JOIN " + channel + "\r\n")
		}
	}
	h.channels[channel][c] = true
}

func (h *Hub) Leave(channel string, c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if clients, ok := h.channels[channel]; ok {
		delete(clients, c)
		if len(clients) == 0 {
			delete(h.channels, channel) // no one watching, clean up
			if h.upstream != nil {
				h.upstream.send <- "PART " + channel + "\r\n"
			}
		}
	}
}

func NewHub() *Hub {
	return &Hub{
		channels:   make(map[string]map[*Client]bool),
		broadcast:  make(chan Message, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}