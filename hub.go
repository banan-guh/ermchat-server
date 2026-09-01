package main

import (
	"log"
)

type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
			log.Printf("client joined (%d total)", len(h.clients))
		case client := <-h.unregister:
			delete(h.clients, client)
			log.Printf("client left (%d total)", len(h.clients))
		case msg := <-h.broadcast:
			for client := range h.clients {
				client.send <- msg
			}
		}
	}
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}