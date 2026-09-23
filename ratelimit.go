package main

// 20ch/10s, this is important for both normal clients and cached join on start

import (
	"time"
	"sync"
)

type RateLimiter struct {
	mu       sync.Mutex
	queue    []string
	upstream chan string
}

func NewRateLimiter(upstream chan string) *RateLimiter {
	r := &RateLimiter{upstream: upstream}
	go r.drain()
	return r
}

func (r *RateLimiter) Enqueue(cmd string) {
	r.mu.Lock()
    defer r.mu.Unlock()
	r.queue = append(r.queue, cmd)
}

func (r *RateLimiter) drain() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		r.mu.Lock()
		sent := 0
		for sent < 6 && len(r.queue) > 0 {
			select {
			case r.upstream <- r.queue[0]:
				r.queue = r.queue[1:]
				sent++
			default:
				sent = 6
			}
		}
		r.mu.Unlock()
	}
}