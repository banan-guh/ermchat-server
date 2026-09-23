package main

import (
	"sync"
	"time"
	"net/http"
	"fmt"
	"encoding/json"
)

// $$$$$#########!!!!!!!!!!!!!!!!!!!!! TODO TODO TODO: ADD COMMENTS !!!!!!

var validateClient = &http.Client{Timeout: 5 * time.Second}

type identity struct {
	userID string // uncapitalized
	login  string
}

type identityCache struct { // for tokens
	mu  sync.Mutex
	ids map[string]identity
}

func NewIdentityCache() *identityCache {
	return &identityCache{ids: make(map[string]identity)}
}

func (c *identityCache) get(token string) (identity, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id, ok := c.ids[token]
	return id, ok
}

func (c *identityCache) set(token string, id identity) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids[token] = id
}

var identities = NewIdentityCache()
func validateToken(token string) (identity, error) {
	id, ok := identities.get(token)
	if ok { return id, nil }

	req, err := http.NewRequest("GET", "https://id.twitch.tv/oauth2/validate", nil)
	if err != nil {
		return identity{}, err
	}
	req.Header.Set("Authorization", "OAuth "+token)
	resp, err := validateClient.Do(req)
	if err != nil {
		return identity{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return identity{}, fmt.Errorf("validate: %s", resp.Status)
	}

	var tokenClaims struct {
		UserID string `json:"user_id"` // capitalized
		Login  string `json:"login"`
	}

	err = json.NewDecoder(resp.Body).Decode(&tokenClaims)
	if err != nil {
		return identity{}, err
	}

	id = identity{
		userID: tokenClaims.UserID,
		login: tokenClaims.Login,
	}
	identities.set(token, id)
	return id, nil
}