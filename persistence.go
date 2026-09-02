package main

import (
    "encoding/json"
    "os"
    "time"
)

func SaveChannels(channels map[string]int64, path string) {
    data, _ := json.MarshalIndent(channels, "", "  ")
    os.WriteFile(path, data, 0644)
}

func LoadChannels(path string) map[string]int64 {
    data, err := os.ReadFile(path)
    if err != nil {
        return make(map[string]int64)
    }
    var channels map[string]int64
    json.Unmarshal(data, &channels)
    return channels
}

func FilterStale(channels map[string]int64) []string {
    cutoff := time.Now().Add(-24 * time.Hour).Unix() // 1day old without anyone asking for it
    var fresh []string
    for ch, ts := range channels {
        if ts > cutoff {
            fresh = append(fresh, ch)
        }
    }
    return fresh
}