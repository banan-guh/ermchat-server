package main

import (
    //"encoding/json"
    //"os"
    "time"
)

func FilterStale(channels map[string]int64) ([]string, []string) {
    cutoff := time.Now().Add(-24 * time.Hour).Unix() // 1day old without anyone asking for it
    var fresh []string
	var stale []string
    for ch, ts := range channels {
        if ts > cutoff {
            fresh = append(fresh, ch)
        } else {
			stale = append(stale, ch)
		}
    }
    return fresh, stale
}