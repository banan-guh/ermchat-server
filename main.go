package main

// TODO: future things to do:
/*
- shrink tags (diff, send hashes), data saver
- pings (later)
*/

import (
	"log"
	"net/http"
	//"os"
	//"time"
)

func main() {
	
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(w, r)
	}) // this is super ugly, can we make it cleaner?
	
	log.Println("listening on :8080")
	log.Println("join this: ws://localhost:8080/ws")
	log.Fatal(http.ListenAndServe(":8080", nil))
}