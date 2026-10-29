// init: 
// start server
// - serve clients on port ___, push server uses /register, /unregister.
// - use /ws

/*
client:
- data struct
- every client has their nick and pass as vars
- func: validate lives here
- 
*/

/*
upstream:
- connect to twitch via irc
- pass, nick from hub
- functions only. nothing "runs" here, hub calls the object

functions:
- connect: takes pass, nick, returns ws pointer with tags
(tags are NOT something that we config, we just pass them anyway)
- maintainenance is from the hub run()
*/


/*
one hub per device, hub owns an upstream object (?).
channels:
- message in (from upstream)
- message out (to client)

IRC:
- accept nick
- accept pass (token)
- auth pass
- pass tags
- pass userstate, globaluserstate, everything twitch gives
- join channel
- part channel

upstream:
- register for all tags
- pong the ping
- part after x time inactivity

downstream:
- kill hub
- pass up and down tags
- 


- app opens
- client joins /ws
- hub is spawned for the client.
- client id generated from the hub, given to client
- the client sends nick / pass (token) through IRC
- server parses, then stores it in the hub.
- server creates an upstream ws.
- server passes creds to upstream IRC.
- requests all tags.
- how to communicate from upstream to hub?
- hub gets the messages from upstream.
- hub handles ping to upstream.
- user can ping hub to check connection.
- everything gets forwarded to user from upstream.
- user can pass commands to hub (WHAT COMMANDS?), hub handles and asks if needed
- JOIN, PART commands. what else?
- hub routes, forwards to upstream.
- when user disconnects: hub stays
- user reconnects, main routes user to the right hub.
- user doesn't reconnect for >1 day (we can adjust to demand!), hub kills.
- need a kill func for hub
- need channels to connect from hub to user
*/


package main

import ()

func main() {
	
}