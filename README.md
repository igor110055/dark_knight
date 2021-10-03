A single connection is only valid for 24 hours; expect to be disconnected at the 24 hour mark
The websocket server will send a ping frame every 5 minutes. If the websocket server does not receive a pong frame back from the connection within a 15 minute period, the connection will be disconnected. Unsolicited pong frames are allowed.

WebSocket connections have a limit of 10 incoming messages per second.

A connection that goes beyond the limit will be disconnected; IPs that are repeatedly disconnected may be banned.

A single connection can listen to a maximum of 200 streams.