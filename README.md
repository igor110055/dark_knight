A single connection is only valid for 24 hours; expect to be disconnected at the 24 hour mark
The websocket server will send a ping frame every 5 minutes. If the websocket server does not receive a pong frame back from the connection within a 15 minute period, the connection will be disconnected. Unsolicited pong frames are allowed.

WebSocket connections have a limit of 10 incoming messages per second.

A connection that goes beyond the limit will be disconnected; IPs that are repeatedly disconnected may be banned.

A single connection can listen to a maximum of 200 streams.

---
1. get available trading size
2. estimate profit
(3. add age of order to order book)
4. estimate probability of making profit (available size and age of order)
5. post related limit order to chain the reaction (STORM!)


Architecture:

- Asyncio for WebSocket streams
  - receive order book update

- Threading for WebSockets (easier to share memory)

- Celery for order execution (multiprocessing)
  - update order book redis
  - create order
  - calculate PnL




Tasks:
1. check ping lag (milliseconds)
2. respect lot size
3. calculate better size


Pending:
1. make singleton binance client
2. check redis failed trade

3. gradually load order book, one by one
4. TTL on everything in redis to ensure no obsolete data, re populate if needed
5. clear redis cache on initialisation