PG Overpass
===========

This is an experimental project. Do not use.

A simple utility that allows for easy connection switching. No pooling.

What this does in steps:
1. On client connect, connects to the current destination server.
2. Handles client hellow based on server hellow.
3. Watches query loop. Proxy bytes.
4. When destrination server changes, swap out a new server for each client after ReadyForQuery.

---

You can send a SIGHUP to the process and it will reload `examples/config.json` from disk. Whe it does
this is will also force all postgres connections to refresh. Connections with an active transaciton
will wait for the transaction to copmlete before refreshing the connection.

License MIT
