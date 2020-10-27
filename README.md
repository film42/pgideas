PG Ideas
========

This repo is a collection of experiments I've toyed with to test different ideas.


1. PgFiesta - A connection pooler written in rust. Inspired by pgbouncer.
2. PgOverpass - A connection proxy with graceful connection reloading. Suppose you have a postgres health check that failed 
and you need to migrate your conection. Use PgOverpass to migrate the connection over to the new server without downtime. 
Active transactions will reload once they complete.

---

License MIT
