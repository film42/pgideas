Schema Finder
=============

This was a friday afternoon fun project to see if I couldn't pull the cockroachdb postgres sql
parser in as a lib and use it to pluck the schemas found in postgres sql queries. This was just
a fun thing to do. But if you're looking for an example of using the cockroachdb sql parser in
your project, then take a close look at the `go.mod` file. NOTE: I referenced the main github
project but had to add a replace rule to point to their mirrored project with all code
pre-generated.

### Building

```
$ go get ./...

$ go build
```

### Running

```
$ ./schema_finder 
=== SQL:
SELECT * FROM boyz.to_men AS t, yolozzz.bro AS x WHERE t.id = 1
==== SCHEMAS:
[]string{"boyz", "yolozzz"} nil

...
```
