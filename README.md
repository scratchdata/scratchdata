# ScratchDB

ScratchDB is a wrapper around duckdb that lets you input arbitrary JSON and
perform analytical queries against it. It automatically creates tables
and columns when new data is added.

## Quickstart

### 1. Run the server

``` bash
$ go run scratch test.db
```

### 2. Insert JSON data

``` bash
$ curl -X POST http://localhost:3000/data \
    -H 'Content-Type: application/json' \
    -d '{"table":"my_table","data":{"fruit": "apple"}}'
```

### 2. Query 

To view data in JSON format:

```
http://localhost:3000/query?q=select * from my_table
```

To view data in an HTML table:

```
http://localhost:3000/query?format=html&q=select * from my_table
```