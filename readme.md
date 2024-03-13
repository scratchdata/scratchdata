# Scratch Data

Scratch Data is a wrapper that lets you stream data into and
out of your analytics database.
It takes arbitrary JSON as input and lets you perform analytical queries.

## Quickstart

### 1. Run the server

Clone the repo:
```bash
$ git clone git@github.com:scratchdata/scratchdata.git
$ cd scratchdata
```

Start the service:
``` bash
$ go run . 
```

With no configuration, this will automatically set up a local DuckDB 
database ready for reading and writing.

### 2. Insert JSON data

``` bash
$ curl -X POST "http://localhost:8080/api/data/insert/events&api_key=local" \
    --json '{"user": "alice", "event": "click"}'
```

The "events" table and columns are automatically
created.

### 3. Query

```bash
curl -G "http://localhost:8080/api/data/query" \
     --data-urlencode="api_key=local" \
     --data-urlencode="query=select * from events" 
```

## Next Steps

To see the full list of options, look at:
https://docs.scratchdata.com
