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

#### Run with custom config

Create a `config.yaml` file with all of your settings and run:

``` bash
$ go run . config.yaml
```

#### Run in docker with custom config

Create a `config.yaml` file with all of your settings, as well as a data folder, and run:

``` bash
$ docker build . -t scratchdata
$ docker run -p 8080:8080 -v "./config.yaml:/build/config.yaml:ro" -v "./data:/build/data:rw" scratchdata config.yaml
```


### 2. Insert JSON data

``` bash
$ curl -X POST "http://localhost:8080/api/data/insert/events?api_key=local" \
    --data '{"user": "alice", "event": "click"}'
```

The "events" table and columns are automatically
created.

### 3. Query

```bash
curl -G "http://localhost:8080/api/data/query" \
     --data-urlencode "api_key=local" \
     --data-urlencode "query=select * from events" 
```

## Other Features

### Share Data

You can share data as CSV or JSON by creating "share links".

``` bash
$ curl -X POST "http://localhost:8080/api/data/query/share?api_key=local" \
    --data '{"query": "select * from events", "duration": 120}'
```

This will produce a query ID that expires in 120 seconds. From there, send the following link to users:

```
http://localhost:8080/share/<query_id>/data.csv
http://localhost:8080/share/<query_id>/data.json
```

### Copy Data

You can set up multiple databases and copy data between them.
You can run a SQL query against your source database and 
Scratch will automatically create a table and insert data into
a destination.

``` bash
$ curl -X POST "http://localhost:8080/api/data/copy?api_key=local" \
    --data '{"query": "select * from events", "destination_id": 3, "destination_table": "events"}'
```

## Next Steps

To see the full list of options, look at:
https://docs.scratchdata.com
