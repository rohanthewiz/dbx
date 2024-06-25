# DBX - Database Explorer

DBX is a simple tool to programmatically explore databases by running adhoc and benchmark queries.
Results are collected into an in-memory table and can be pretty printed or exported to CSV

## Setup
1. Make sure you have Go >= 1.21.0 installed https://go.dev/doc/install. 
2. Clone the repository `git clone https://github.com/rohanthewiz/dbx.git`.
3. Change directory to the project and build `cd dbx && go mod tidy && go build`
4. Run the app with options pointing to your database.

## Usage

```text
./dbx -h
Usage of ./dbx:
  -columnar
        use columnar engine
  -dbname string
        database name
  -dbtype string
        database type: pg, alloydb
  -description string
        query description (default "Query")
  -host string
        database host
  -pass string
        password
  -port int
        database port (default 5432)
  -query string
        query file path
  -runs int
        number of runs (default 5)
  -ssl
        use ssl
  -user string
        username
```

### Examples

```bash
./dbx -dbtype pg -host localhost -dbname mydb -user myuser -pass mypass -query="sample_query.sql"
go run . -dbtype pg -host localhost -dbname mydb -user me -pass secret -ssl -query="sample_query.sql" -runs 3
./dbx -dbtype alloydb -columnar -host localhost -dbname mydb -user myuser -pass mypass -port 5434 -ssl -query="sample_query.sql" -runs 5
```

###  Note
* See `bench/db_run.go` for programmatic usage.
