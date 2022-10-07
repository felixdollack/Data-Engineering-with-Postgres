# Data Modeling with Postgres
This project contains an ETL pipeline for the fictionary startup Sparkify.

The purpose of the pipeline is to analyze what songs users are listening to.

The data is part of the [Million Song Dataset] and comes as JSON user logs and song metadata.

A PostgreSQL database with a star schema is setup with analytic focus and optimized queries for song plays.


## Data:
The data is a subset of the [Million Song Dataset].

Example of song metadata:
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

Example of song play user data:
```
{"artist": null, "auth": "Logged In", "firstName": "Walter", "gender": "M", "itemInSession": 0, "lastName": "Frye", "length": null, "level": "free", "location": "San Francisco-Oakland-Hayward, CA", "method": "GET", "page": "Home", "registration": 1540919166796.0, "sessionId": 38, "song": null, "status": 200, "ts": 1541105830796, "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"", "userId": "39"}
```


### Files included:
- README.md (this file)
- sql_queries.py
- create_tables.py
- etl.py
- etl.ipynb
- test.ipynb


### Requirements:
- a running PostgreSQL server
- psycopg2 package
- data from the [Million Song Dataset] in `data/song_data` and `data/log_data`

## Run:
```
# connect to the database and create the schema
python create_tables.py

# read the JSON files and fill the database
python etl.py
```

[Million Song Dataset]: http://millionsongdataset.com
