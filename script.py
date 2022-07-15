import sqlite3

with sqlite3.connect("data/movies.sqlite") as co:
    print(
        co.execute(
            "select datetime(rating_timestamp, 'unixepoch') from ratings limit 10"
        ).fetchall()
    )
