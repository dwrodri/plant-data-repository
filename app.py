from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from typing import Dict

from datetime import datetime

import sqlite3

app = FastAPI()


@app.post("/upload")
async def receive_data(payload: Dict):
    # Step  1: Obtain the JSON payload as a dictionary
    readings = payload.get("readings", [])
    timestamps = payload.get("timestamps", [])
    sensor_id = payload.get("sensor_id", "")

    # Step  2: Open a connection to the SQLite database
    conn = sqlite3.connect("planthub.sqlite")
    cursor = conn.cursor()

    # Step  3: Check if there's a table present with a name matching the "sensor_id" field
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (sensor_id,)
    )
    table_exists = cursor.fetchone() is not None

    if not table_exists:
        # Create the table if it doesn't exist
        cursor.execute(
            f"""
            CREATE TABLE {sensor_id} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                readings INTEGER NOT NULL,
                timestamps TEXT NOT NULL
            )
        """
        )
        conn.commit()

    # Step  4: Convert the "ms since Unix epoch" timestamps into ISO-8601 format
    # and then append the timestamp and the reading integer of the same index to the list
    for i in range(len(timestamps)):
        timestamp_iso = datetime.utcfromtimestamp(timestamps[i] / 1000).isoformat()
        reading = readings[i]
        cursor.execute(
            f"INSERT INTO {sensor_id} (readings, timestamps) VALUES (?, ?)",
            (reading, timestamp_iso),
        )

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    return {"message": f"Data received and stored for sensor ID {sensor_id}"}


@app.get("/", response_class=HTMLResponse)
async def root():
    return """<h1>DB Wrapper is up!</h1>"""
