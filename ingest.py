import json
import pandas as pd
import duckdb
from datetime import datetime, timezone

def init_tables(con):
    """
    Create tables as defined in the schema plan
    """
    con.sql("""
    CREATE TABLE IF NOT EXISTS users (
        user_name VARCHAR PRIMARY KEY
    );
    """)
    con.sql("""
    CREATE TABLE IF NOT EXISTS recordings (
        recording_msid  VARCHAR PRIMARY KEY,
        track_name      VARCHAR NOT NULL,
        artist_name     VARCHAR NOT NULL,
        release_name    VARCHAR NOT NULL,
        artist_msid     VARCHAR,
        release_msid    VARCHAR
    );
    """)
    con.sql("""
    CREATE TABLE IF NOT EXISTS listens (
        user_name       VARCHAR REFERENCES users(user_name),
        recording_msid  VARCHAR REFERENCES recordings(recording_msid),
        listened_at     TIMESTAMP,
        PRIMARY KEY (user_name, recording_msid, listened_at)
    );
    """)
    con.sql("""
    CREATE TABLE IF NOT EXISTS corrupted_data_log (
        line_number   INTEGER PRIMARY KEY,
        raw_text      VARCHAR,
        error_message VARCHAR
    );
    """)

def log_malformed(con, line_number, raw_text, error_message):
    """
    Insert into corrupted_data_log if that line_number isn't json.
    """
    con.execute("""
        INSERT OR IGNORE INTO corrupted_data_log(line_number, raw_text, error_message)
        VALUES (?, ?, ?);""", (line_number, raw_text, error_message))

def ingest_all_at_once(file_path, db_path="listens_all.duckdb"):
    """
    Bulk ingest into the duckdb tables
    """
    # Connect to DuckDB and initialize tables
    con = duckdb.connect(db_path)
    print("connection to database established")
    init_tables(con)
    print("tables created and ingestion is in progress")

    users_list = []
    recordings_list = []
    listens_list = []

    # parse through the file once
    with open(file_path, "r", encoding="utf-8") as f:
        for lineno, json_line in enumerate(f, start=1):
            raw_json = json_line.strip()
            if not raw_json:
                continue

            try:
                rec = json.loads(raw_json)
            except json.JSONDecodeError as e:
                log_malformed(con, lineno, raw_json, str(e))
                continue

            # Extract user_name
            user_name = rec.get("user_name")
            if not user_name:
                continue
            users_list.append({"user_name": user_name})

            # Extract recording metadata
            track_metadata   = rec.get("track_metadata", {}) or {}
            additional_info = track_metadata.get("additional_info", {}) or {}
            rec_id = additional_info.get("recording_msid") or rec.get("recording_msid")
            if not rec_id:
                continue

            track_name = (track_metadata.get("track_name") or "").strip()
            artist_name = (track_metadata.get("artist_name") or "").strip()
            release_name = (track_metadata.get("release_name") or "").strip()
            if not (track_name and artist_name and release_name):
                continue

            recordings_list.append({
                "recording_msid": rec_id,
                "track_name":     track_name,
                "artist_name":    artist_name,
                "release_name":   release_name,
                "artist_msid":    additional_info.get("artist_msid"),
                "release_msid":   additional_info.get("release_msid")
            })

            # Extract listened_at timestamp if numeric
            listened_time = rec.get("listened_at")
            if isinstance(listened_time, (int, float)):
                ts = datetime.fromtimestamp(listened_time, tz=timezone.utc)
                listens_list.append({
                    "user_name":       user_name,
                    "recording_msid":  rec_id,
                    "listened_at":     ts
                })

    # Convert to DataFrames & drop duplicates
    users_df   = pd.DataFrame(users_list).drop_duplicates()
    records_df    = pd.DataFrame(recordings_list).drop_duplicates()
    listens_df = pd.DataFrame(listens_list).drop_duplicates()

    # Bulk‐upsert users via INSERT OR IGNORE
    if not users_df.empty:
        con.execute("""
            INSERT OR IGNORE INTO users (user_name)
            SELECT user_name
            FROM users_df;
        """)

    # Bulk‐upsert recordings via INSERT OR IGNORE
    if not records_df.empty:
        con.execute("""
            INSERT OR IGNORE INTO recordings
                (recording_msid, track_name, artist_name, release_name, artist_msid, release_msid)
            SELECT recording_msid, track_name, artist_name, release_name, artist_msid, release_msid
            FROM records_df;
        """)

    # Bulk‐upsert listens via INSERT OR IGNORE
    if not listens_df.empty:
        con.execute("""
            INSERT OR IGNORE INTO listens (user_name, recording_msid, listened_at)
            SELECT user_name, recording_msid, listened_at
            FROM listens_df;
        """)

    con.close()
    print("Full ingestion complete. corrupted lines (if any) have been logged. Proceed with Data Analysis.")


if __name__ == "__main__":
    import sys

    # Expect at least one argument (the data file); second arg (the output db) is optional.
    if len(sys.argv) < 2:
        print("Usage: python ingest.py <path/to/dataset.txt> [<path/to/output.duckdb>]")
        sys.exit(1)

    input_file = sys.argv[1]
    # If the user gave a second argument, use it; otherwise default to "music-listens.duckdb"
    output_db = sys.argv[2] if len(sys.argv) > 2 else "music-listens.duckdb"

    ingest_all_at_once(input_file, output_db)