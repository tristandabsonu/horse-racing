import sqlite3

conn = sqlite3.connect("racing_data.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS meetings (
    meeting_id TEXT PRIMARY KEY,
    name TEXT,
    slug TEXT,
    date_utc TEXT,
    time_group TEXT,
    address TEXT,
    state TEXT,
    country TEXT,
    rail_position TEXT,
    track_comments TEXT,
    penetrometer REAL,
    weather_last_updated TEXT,
    meeting_type TEXT,
    meeting_category TEXT,
    meeting_total_prize REAL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS races (
    race_id TEXT PRIMARY KEY,
    meeting_id TEXT,
    slug TEXT,
    event_number TEXT,
    name TEXT,
    distance INTEGER,
    event_class TEXT,
    group_type TEXT,
    track_type TEXT,
    start_time TEXT,
    end_time TEXT,
    track_condition_overall TEXT,
    track_condition_rating TEXT,
    track_condition_surface TEXT,
    is_abandoned BOOLEAN,
    place_winners INTEGER,
    FOREIGN KEY (meeting_id) REFERENCES meetings (meeting_id)
)
""")

conn.commit()
conn.close()