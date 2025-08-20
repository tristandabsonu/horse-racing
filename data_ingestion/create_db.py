import sqlite3

conn = sqlite3.connect("raw_racing_data.db")
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


cursor.execute("""
CREATE TABLE IF NOT EXISTS race_details (
    race_id TEXT PRIMARY KEY,
    total_prize TEXT,
    first_prize TEXT,
    second_prize TEXT,
    third_prize TEXT,
    winning_time TEXT,
    sectional_time TEXT,
    track_rail_info TEXT,
    FOREIGN KEY (race_id) REFERENCES races (race_id)
)
""")


cursor.execute("""
CREATE TABLE IF NOT EXISTS horse_results (
    meeting_id TEXT,
    race_id TEXT,
    finish_position TEXT,
    running_number TEXT,
    name TEXT,
    barrier TEXT,
    age TEXT,
    sex TEXT,
    trainer TEXT,
    jockey TEXT,
    weight TEXT,
    sire TEXT,
    dam TEXT,
    position_400m TEXT,
    position_800m TEXT,
    margin TEXT,
    sp TEXT,
    flucs TEXT,
    sire_all TEXT,
    sire_dry TEXT,
    sire_wet TEXT,
    sire_starts TEXT,
    form_letters TEXT,
    rating TEXT,
    last_race TEXT,
    best_win TEXT,
    career TEXT,
    last_10 TEXT,
    prize TEXT,
    avg_earn TEXT,
    last_win TEXT,
    win_percent TEXT,
    place_percent TEXT,
    tj_win_percent TEXT,
    jh TEXT,
    twelve_month TEXT,
    season TEXT,
    track TEXT,
    distance TEXT,
    track_dist TEXT,
    firm TEXT,
    good TEXT,
    soft TEXT,
    heavy TEXT,
    wet TEXT,
    first_up TEXT,
    second_up TEXT,
    third_up TEXT,
    class TEXT,
    group1 TEXT,
    group2 TEXT,
    group3 TEXT,
    listed TEXT,
    clockwise TEXT,
    a_clockwise TEXT,
    night TEXT,
    synthetic TEXT,
    as_fav TEXT,
    roi TEXT,
    PRIMARY KEY (race_id, running_number),
    FOREIGN KEY (meeting_id) REFERENCES meetings (meeting_id),
    FOREIGN KEY (race_id) REFERENCES races (race_id)
)
""")

conn.commit()
conn.close()
