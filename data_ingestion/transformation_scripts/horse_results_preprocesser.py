import sqlite3
import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 500)

"""
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
"""


def extract_running_number(running_number):
   """Extract numeric running number, handling emergency runners."""
   running_str = str(running_number).strip().lower()
   
   # Remove 'e' suffix and convert to int
   if running_str.endswith('e'):
       running_str = running_str[:-1]
   
   try:
       return int(running_str)
   except ValueError:
       return None


def process_chunk(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.replace("", None)

    # finish_position
    df["finish_position"] = pd.to_numeric(df["finish_position"].astype(str).str.extract(r"^(\d+)")[0], errors="coerce")    
    
    # running_number
    df['is_emergency_runner'] = df['running_number'].astype(str).str.endswith('e', na=False)
    df['running_number'] = df['running_number'].apply(extract_running_number)
    
    # name
    df['name'] = df['name'].str.lower()
    
    # barrier
    df['barrier'] = pd.to_numeric(df['barrier'], errors='coerce')
    
    # age
    df['age'] = df['age'].str.replace('yo', '')     
    
    # sex
    df['sex'] = df['sex'].str.lower()
    
    # trainer and jockey
    df['trainer'] = df['trainer'].str.lower()
    df['jockey'] = df['jockey'].str.lower()

    # weight
    weight_pattern = r'\((\d+(?:\.\d+)?)kg(?: cd (\d+(?:\.\d+)?)kg)?\)'
    weight_extracted = df['weight'].str.extract(weight_pattern)
    df['original_weight'] = pd.to_numeric(weight_extracted[0], errors='coerce')
    df['adjusted_weight'] = pd.to_numeric(weight_extracted[1].fillna(weight_extracted[0]), errors='coerce')
    
    # sire and dam
    df['sire'] = df['sire'].str.lower()
    df['dam'] = df['dam'].str.lower()
    
    # pos400 and pos800
    df["pos_400"] = pd.to_numeric(df["position_400m"].astype(str).str.extract(r"^(\d+)")[0], errors="coerce")    
    df["pos_800"] = pd.to_numeric(df["position_800m"].astype(str).str.extract(r"^(\d+)")[0], errors="coerce")    
    
    
    df = df.drop(columns=['position_400m','position_800m'])
    
    # margin
    df["margin"] = np.where(
    df["finish_position"] == 1,                     # winners
    0,                                              # → put 0
    pd.to_numeric(                                  # everyone else:
        df["margin"].astype(str).str.rstrip("L"),   #   drop the final 'L'
        errors="coerce"                             #   make bad text → NaN
        )
    )
    
    print(df['margin'].value_counts())
    
    







    # print(df.shape)
    print(df.columns)
    # print(df.head())

    return df


def transform_horse_results(raw_conn, chunk_size: int = 25000) -> pd.DataFrame:
    """Process horse results data in chunks."""
    query = "SELECT * FROM horse_results"
    chunks = []
    
    for i, chunk in enumerate(pd.read_sql(query, raw_conn, chunksize=chunk_size)):
        print(f"Processing chunk {i+1} ({len(chunk)} rows)...")
        
        # Apply transformation to chunk
        transformed_chunk = process_chunk(chunk)
        chunks.append(transformed_chunk)
        
        # Optional: Memory monitoring
        print(f"Chunk memory: {transformed_chunk.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
        
        return      ## TEMPORARY RETURN
    
    print("Combining all chunks...")
    return pd.concat(chunks, ignore_index=True)




raw_conn = sqlite3.connect('../raw_racing_data.db')
transform_horse_results(raw_conn)
