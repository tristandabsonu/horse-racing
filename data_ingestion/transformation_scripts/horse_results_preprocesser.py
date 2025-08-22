import sqlite3
import pandas as pd
import numpy as np
import re

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
    
    last_10 TEXT,
    prize TEXT,
    avg_earn TEXT,
    last_win TEXT,
    win_percent TEXT,
    place_percent TEXT,
    tj_win_percent TEXT,
    
    career TEXT,
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
    df['age'] = pd.to_numeric(df['age'].str.replace('yo', ''), errors='coerce')     
    
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
    pd.to_numeric(                                  # everywhere else:
        df["margin"].astype(str).str.rstrip("L"),   #   drop the final 'L'
        errors="coerce"                             #   make bad text → NaN
        )
    )
    
    # sp
    df['sp'] = pd.to_numeric(df['sp'].replace('-', None))
    
    # form_letters
    # make one 0/1 column per code (puntapi )
    s = df['form_letters'].replace({'None': None}).fillna('').astype(str)
    s = s.str.replace(r'\s+', '', regex=True)  # remove spaces

    # multi-char codes
    flag_HT = s.str.contains(r'\(HT\)|\bHT\b', case=False, regex=True)
    flag_G  = s.str.contains(r'\(G\)', case=False, regex=True)
    flag_DA = s.str.contains(r'D/A', case=False, regex=True)  # matches (D/A) too

    # avoid counting the 't' inside (HT) when checking single letters
    s_single = s.str.replace(r'\(HT\)', '', case=False, regex=True).str.lower()

    # single-letter codes
    for code in ['t', 'd', 's', 'h', 'n', 'b', 'o']:
        df[f'has_{code}'] = s_single.str.contains(code, regex=False).astype('uint8')

    # add multi-char flags
    df['has_HT'] = flag_HT.astype('uint8')
    df['has_G']  = flag_G.astype('uint8')
    df['has_DA'] = flag_DA.astype('uint8')
    
    # rating
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0)
    
    # best_win
    df["best_win"] = pd.to_numeric(df["best_win"].str.strip('$'), errors="coerce")    
    
    # "career", "jh", "twelve_month", "season", "track", "distance", "track_dist",
    # "firm", "good", "soft", "heavy", "wet", "first_up", "second_up", "third_up",
    # "class", "group1", "group2", "group3", "listed", "clockwise", "a_clockwise",
    # "night", "synthetic", "as_fav"    
    # split record-style fields like "2: 0-0-0" -> *_starts, *_wins, *_seconds, *_thirds
    record_cols = [
        "career", "jh", "twelve_month", "season", "track", "distance", "track_dist",
        "firm", "good", "soft", "heavy", "wet", "first_up", "second_up", "third_up",
        "class", "group1", "group2", "group3", "listed", "clockwise", "a_clockwise",
        "night", "synthetic", "as_fav"
    ]

    pattern = r'^\s*(?P<starts>\d+)\s*:\s*(?P<wins>\d+)\s*-\s*(?P<seconds>\d+)\s*-\s*(?P<thirds>\d+)\s*$'

    for col in record_cols:
        if col in df.columns:
            parts = (
                df[col]
                .astype(str).str.strip()           # handles None/'-'/'' etc.
                .str.extract(pattern)              # non-matches -> NaN
            )

            for k in ("starts", "wins", "seconds", "thirds"):
                df[f"{col}_{k}"] = (
                    pd.to_numeric(parts[k], errors="coerce")
                    .fillna(0)
                    .astype("Int16")
                )

    df.drop(columns=record_cols, inplace=True)      # drop original columns
    
    # last_10
    def _map_pos(c):
        if c == '0':  # 10th or worse
            return 10
        if c in 'FL':  # DNF / Lost rider
            return 11
        return int(c)   # '1'..'9'

    def _last10_five(s):
        s = '' if pd.isna(s) else str(s).upper().strip()
        s = re.sub(r'[^0-9XFL]', '', s)  # keep only valid chars

        runs = [c for c in s if c in '0123456789FL']

        if runs:
            pos = [_map_pos(c) for c in runs]
            n = len(pos)
            w = np.arange(1, n + 1, dtype=float)  # recency weights 1..n
            out = {
                'last10_wavg_pos': float(np.average(pos, weights=w)),
                'last10_last_pos': pos[-1],
                'last10_best_pos': min(pos),
                'last10_top5': int(sum(p <= 5 for p in pos)),
            }
        else:
            out = {
                'last10_wavg_pos': 0,
                'last10_last_pos': 0,
                'last10_best_pos': 0,
                'last10_top5': 0,
            }

        # runs since last spell = race symbols after last 'X'
        last_x = s.rfind('X')
        since = (len(runs) if last_x == -1
                    else sum(c in '0123456789FL' for c in s[last_x+1:]))
        out['last10_runs_since_spell'] = since
        return pd.Series(out)

    feats5 = df['last_10'].apply(_last10_five)

    # tidy dtypes
    feats5['last10_top5'] = feats5['last10_top5'].astype('Int16')
    feats5['last10_runs_since_spell'] = feats5['last10_runs_since_spell'].astype('Int16')
    feats5['last10_last_pos'] = feats5['last10_last_pos'].astype('Int16')
    feats5['last10_best_pos'] = feats5['last10_best_pos'].astype('Int16')

    df = pd.concat([df, feats5], axis=1).drop(columns='last_10')
    
    # prize
    tmp = (df['prize'].astype(str).str.strip().str.replace(',', '', regex=False).str.upper())
    ex  = tmp.str.extract(r'^\$?\s*(\d+(?:\.\d+)?)\s*([KM]?)\s*$')
    df['prize'] = pd.to_numeric(ex[0], errors='coerce') * ex[1].map({'K':1000.0,'M':1000000.0}).fillna(1.0)
    
    # avg_earn    
    tmp = (df['avg_earn'].astype(str).str.strip().str.replace(',', '', regex=False).str.upper())
    ex  = tmp.str.extract(r'^\$?\s*(\d+(?:\.\d+)?)\s*([KM]?)\s*$')
    df['avg_earn'] = pd.to_numeric(ex[0], errors='coerce') * ex[1].map({'K':1000.0,'M':1000000.0}).fillna(1.0)
    
    # win_percent, place_percent, tj_win_percent, roi
    for c in ['win_percent', 'place_percent', 'tj_win_percent', 'roi']:
        s = (df[c].astype(str).str.strip()
                        .str.replace('%', '', regex=False))
        s = s.replace({'': '0', '-': '0', '–': '0', '—': '0', 'None': '0'})
        df[c] = pd.to_numeric(s, errors='coerce') / 100.0
    
    print(df['win_percent'].value_counts())
    






    # print(df.shape)
    #print(df.columns)
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
        
        #return      ## TEMPORARY RETURN
    
    print("Combining all chunks...")
    return pd.concat(chunks, ignore_index=True)




raw_conn = sqlite3.connect('../raw_racing_data.db')
transform_horse_results(raw_conn)
