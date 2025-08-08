import sqlite3
import pandas as pd
import re

pd.set_option('display.max_rows', 500)


AGE_PATTERNS = [
    r'2[-\s&/]*y[-\s]*o\+?', r'3[-\s&/]*y[-\s]*o\+?',
    r'4[-\s&/]*y[-\s]*o\+?', r'5[-\s&/]*y[-\s]*o\+?',
    r'2\s*&\s*3[-\s]*y[-\s]*o', r'3\s*&\s*4[-\s]*y[-\s]*o',
    r'3,4\s*&\s*5[-\s]*y[-\s]*o', r'4\s*&\s*5[-\s]*y[-\s]*o',
    r'open'
]

SEX_PATTERNS = [
    r'fillies\s*&\s*mares', r'fillies', r'mares',
    r'colts\s*,?\s*horses\s*&\s*geldings',
    r'colts\s*&\s*geldings', r'c,g&e', r'c&g', r'e&g'
]

RACE_TYPE_PATTERNS = [
    r'weight\s*for\s*age|wfa',
    r'sw\s*\+\s*p', r'\bsw\b',           # SW + P and plain SW
    r'set\s*weights', r'handicap', r'highweight',
    r'quality', r'hurdle', r'steeple',
    r'special\s*conditions', r'trophy\s*race'
]

RACE_CLASS_PATTERNS = [
    r'group\s*1', r'group\s*2', r'group\s*3',
    r'lr|listed',
    r'class\s*\d{1,2}|class\s*[ab]',
    r'bm\d{2,3}\+?',                    # BM70, BM70+
    r'benchmark\s*\d{2,3}',             # "Benchmark 89"
    r'rtg\d{2,3}\+?',                   # RTG70+
    r'0\s*-\s*\d{2,3}',                 # 0-58 etc.
    r'rst\.?\s*\d{2,3}|rest\.?\s*\d{2,3}',  # Rst. 64, Rest 58
    r'open\s*trophy\s*race',
    r'benchmark\s*nt|bmnt',
    r'maiden',
    r'restricted',
    r'trophy\s*race(?:\(\d\))?'
]

def make_regex(patterns):
    return re.compile(r'(' + r'|'.join(patterns) + r')', re.IGNORECASE)


AGE_RE   = make_regex(AGE_PATTERNS)
SEX_RE   = make_regex(SEX_PATTERNS)
TYPE_RE  = make_regex(RACE_TYPE_PATTERNS)
CLASS_RE = make_regex(RACE_CLASS_PATTERNS)


def tidy(token: str) -> str:
    """Remove spaces, make upper-case, keep + and & chars."""
    return re.sub(r'\s+', '', token.upper())


def parse_event_class(raw: str) -> dict:
    text = raw.lower()

    def first(regex, default="OPEN", post=tidy):
        m = regex.search(text)
        return post(m.group(0)) if m else default

    return {
        "age_restriction": first(AGE_RE),
        "sex_restriction": first(SEX_RE, post=lambda x: tidy(x.title())),
        "race_type":       first(TYPE_RE),
        "race_class":      first(CLASS_RE),
    }

def transform_races(raw_conn: pd.DataFrame) -> pd.DataFrame:
    df = pd.read_sql("SELECT * FROM races LEFT JOIN race_details ON races.race_id = race_details.race_id", raw_conn)
    
    df = df.loc[:, ~df.columns.duplicated()]    # removes duplicated race_id column
    df = df.replace("", None)
    
    print(df.columns)
    print(df.head())

    # name
    df['name'] = df['name'].str.lower()
    
    # group_type
    df['group_type'] = df['group_type'].str.lower()
    
    # track_type
    df['track_type'] = df['track_type'].str.lower()
    
    # track_condition_overall
    df['track_condition_overall'] = df['track_condition_overall'].replace("N/A", None)
    
    # track_condition_surface
    # remove as it is 100% the same as track_type
    df = df.drop(columns='track_condition_surface')
    
    # total_prize
    df['total_prize'] = df['total_prize'].replace('[\$,]', '', regex=True)

    # first_prize
    df['first_prize'] = df['first_prize'].replace('[\$,]', '', regex=True)
    
    # second_prize
    df['second_prize'] = df['second_prize'].replace('[\$,]', '', regex=True)

    # third_prize
    df['third_prize'] = df['third_prize'].replace('[\$,]', '', regex=True)
    
    # winning_time
    parts = df['winning_time'].str.extract(r'(\d+):(\d+\.\d+)')

    minutes = pd.to_numeric(parts[0], errors='coerce')
    seconds = pd.to_numeric(parts[1], errors='coerce')
    
    df['winning_time'] = minutes * 60 + seconds 

    # sectional_time
    parts = df['sectional_time'].str.extract(r'(\d+):(\d+\.\d+)\s+at\s+(\d+)m')

    minutes = pd.to_numeric(parts[0], errors='coerce')
    seconds = pd.to_numeric(parts[1], errors='coerce')
    distance = pd.to_numeric(parts[2], errors='coerce')

    df['sectional_time'] = minutes * 60 + seconds          
    df['sectional_distance']  = distance.astype('Int64')
    
    # track_rail_info
    df['track_rail_info'] = df['track_rail_info'].str.lower()
    
    # event_class
    parsed = df['event_class'].astype(str).apply(parse_event_class).apply(pd.Series)
    df = pd.concat([df, parsed], axis=1)

    print(df.columns)
    print(df['age_restriction'].value_counts())
    print(df['sex_restriction'].value_counts())
    print(df['race_type'].value_counts())
    print(df['race_class'].value_counts())

    
    return df

raw_conn = sqlite3.connect('../raw_racing_data.db')
transform_races(raw_conn)