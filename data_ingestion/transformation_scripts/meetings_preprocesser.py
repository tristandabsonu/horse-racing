import pandas as pd


def transform_meetings(raw_conn: pd.DataFrame) -> pd.DataFrame:
    df = pd.read_sql("SELECT * FROM meetings", raw_conn)
    df = df.replace("", None)

    # name
    df['name'] = df['name'].str.lower()
    
    # time_group
    df['time_group'] = df['time_group'].str.lower()
    
    # address
    df['address'] = df['address'].str.strip()
    df['address'] = df['address'].str.lower()

    # state
    df['state'] = df['state'].str.lower()
    
    # country
    df['country'] = df['country'].str.lower()
    
    # rail_position
    df['rail_position'] = df['rail_position'].str.lower()
    
    # track_comments
    df['track_comments'] = df['track_comments'].replace("N/A", "no comments")
    df['track_comments'] = df['track_comments'].str.lower()

    # meeting_type
    df['meeting_type'] = df['meeting_type'].str.lower()

    # meeting_category
    df['meeting_category'] = df['meeting_category'].str.lower()

    return df    
