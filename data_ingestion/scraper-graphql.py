import requests, json
import sqlite3
import time
import random
from datetime import datetime, timedelta


BASE = "https://puntapi.com/graphql-horse-racing"

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",               
    "X-Apollo-Operation-Name": "meetingsIndexByStartEndDate",  
    "Apollo-Require-Preflight": "true",       
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-AU,en;q=0.9",
    "Origin": "https://www.racenet.com.au",
    "Referer": "https://www.racenet.com.au/",
    "Authorization": "Bearer none",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    # avoid 'PersistedQueryNotFound' errors
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def fetch_params(date: str):
    variables = {
        "startDate": date,
        "endDate": date,
        "limit": 100,
    }
    params = {
        "operationName": "meetingsIndexByStartEndDate",
        "variables": json.dumps(variables, separators=(",", ":")),
        "extensions": json.dumps({
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "992dec969fbec58f92989c0dbfcefbed0b49bdf0ed2adc2d8f19918e7a8599db",
            }
        }, separators=(",", ":")),
    }

    return params


def fetch_meeting_group_index(groups, group="australia"):
    for g in range(len(groups)):
        if str(groups[g].get("group", "")).lower() == group:
            return g
    return None
        

def process_meeting(meeting):
    # basic info
    meeting_id = meeting.get("id", "")
    name = meeting.get("name", "")
    slug = meeting.get("slug", "")
    date_utc = meeting.get("meetingDateUtc", "")
    time_group = meeting.get("timeGroup", "")
    
    # location
    venue = meeting.get("venue", {})
    address = venue.get("address", "")
    state = meeting.get("state", "")
    country = venue.get("country", {}).get("name", "")
    
    # track info
    rail_position = meeting.get("railPosition", "")
    track_comments = meeting.get("trackComments", "")
    penetrometer = meeting.get("penetrometer", "")
    weather_last_updated = venue.get("weatherLastUpdated", "")
    
    # type and category
    meeting_type = meeting.get("meetingType", "")
    meeting_category = meeting.get("meetingCategory", "")
    
    # money
    meeting_total_prize = meeting.get("totalPrizeMoney", "")
    
    return (
        meeting_id, name, slug, date_utc, time_group,
        address, state, country,
        rail_position, track_comments, penetrometer, weather_last_updated,
        meeting_type, meeting_category,
        meeting_total_prize
    )


def process_race(race, meeting_id):
    # id
    race_id = race.get("id", "")
    slug = race.get("slug", "")
    event_number = race.get("eventNumber", "")
    
    # basic info
    name = race.get("name", "")
    distance = race.get("distance", "")
    event_class = race.get("eventClass", "")
    group_type = race.get("groupType", "")
    track_type = race.get("trackType", "")
    
    # timing
    start_time = race.get("startTime", "")
    end_time = race.get("endTime", "")
    
    # track conditions
    track_condition = race.get("trackCondition", {})
    track_condition_overall = track_condition.get("overall", "")
    track_condition_rating = track_condition.get("rating", "")
    track_condition_surface = track_condition.get("surface", "")
    
    # result-related
    is_abandoned = race.get("isAbandoned", "")
    place_winners = race.get("placeWinners", "")

    return (
        race_id, meeting_id, slug, event_number,
        name, distance, event_class, group_type, track_type,
        start_time, end_time,
        track_condition_overall, track_condition_rating, track_condition_surface,
        is_abandoned, place_winners
    )
    
    
def fetch_meetings_for_date(date, max_retries=5):
    meetings = []
    last_err = None

    for attempt in range(1, max_retries + 1):
        try:
            # --- API call
            params = fetch_params(date)
            response = requests.get(BASE, params=params, headers=HEADERS, timeout=30)
            print(f"[{date}] Attempt {attempt}/{max_retries} → HTTP {response.status_code}")

            data = response.json() if response.content else {}
            if isinstance(data, dict) and data.get("errors"):
                # GraphQL can return 200 + errors
                raise RuntimeError(f"GraphQL errors: {data['errors']}")

            groups = (data.get("data") or {}).get("meetingsGrouped", []) or []
            # find Australia group
            g_index = fetch_meeting_group_index(groups)

            if g_index is None:
                names = [str(g.get("group")) for g in groups]
                raise RuntimeError(f"'Australia' group missing. Groups seen: {names}")

            meetings = groups[g_index].get("meetings", []) or []
            if not meetings:
                raise RuntimeError("Australia group present but 'meetings' is empty")

            # exit retry loop when successful
            break

        except Exception as e:
            last_err = e
            if attempt == max_retries:
                msg = f"Error on {date} after {max_retries} tries: {e}\n"
                print(msg)
                with open("errors.txt", "a") as f:
                    f.write(msg)
                return  # give up for this date

            # custom backoff for PersistedQueryNotFound
            if 'PersistedQueryNotFound' in str(e):
                # longer delay: 3 minutes (150 seconds) + random jitter
                delay = 150 + random.uniform(0, 30)  # 150-180 seconds
                print(f"[{date}] PersistedQueryNotFound → Waiting longer: {delay:.1f}s…")
            else:
                # standard exponential backoff for other errors
                delay = min(2 ** (attempt - 1), 8) + random.uniform(0, 0.5)
                print(f"[{date}] Retry because: {e}. Waiting {delay:.1f}s…")
            
            time.sleep(delay)

    # --- DB write after we have data
    conn = sqlite3.connect("raw_racing_data.db")
    cursor = conn.cursor()

    for meeting in meetings:
        cursor.execute("""
            INSERT OR IGNORE INTO meetings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, process_meeting(meeting))

        for race in meeting.get("events", []):
            cursor.execute("""
                INSERT OR IGNORE INTO races VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, process_race(race, meeting.get("id", "")))

    conn.commit()
    conn.close()

        
        
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def main(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    for single_date in daterange(start, end):
        # convert date back to str
        date_str = single_date.strftime("%Y-%m-%d")
        print(f"Processing {date_str}...")
        try:
            # extract and load data
            fetch_meetings_for_date(date_str)
            # Wait between 10–15 seconds (very polite timing)
            time.sleep(random.uniform(5, 10))
            
        except Exception as e:
            error_message = f"Error on {date_str}: {e}\n"
            print(error_message)

            # Save the error to a file
            with open("errors.txt", "a") as f:
                f.write(error_message)
        
        print()


if __name__ == "__main__":
    main(start_date="2015-01-01", end_date="2025-06-30")






"""
r
-> 'data'

--> 'meetingsGrouped': LIST of groups of meetings (races) (known groups: Australia, International, Barrier Trials)

---> 'group': name of the meeting for the group (Australia, International, Barrier Trials)
---> 'meetings': LIST of the meetings within each group


SAMPLE 
----> {
        "id":"310010",
        "name":"Darwin",
        "slug":"darwin-20250716",
        "railPosition":"True",
        "timeGroup":"Day",
        "meetingDateUtc":"2025-07-16",
        "meetingDateLocal":"2025-07-16",
        "regionId":"1",
        "__typename":"Meeting",
        "sportId":"1",
        "penetrometer":0,
        "trackComments":"N/A",
        "tabStatus":true,
        "meetingCategory":"Professional",
        "meetingStage":"Results",
        "meetingType":"Metro",
        "totalPrizeMoney":189000,
        "state":"NT",
        "venue":{
            "id":"74",
            "name":"Darwin",
            "nameAbbrev":"DRWN",
            "slug":"darwin",
            "state":"NT",
            "__typename":"Venue",
            "isMetro":false,
            "address":"Buntine Drive, Fannie Bay NT 0820",
            "weatherLastUpdated":"2025-07-26T15:03:07.000Z",
            "country":{
                "id":"16",
                "name":"Australia",
                "iso2":"AU",
                "iso3":"AUS",
                "horseCountry":"AUS",
                "__typename":"Country"
            }
     },
"""