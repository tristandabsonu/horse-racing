import requests
from bs4 import BeautifulSoup
import sqlite3
import random
import time
from datetime import datetime, timedelta



USER_AGENTS = [
    # Chrome ‑ Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/137.0.0.0 Safari/537.36",                     

    # Chrome ‑ macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/137.0.0.0 Safari/537.36",                    

    # Chrome ‑ Linux
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36",                     

    # Chrome ‑ Android
    "Mozilla/5.0 (Linux; Android 10; K) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Mobile Safari/537.36",               

    # Chrome (CriOS) ‑ iPhone
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "CriOS/126.0.0.0 Mobile/15E148 Safari/604.1",           

    # Firefox ‑ Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) "
    "Gecko/20100101 Firefox/128.0",                        

    # Firefox ‑ Linux
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) "
    "Gecko/20100101 Firefox/128.0",                         

    # Safari 18 ‑ macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/18.0 Safari/605.1.15",                        

    # Edge 126 ‑ Windows (clean)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",          

    # Edge 126 ‑ Windows (full string)
    "mozilla/5.0 (windows nt 10.0; win64; x64) "
    "applewebkit/537.36 (khtml, like gecko) "
    "chrome/126.0.0.0 safari/537.36 edg/126.0.0.0 "
    "gls/100.10.9252.92",                                  

    # Opera 120 ‑ Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36 OPR/120.0.5543.53",     

    # Brave 134 ‑ Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36 Brave/134.0.0.1"        
]

def get_random_header():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,"
                  "application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice([
            "en-US,en;q=0.9", "en-US;q=0.8"
        ]),
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Referer": "https://www.racenet.com.au/",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1"
    }


def fetch_slugs(date):
    conn = sqlite3.connect("raw_racing_data.db")
    cursor = conn.cursor()
    cursor.execute("""
                    SELECT 
                        meetings.meeting_id AS meeting_id, 
                        races.race_id AS race_id,
                        meetings.date_utc AS date,
                        meetings.slug AS meeting_slug, 
                        races.slug AS race_slug 
                    FROM races
                    LEFT JOIN meetings ON races.meeting_id = meetings.meeting_id
                    WHERE date = (?)
                """, (date,))
    rows = cursor.fetchall()
    conn.close()
    
    return rows


def fetch_soup(url, max_retries=5):
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=get_random_header())
            ok = resp.status_code == 200


            print(f"Attempt {attempt}/{max_retries} → HTTP {resp.status_code}")

            if not ok:
                raise ValueError(f"status {resp.status_code}")

            return BeautifulSoup(resp.content, "lxml")
        
        except Exception as e:
            if attempt == max_retries:
                raise
            
            # 2 minutes * attempt number + random jitter [2, 4, 6, 8, 10]
            delay = (120 * attempt) + random.uniform(0, 30)
            print(f"Error on {url}")
            time.sleep(delay)

    
def process_race_details(soup: BeautifulSoup):
    fields = [                      
        "Prize",
        "1st",
        "2nd",
        "3rd",
        "Time",
        "Sectional Time",
        "Track Info",
    ]
    info = {key: None for key in fields}
    
    for row in soup.select(".event-header__expand-column-row"):
        # extract label text and clean it
        label_tag = row.select_one(".header")
        if not label_tag:
            continue
            
        label = label_tag.get_text(strip=True).rstrip(":")
        
        # skip unwanted labels
        if label not in info:
            continue
        
        # get all text after the label tag
        value = ''.join(row.find_all(string=True, recursive=False)).strip()
        
        # handle cases where value might be in subsequent tags
        if not value:
            value_tag = label_tag.find_next_sibling()
            if value_tag:
                value = value_tag.get_text(strip=True)
        
        info[label] = value if value else ""

    return info


def process_results(soup: BeautifulSoup) -> list[dict]:
    results = []
    
    for horse in soup.select(".selection-result"):
        # Extract basic horse info
        name_tag = horse.select_one(".selection-result__info-competitor-name a")
        name_text = name_tag.get_text(strip=True) if name_tag else ""
        
        # Split running number and name
        if '.' in name_text:
            running_number, name = name_text.split('.', 1)
            running_number = running_number.strip()
            name = name.strip()
        else:
            running_number = ""
            name = name_text
            
        # Extract position
        position_tag = horse.select_one(".selection-result__competitor-place")
        position = position_tag.get_text(strip=True) if position_tag else ""
        
        # Extract barrier
        barrier_tag = horse.select_one(".selection-result__info-barrier")
        barrier = barrier_tag.get_text(strip=True).strip('()') if barrier_tag else ""
        
        # Extract age and sex
        age_tag = horse.select_one(".selection-result__info-age")
        age = age_tag.get_text(strip=True) if age_tag else ""
        
        sex_tag = horse.select_one(".selection-result__info-sex")
        sex = sex_tag.get_text(strip=True).strip('()') if sex_tag else ""
        
        # Extract trainer and jockey
        trainer_tag = horse.select_one(".selection-result__info-trainer")
        trainer = trainer_tag.get_text(strip=True).replace("T:", "").strip() if trainer_tag else ""
        
        jockey_tag = horse.select_one(".selection-result__info-jockey")
        jockey = jockey_tag.get_text(strip=True).replace("J:", "").strip() if jockey_tag else ""
        
        # Extract weight
        weight_tag = horse.select_one(".selection-result__info-weight")
        weight = weight_tag.get_text(strip=True) if weight_tag else ""
        
        # Extract sire and dam
        sire_tag = horse.select_one(".selection-result__info-sire")
        if sire_tag:
            sire_text = sire_tag.get_text(strip=True)
            if ' x ' in sire_text:
                sire, dam = sire_text.split(' x ', 1)
            else:
                sire = sire_text
                dam = ""
        else:
            sire = dam = ""
        
        # Extract running positions and margins
        margin_table = horse.select(".selection-result__table.margin .selection-result__table-column")
        pos_400 = pos_800 = margin = ""
        
        for col in margin_table:
            header = col.select_one(".selection-result__table-column-header")
            if not header: continue
            
            header_text = header.get_text(strip=True)
            details = col.select_one(".selection-result__table-column-details")
            if not details: continue
            
            if "400" in header_text:
                pos_400 = details.get_text(strip=True)
            elif "800" in header_text:
                pos_800 = details.get_text(strip=True)
            elif "Margin" in header_text:
                margin = details.get_text(strip=True)
        
        # extract SP (Starting Price)
        sp = ""
        odds_table = horse.select(".selection-result__table.odds .selection-result__table-column")
        for col in odds_table:
            header = col.select_one(".selection-result__table-column-header")
            if header and header.get_text(strip=True) == "SP":
                details = col.select(".selection-result__table-column-details")
                if details:
                    sp = details[0].get_text(strip=True)  # first value is Win SP
        
        results.append({
            "Position": position,
            "RunningNumber": running_number,
            "Name": name,
            "Barrier": barrier,
            "Age": age,
            "Sex": sex,
            "Trainer": trainer,
            "Jockey": jockey,
            "Weight": weight,
            "Sire": sire,
            "Dam": dam,
            "400m": pos_400,
            "800m": pos_800,
            "Margin": margin,
            "SP": sp
        })
    
    return results


def process_overview(soup: BeautifulSoup) -> list[dict]:
    results = []
    
    for container in soup.select('.event-selection-row-container'):
        # Skip scratched horses
        if 'selection-scratched' in container.get('class', []):
            continue
            
        # Extract horse name
        name_tag = container.select_one('.horseracing-selection-details-name')
        name_text = name_tag.get_text(strip=True) if name_tag else ""
        if '.' in name_text:
            name = name_text.split('.', 1)[1].strip()
        else:
            name = name_text

        # Extract form letters
        form_tag = container.select_one('.form-letters')
        form_letters = form_tag.get_text(strip=True) if form_tag else ""

        # Extract rating
        rating_tag = container.select_one('.event-selection-row-right__column--rating')
        rating = rating_tag.get_text(strip=True) if rating_tag else ""

        # Extract last race
        last_race_tag = container.select_one('.event-selection-row-right__column--lastRace')
        last_race = last_race_tag.get_text(strip=True) if last_race_tag else ""
        
        # Best Win odds
        best_win_tag = container.select_one('.odds-link__odds')
        best_win = best_win_tag.get_text(strip=True) if best_win_tag else ""

        results.append({
            'Name': name,
            'FormLetters': form_letters,
            'Rating': rating,
            'LastRace': last_race,
            'BestWin': best_win
        })
    
    return results


def process_form(soup: BeautifulSoup) -> list[dict]:
    """Scrape the career/track stats table for every horse."""
    results = []
    target_labels = [
        "Career", "Last 10", "Prize", "Avg Earn", "Last Win",
        "Win %", "Place %", "T/J Win %", "J/H",
        "12 Month", "Season", "Track", "Distance", "Track/Dist",
        "Firm", "Good", "Soft", "Heavy", "Wet",
        "1st Up", "2nd Up", "3rd Up", "Class",
        "Group 1", "Group 2", "Group 3", "Listed",
        "Clockwise", "A-Clockwise", "Night", "Synthetic",
        "As Fav", "ROI $",
    ]

    for block in soup.select(".form-guide-full-form__selection"):
        # ignore scratched runners
        if block.select_one(".selection-details--scratched"):
            continue

        # horse name (strip running number “1.” etc.)
        name_tag = block.select_one(".selection-details__name strong")
        raw_name = name_tag.get_text(strip=True) if name_tag else ""
        name = raw_name.split(".", 1)[1].strip() if "." in raw_name else raw_name

        # build a {label: value} map from the grid
        stats = {}
        for box in block.select(".form-grid-box"):
            header = box.select_one(".form-grid-box__header")
            detail = box.select_one(".form-grid-box__details")
            if header and detail:
                label = header.get_text(strip=True)
                stats[label] = detail.get_text(strip=True)

        # assemble one neat row, defaulting to ""
        row = {"Name": name}
        for label in target_labels:
            row[label] = stats.get(label, "") 

        results.append(row)

    return results


def extract_and_load_race(slug: str, meeting_id: str, race_id: str):
    # build urls 
    result_url   = f"https://www.racenet.com.au/results/horse-racing/{slug}"
    overview_url = f"https://www.racenet.com.au/form-guide/horse-racing/{slug}/overview"
    form_url     = f"https://www.racenet.com.au/form-guide/horse-racing/{slug}/full-form"

    # scrape pages (process inbetween requests -> time taken to process adds to the sleep time per request)
    race_soup   = fetch_soup(result_url)
    race_details = process_race_details(race_soup)
    race_results = process_results(race_soup)
    time.sleep(random.uniform(0, 1))
    
    overview_soup = fetch_soup(overview_url)
    overview = process_overview(overview_soup)
    time.sleep(random.uniform(0, 1))
    
    form_soup = fetch_soup(form_url)
    form_stats = process_form(form_soup)
    time.sleep(random.uniform(0, 1))

    # merge overview + form into dicts keyed by horse name
    overview_map = {row["Name"]: row for row in overview}
    form_map     = {row["Name"]: row for row in form_stats}

    combined = []
    for row in race_results:
        name = row["Name"]
        merged = {**row,
                  **overview_map.get(name, {}),
                  **form_map.get(name, {})}
        combined.append(merged)
        
    # loading data
    conn   = sqlite3.connect("raw_racing_data.db")
    cursor = conn.cursor()

    # race_details table  (one row per race)
    cursor.execute("""
        INSERT OR IGNORE INTO race_details (
            race_id, total_prize, first_prize, second_prize, third_prize,
            winning_time, sectional_time, track_rail_info
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        race_id,
        race_details.get("Prize", ""),
        race_details.get("1st", ""),
        race_details.get("2nd", ""),
        race_details.get("3rd", ""),
        race_details.get("Time", ""),
        race_details.get("Sectional Time", ""),
        race_details.get("Track Info", "")
    ))
    
    # horse_results table  (one row per runner)
    insert_sql = """
        INSERT OR REPLACE INTO horse_results (
            meeting_id, race_id, finish_position, running_number, name,
            barrier, age, sex, trainer, jockey, weight, sire, dam,
            position_400m, position_800m, margin, sp,
            form_letters, rating, last_race, best_win,
            career, last_10, prize, avg_earn, last_win,
            win_percent, place_percent, tj_win_percent, jh,
            twelve_month, season, track, distance, track_dist,
            firm, good, soft, heavy, wet,
            first_up, second_up, third_up, class,
            group1, group2, group3, listed,
            clockwise, a_clockwise, night, synthetic,
            as_fav, roi
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    for h in combined:
        cursor.execute(insert_sql, (
            meeting_id, race_id,
            h.get("Position", ""),
            h.get("RunningNumber", ""),
            h.get("Name", ""),
            h.get("Barrier", ""),
            h.get("Age", ""),
            h.get("Sex", ""),
            h.get("Trainer", ""),
            h.get("Jockey", ""),
            h.get("Weight", ""),
            h.get("Sire", ""),
            h.get("Dam", ""),
            h.get("400m", ""),
            h.get("800m", ""),
            h.get("Margin", ""),
            h.get("SP", ""),
            h.get("FormLetters", ""),
            h.get("Rating", ""),
            h.get("LastRace", ""),
            h.get("BestWin", ""),
            h.get("Career", ""),
            h.get("Last 10", ""),
            h.get("Prize", ""),
            h.get("Avg Earn", ""),
            h.get("Last Win", ""),
            h.get("Win %", ""),
            h.get("Place %", ""),
            h.get("T/J Win %", ""),
            h.get("J/H", ""),
            h.get("12 Month", ""),
            h.get("Season", ""),
            h.get("Track", ""),
            h.get("Distance", ""),
            h.get("Track/Dist", ""),
            h.get("Firm", ""),
            h.get("Good", ""),
            h.get("Soft", ""),
            h.get("Heavy", ""),
            h.get("Wet", ""),
            h.get("1st Up", ""),
            h.get("2nd Up", ""),
            h.get("3rd Up", ""),
            h.get("Class", ""),
            h.get("Group 1", ""),
            h.get("Group 2", ""),
            h.get("Group 3", ""),
            h.get("Listed", ""),
            h.get("Clockwise", ""),
            h.get("A-Clockwise", ""),
            h.get("Night", ""),
            h.get("Synthetic", ""),
            h.get("As Fav", ""),
            h.get("ROI $", "")
        ))

    conn.commit()
    conn.close()
    print(f"Loaded race {race_id} ({slug})")
    print()


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def main(start_date=None, end_date="2025-06-30"):
    if not start_date:
        start_date = input("Enter a start date (YYYY-MM-DD) : ")
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    for single_date in daterange(start, end):
        date_str = single_date.strftime("%Y-%m-%d")
        
        races = fetch_slugs(date_str)
        
        for meeting_id, race_id, date, meeting_slug, race_slug in races:
            slug = f"{meeting_slug}/{race_slug}"

            try:
                print(f"Processing: [{date}, {meeting_id}, {race_id}] {slug}")
                extract_and_load_race(slug, meeting_id, race_id)
            except Exception as e:
                error_message = f"Error on {slug}: {e}\n"
                print(error_message)

                # Save the error to a file
                with open("errors.txt", "a") as f:
                    f.write(error_message)
                
            
if __name__ == "__main__":
    main()

# start_date="2015-01-01", end_date="2025-06-30"