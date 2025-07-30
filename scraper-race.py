import requests
from bs4 import BeautifulSoup
import sqlite3
import random

URL = "https://www.racenet.com.au/form-guide/horse-racing/ballarat-20221009/ajm-contracting-maiden-plate-race-1/odds-comparison"

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
                        meetings.meeting_id AS meeting_id1, 
                        races.meeting_id AS meeting_id2, 
                        races.race_id AS race_id,
                        meetings.date_utc AS date,
                        meetings.slug AS meeting_slug, 
                        races.slug AS race_slug 
                    FROM races
                    LEFT JOIN meetings ON races.meeting_id = meetings.meeting_id
                    WHERE date = (?)
                """, (date,))
    rows = cursor.fetchall()
    complete_slugs = [f"{row[4]}/{row[5]}" for row in rows]
    conn.close()
    
    return complete_slugs


fetch_slugs("2024-11-01")
    
    
    
html = requests.get(URL, headers=get_random_header()).text

soup = BeautifulSoup(html, "lxml")
print(soup.prettify()[:500])
print(html.status_code)