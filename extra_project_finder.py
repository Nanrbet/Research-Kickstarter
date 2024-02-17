import re
import time
from datetime import datetime
import json
import random
import logging
import os
import winsound
import threading
import sqlite3
import traceback

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

import pyautogui
from bs4 import BeautifulSoup

# Location of json with creator ids.
CREATOR_ID_PATH = r'D:\unscraped_creators_0.json'
# Location of json with already scraped project links.
EXISTING_LINKS_PATH = r'D:\kickstarter_existing_links.json' 
# Output file path.
OUTPUT_PATH = r"D:"
# Chromedriver path
CHROMEDRIVER_PATH = r"C:\Users\jaber\OneDrive\Desktop\Research_JaberChowdhury\Kickstarter-Data-Scraper\chromedriver.exe"
# Proton vpn windows taskbar location.
icon_num = 5 

# Number of threads per try.
chunk_size = 5
# Set logging. 
logging.getLogger('uc').setLevel(logging.ERROR)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p')
# Pyautogui settings.
pyautogui.PAUSE = 1
pyautogui.FAILSAFE = True

def main():
    global results, drivers

    results = []
    click_random(icon_num, False)
    logging.info("Creating driver instances...")
    drivers = [uc.Chrome(driver_executable_path=CHROMEDRIVER_PATH, headless=True) for i in range(chunk_size)]

    # Get connection to database file.
    con = create_project_db(OUTPUT_PATH)
    cur = con.cursor()

    # Get new creator ids.
    with open(CREATOR_ID_PATH, "r") as f_obj:
        new_creator_ids = json.load(f_obj)

    # Get already scraped projects.
    extracted = set(int(cid[0]) for cid in cur.execute("SELECT creator_id FROM projects;"))

    # Get deleted creators.
    deleted = set(int(cid[0]) for cid in cur.execute("SELECT creator_id FROM deleted_creators;"))
    
    skip = extracted | deleted
    creator_ids = [creator_id for creator_id in new_creator_ids if creator_id not in skip]

    total = 0
    for i in range(0, len(creator_ids), chunk_size):
        # Retry after changing server in case of errors.
        while True:
            results.clear()
            threads = []
            try:
                for j, creator_id in enumerate(creator_ids[i:i + chunk_size]):
                    thread = threading.Thread(target=extract_creator_data, args=(creator_id, j))
                    thread.start()
                    threads.append(thread)
                for thread in threads:
                    thread.join()
            except Exception:
                logging.info(f"\nException -\n {traceback.format_exc()} \nRetrying...")
                winsound.Beep(440, 1000)
                click_random(icon_num)
            else:
                break

        # Write results
        for creator_id, created_projects in results:
            if created_projects:
                cur.executemany("INSERT OR IGNORE INTO projects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [tuple(project.values()) for project in created_projects])
            else:
                cur.execute("INSERT OR IGNORE INTO deleted_creators VALUES (?)", (creator_id,))
        con.commit()                

        # Change server after scraping a certain amount to not be blocked by kickstarter. Also refresh drivers
        # to obfuscate bot detection.
        total += chunk_size
        if total % (chunk_size * 4) == 0:
            logging.info("Changing server...\n")
            click_random(icon_num, False)

            for driver in drivers:
                driver.quit()
            
            drivers = [uc.Chrome(driver_executable_path=CHROMEDRIVER_PATH, headless=True) for i in range(chunk_size)]

def create_project_db(path):
    """
    Creates projects.db in path and returns a connection.
    
    path[str] - Location to save/load 'projects.db'
    """
    con = sqlite3.connect(os.path.join(path, "projects.db"))
    cur = con.cursor()

    # Table for project data.
    cur.execute("""CREATE TABLE IF NOT EXISTS projects(
                name TEXT,
                url TEXT UNIQUE,
                creator_id TEXT,
                blurb TEXT,
                original_currency TEXT,
                converted_currency TEXT,
                conversion_rate REAL,
                goal REAL,
                pledged REAL,
                backers INTEGER,
                state TEXT,
                pwl INTEGER,
                location TEXT,
                subcategory TEXT,
                category TEXT,
                created_date TEXT,
                launched_date TEXT,
                deadline_date TEXT
                    )""")
    
    # Table for deleted creators.
    cur.execute("""CREATE TABLE IF NOT EXISTS deleted_creators(
                creator_id TEXT UNIQUE
    )
        """)

    # Table for previously extracted project urls.
    cur.execute("""CREATE TABLE IF NOT EXISTS previous_projects(
                url TEXT UNIQUE
                )""")
    
    with open(EXISTING_LINKS_PATH) as f_obj:
        new_creator_ids = json.load(f_obj)

    for creator_id in new_creator_ids:
        cur.execute("INSERT OR IGNORE INTO previous_projects VALUES (?)", (creator_id,))

    con.commit()
    return con

def click_random(icon_num, wait=True):
    """
    Clicks random button in proton vpn. Proton VPN needs
    to be open on the profiles page. 
    
    icon_num [int] - Index of proton vpn icon on the windows taskbar.
    wait [bool] - If True, function will sleep for 10s to make sure Proton Vpn
    connects and otherwise it will not sleep. True by default.
    """
    pyautogui.hotkey('win', str(icon_num))
    pyautogui.click(333, 563, clicks=3, interval=0.15)
    time.sleep(2)
    pyautogui.hotkey('alt', 'tab')
    if wait:
        time.sleep(10)

def get_digits(string, conv="float"):
    """
    Returns only digits from string as a single int/float. Default
    is float. Returns empty string if no digit found.

    Inputs: 
    string[str] - Any string.
    conv[str] - Enter "float" if you need float. Otherwise will provide integer. "float" by default.
    """
    if conv == "float":
        res = re.findall(r'[0-9.]+', string)
        if res == "":
            return ""
        return float("".join(res))
    else:
        res = re.findall(r'\d+', string)
        if res == "":
            return ""
        return int("".join(res))
    
def get_live_soup(link, scroll=False, given_driver=None):
    """
    Returns a bs4 soup object of the given link. Returns None if it is a deleted kickstarter account.
    
    link [str] - A link to a website.
    scroll [bool] - True if you want selenium to keep scrolling down till loading no longer happens.
    False by default.
    given_driver [selenium webdriver] - A webdriver. None by default.
    """
    if given_driver == None:
        driver = uc.Chrome(executable_path=CHROMEDRIVER_PATH, headless=True)
    else:
        driver = given_driver
    driver.get(link)

    soup = BeautifulSoup(driver.page_source, "lxml")

    # If there is a capcha, raise an exception.
    capcha_elem = soup.select_one('div[id="px-captcha"]')
    if capcha_elem != None:
        raise Exception("Captcha encountered.")
    
    # If it is a deleted account or there is a 404 error, return.
    deleted_elem = soup.select_one('div[class="center"]')
    non_existent_elem = soup.select_one('a[href="/?ref=404-ksr10"]')
    if deleted_elem != None or non_existent_elem != None:
        if given_driver == None:
            driver.quit()
        return
    
    if scroll:
        scroll_num = 1
        while True:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait to scroll. Notify if unusually high number of scrolls (which may mean that
            # there is a 403 error).
            if scroll_num % 60 == 0:
                winsound.Beep(440, 1000)

            if scroll_num % 30 == 0:
                time.sleep(30)
            else:
                time.sleep(random.uniform(1, 2))

            scroll_num += 1

            # Stop scrolling if no longer loading.
            try:
                elem = driver.find_element(By.CSS_SELECTOR, 'li[data-last_page="true"]')
            except:
                continue
            else:
                break

    soup = BeautifulSoup(driver.page_source, "lxml")

    if given_driver == None:
        driver.quit()

    return soup

def extract_elem_text(soup, selector):
    """
    Returns resulting text of using given selector in soup.
    If there was no text, returns empty string. 
    
    Inputs:
    soup[bs4.BeautifulSoup] - A soup.
    selector[str] - A css selector.
    """
    elem = soup.select_one(selector)
    if elem == None:
        return ""
    else:
        return elem.getText()

def parse_data_project(data_project):
    """
    Parses a kickstarter data project dictionary and returns a dictionary of
    required keys.
    
    data-project [dict]- A kickstarter data project dict.
    """
    result = {}

    result['name'] = data_project['name']
    url = data_project['urls']['web']['project']

    result['url'] = url
    result['creator_id'] = data_project['creator']['id']
    result['blurb'] = data_project['blurb']

    # Convert currencies to USD.
    result['original_currency'] = data_project['currency']
    result['converted_currency'] = 'USD'
    result['conversion_rate'] = data_project['static_usd_rate']
    result['goal'] = data_project['goal'] * data_project['static_usd_rate']
    result['pledged'] = data_project['usd_pledged']

    result['backers'] = data_project['backers_count']
    result['state'] = data_project['state'].title()
    result['pwl'] = int(data_project['staff_pick'])
    result['location'] = data_project.get('location', {}).get('short_name', "")

    if 'parent_name' in data_project['category']:
        result['subcategory'] = data_project['category']['name']
        result['category'] = data_project['category']['parent_name']
    else:
        result['subcategory'] = ""
        result['category'] = data_project['category']['name']

    result['created_date'] = datetime.fromtimestamp(data_project['created_at']).strftime('%Y-%m-%d')
    result['launched_date'] = datetime.fromtimestamp(data_project['launched_at']).strftime('%Y-%m-%d')
    result['deadline_date'] = datetime.fromtimestamp(data_project['deadline']).strftime('%Y-%m-%d')

    return result

def extract_creator_data(creator_id, index=None):
    """
    Returns a dictionary of the data for the creator. Returns None in case of a deleted account.
    Can load a webdriver from the given index from the global list drivers (optional).
    
    creator_id [str/int] - A kickstarter creator id.
    index [int] - Index of webdriver in drivers for this function call.
    """
    logging.info(f"Started extracting {creator_id} data...")
    path = r"https://www.kickstarter.com/profile/" + str(creator_id)

    if index == None:
        driver = uc.Chrome(executable_path=CHROMEDRIVER_PATH, headless=True)
    else:
        driver = drivers[index]
    # Extract data from available pages. There may be multiple pages for created projects.

    try:
        created_soup = get_live_soup(path + "/created", given_driver=driver)
    except Exception as e:
        if index == None:
            driver.quit()
        raise e

    if created_soup == None:
        return (creator_id, [])
    
    created_soups = [created_soup]
    while True:
        next_elem = created_soup.select_one('a[rel="next"]')

        # No further pages.
        if next_elem == None:
            break   
        
        created_soup = get_live_soup("https://www.kickstarter.com/" + next_elem['href'], given_driver=driver)
        created_soups.append(created_soup)

    if index == None:
        driver.quit()          

    # Created projects.
    created_data_projects = []
    for created_soup in created_soups:
        created_project_elem = created_soup.select_one('div[data-projects]')
        created_data_projects.extend(json.loads(created_project_elem['data-projects']))

    created_projects = []
    for created_data_project in created_data_projects:
        parsed = parse_data_project(created_data_project)
        if parsed != None:
            created_projects.append(parsed)

    results.append((creator_id, created_projects))

if __name__ == "__main__":
    main()
