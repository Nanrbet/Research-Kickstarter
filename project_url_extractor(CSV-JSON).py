from multiprocessing import Pool, Manager
from datetime import datetime
import re
import logging
import time
import json

from pyautogui import click
import psutil
import sqlite3
import os
import csv
import random
import traceback
from urllib3.exceptions import MaxRetryError

from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from seleniumbase import Driver
import pyautogui
from bs4 import BeautifulSoup
import pandas as pd

# Settings.

# Path to project data. Make sure to use raw strings or escape "\".
# DATA_PATH = r"Kickstarter.csv"
DATA_PATH = r"Kickstarter_Kickstarter.json"
# Output path.
OUTPUT_PATH = r""
DATABASE = os.path.join(OUTPUT_PATH, "new_projects.db")
# JSON_URL_PATH = r"Extracted_project_urls.csv"
# Set logging.
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO,
                    datefmt='%m/%d/%Y %I:%M:%S %p')
# Set what value to enter in case of missinaskg data. Default is ""
MISSING = ""
VALID_STATUSES = {"live", "successful", "failed"}
# Set to True if Testing and False otherwise.
TESTING = 0
# Set to True to use OpenVPN and False to not
IP_FLAG = False
# Number of urls to extract per session and processes per try.
chunk_size = 3          # should be a MULTIPLE of process_size
process_size = 1
# Proton vpn windows taskbar location.
icon_num = 1
last_read_row = 0  # keeps track of last row to update it in main
initial_row = 0
global_driver = None
pyautogui.FAILSAFE = False


# Script.

# Lock to prevent multiple processes from trying to access database or changing IP.
# Initialize a lock object from Manager

def main():
    global last_read_row
    # remove any zombie chrome process
    with Manager() as manager:
        db_lock = manager.Lock()
        if DATA_PATH.lower().endswith('.json'):
            reader = export_filtered_projects()
        elif DATA_PATH.lower().endswith('.csv'):
            reader = reset_reader()
        else:
            raise ValueError("Error in main: Unsupported file extension. Please provide a .json or .csv file.")
        pool = Pool(processes=process_size) if process_size else Pool()

        total = 0
        Done = False
        while not Done:
            # Get at maximum chunk_size number rows as a list per iteration.
            rows_to_process = [
                (db_lock, row) for row in get_rows(reader, DATABASE, chunk_size,
                                                    start_line=get_last_read_line())]  # Adjusted for starmap
            try:
                pool.starmap(scrape_write, rows_to_process)
            except Exception:
                # Handle other exceptions not caught when extracting
                logging.info(f"\nException -mainException\n {traceback.format_exc()} \nRetrying...")
                # Update last read line so unscraped rows_to_process will get added in next iteration.
                save_last_read_line(max(last_read_row-chunk_size,0))

            # Scraping complete since there aren't enough rows_to_process left to reach chunk_size.
            if len(rows_to_process) == 0:
                Done = True

            # Stop scraping for a period of time to not be blocked as a bot.
            total += chunk_size
            if total % (chunk_size * 3) == 0:
                logging.info("Changing server...\n")
                time.sleep(10)
            # Close the pool after each iteration
            pool.close()
            pool.join()
            save_last_read_line(last_read_row)
            pool = Pool(processes=process_size) if process_size else Pool()

        pool.close()
        pool.join()


def get_rows(reader, database, chunk, start_line=0):
    """Returns n rows from csv reader while making sure they weren't already scraped by checking in the database."""
    global last_read_row
    rows = []
    line_num = start_line
    # Connect to the database
    con = get_projects_db(database)
    cur = con.cursor()
    # Read from the list starting from the specified line using a while loop
    while len(rows) < chunk and line_num < len(reader):
        row = reader[line_num]
        if DATA_PATH.lower().endswith('.json'):
            row_url = row['urls']['web']['project'].strip()
        elif DATA_PATH.lower().endswith('.csv'):
            row_url = json.loads(row['urls'])['web']['project'].strip()

        # Modified query to search if url is in the database tables
        cur.execute(
            """SELECT SUM(count_projects + count_ignored_projects + count_hidden_projects) AS total_hidden_projects FROM 
                (SELECT COUNT(*) AS count_projects, 0 AS count_ignored_projects, 0 AS count_hidden_projects FROM projects WHERE rd_project_link = ? UNION
                SELECT 0 AS count_projects, count(*) AS count_ignored_projects, 0 AS count_hidden_projects FROM ignored_projects WHERE rd_project_link =? UNION
                SELECT 0 AS count_projects, 0 AS count_ignored_projects, count(*) AS count_hidden_projects FROM hidden_projects WHERE url =?)""",
            (row_url, row_url, row_url))
        result = cur.fetchone()
        total_count = result[0]

        if total_count == 0: # url is in no table
            rows.append(row)
        line_num += 1
    con.close()
    # Saving the global last read row. To be updated if all processes are done with extraction
    last_read_row = line_num     
    return rows


def close_all_chrome_processes():
    """Closes all Chrome processes."""
    for proc in psutil.process_iter():
        if 'chrome' in proc.name().lower():
            try:
                proc.terminate()  # Use terminate() to end the process gently
                proc.wait(timeout=3)  # Wait up to 3 seconds for process to close
            except Exception:
                continue


def get_last_read_line(filename='last_read_line.txt'):
    try:
        with open(filename, 'r') as file:
            return int(file.read().strip())
    except FileNotFoundError:
        return 0


def test_extract_campaign_data():
    # Testing code.
    file_paths = [
        # "https://www.kickstarter.com/projects/petersand/manylabs-sensors-for-students",
        "https://www.kickstarter.com/projects/2059287567/rainbow-bird",
        "https://www.kickstarter.com/projects/michaeljkospiah/kung-fubar?ref=discovery_category_newest",
        "https://www.kickstarter.com/projects/weird24seven/gemology",
        "https://www.kickstarter.com/projects/maris/25-cent-peep-show",
        "https://www.kickstarter.com/projects/510660030/case2",
        "https://www.kickstarter.com/projects/1765832443/primal-love-a-short-film-by-mina-mohaddess",
        "https://www.kickstarter.com/projects/utopiaman/immortality-a-true-story"
    ]
    with Manager() as manager:
        db_lock = manager.Lock()
        data = extract_campaign_data(db_lock, file_paths[0])
        # pool.close()
        # pool.join()
        # if data:
        #     df = pd.DataFrame(data, index=None)
        #     df.to_csv('test.csv', index=False)
    print(data)
            
def reset_reader():
    f_obj = open(DATA_PATH, encoding="utf8", newline='')
    # Skip to the desired row
    for _ in range(initial_row):
        next(f_obj)
    # Save the initial row as the last read line
    save_last_read_line(initial_row)
    # Create and return the csv.DictReader object
    reader = csv.DictReader(f_obj)
    # Filter rows based on the state
    filtered_rows = [row for row in reader if row.get("state") in VALID_STATUSES]
    
    return filtered_rows

def export_filtered_projects():
    all_projects = []

    with open(DATA_PATH, encoding="utf8", newline='') as f_obj:
        # Read the first character to determine the format
        first_char = f_obj.read(1)
        f_obj.seek(0)  # Reset the file pointer to the beginning
        if first_char == '[': # Handle JSON array format
            try:
                # Read and clean the file content in one step
                clean_content = f_obj.read().translate(str.maketrans('', '', '\n\r'))
                json_array = json.loads(clean_content)  # Parse the string as a JSON list
                # json_array = json.load(f_obj.read())
                for json_obj in json_array:
                    projects = json_obj.get('projects', [])
                    for project in projects:
                        if project.get("state") in VALID_STATUSES:
                            all_projects.append(project)
            except json.JSONDecodeError as e:
                print(f"Error in export_filtered_projects loading JSON list: {e}")
        elif first_char == '{':
            # Handle JSON object format
            try:
                for line in f_obj:
                    try:
                        json_obj = json.loads(line)
                        projects = json_obj.get('data', {}).get('projects', [])
                        for project in projects:
                            if project.get("state") in VALID_STATUSES:
                                all_projects.append(project)
                    except json.JSONDecodeError as e:
                        print(f"Error in export_filtered_projects loading JSON object: {e}")
            except json.JSONDecodeError as e:
                print(f"Error in export_filtered_projects loading JSON dict: {e}")
        else:
            print("Unknown JSON format")
    # Save the filtered projects to a CSV file TODO: remove or delete
    # keys = all_projects[0].keys()
    # with open(JSON_URL_PATH, 'w', newline='', encoding='utf8') as output_file:
    #     dict_writer = csv.DictWriter(output_file, fieldnames=keys)
    #     dict_writer.writeheader()
    #     dict_writer.writerows(all_projects)
    return all_projects

def save_last_read_line(last_read_line, filename='last_read_line.txt'):
    with open(filename, 'w') as file:
        file.write(str(last_read_line))


def click_random(icon_num):
    """
    Clicks random button in proton vpn. Proton VPN needs
    to be open on the profiles page.

    icon_num [int] - Index of proton vpn icon on the windows taskbar.
    wait [bool] - If True, function will sleep for 10s to make sure Proton Vpn
    connects and otherwise it will not sleep. True by default.
    """
    # changes the vpn only if the IP_FLAG is true
    if IP_FLAG:
        # Attempt to change IP
        pyautogui.hotkey('win', str(icon_num))
        time.sleep(2)
        pyautogui.click(1052, 430, clicks=2, interval=0.5)
        time.sleep(7)
        pyautogui.click(1060, 590, clicks=1)  # Attempt to reconnect/change IP
        pyautogui.hotkey('alt', 'tab')


def get_projects_db(database):
    """
    Creates database if it doesn't exist and returns a connection.

    path[str] - Location to save/load database
    """
    con = sqlite3.connect(database)
    # Enable autocommit
    con.isolation_level = None
    cur = con.cursor()

    # Table for projects data.
    table_creation_sql = """
    CREATE TABLE IF NOT EXISTS projects (
        time_interval TEXT, 
        date_accessed TEXT, 
        rd_project_link TEXT UNIQUE, 
        project_id TEXT, 
        creator_id TEXT, 
        title TEXT, 
        rd_creator_name TEXT, 
        blurb TEXT, 
        verified_identity TEXT, 
        status TEXT, 
        cv_duration TEXT, 
        cv_num_backers TEXT, 
        collaborators TEXT, 
        original_curr_symbol TEXT, 
        converted_curr_symbol TEXT, 
        conversion_rate FLOAT, 
        goal FLOAT, 
        converted_goal FLOAT, 
        pledged FLOAT, 
        converted_pledged FLOAT, 
        cv_startday TEXT, 
        cv_startmonth TEXT, 
        cv_startyear TEXT, 
        cv_endday BIGINT, 
        cv_endmonth BIGINT, 
        cv_endyear BIGINT, 
        num_photos BIGINT, 
        num_videos BIGINT, 
        pwl FLOAT, 
        make100 TEXT, 
        category TEXT, 
        subcategory TEXT, 
        location TEXT, 
        rd_creator_created TEXT, 
        num_backed TEXT, 
        rd_comments BIGINT, 
        rd_updates BIGINT, 
        rd_faqs BIGINT, 
        description TEXT, 
        risk TEXT, 
        cv_num_rewards BIGINT,
        """

    # Add columns for pledges.
    max_pledge_num = 126
    for i in range(0, max_pledge_num + 1):
        table_creation_sql += f"""rd_id_{i} TEXT, 
                                rd_title_{i} TEXT, 
                                rd_price_{i} TEXT, 
                                rd_desc_{i} TEXT, 
                                rd_list_{i} TEXT, 
                                rd_delivery_date_{i} TEXT, 
                                rd_shipping_location_{i} TEXT, 
                                rd_backers_{i} TEXT, 
                                rd_limit_{i} TEXT, 
                                rd_gone_{i} TEXT,"""
    # Replace last "," to prevent sql error and also close command.
    table_creation_sql = table_creation_sql[:-1] + "\n)"
    cur.execute(table_creation_sql)

    # Table for projects data.
    table_creation_sql = """
    CREATE TABLE IF NOT EXISTS ignored_projects (time_interval TEXT, date_accessed TEXT, rd_project_link TEXT UNIQUE, 
    project_id TEXT, creator_id TEXT, title TEXT, rd_creator_name TEXT, blurb TEXT, verified_identity TEXT, status TEXT, 
    cv_duration TEXT, cv_num_backers TEXT, collaborators TEXT, original_curr_symbol TEXT, converted_curr_symbol TEXT, 
    conversion_rate FLOAT, goal FLOAT, converted_goal FLOAT, pledged FLOAT, converted_pledged FLOAT, cv_startday TEXT, 
    cv_startmonth TEXT, cv_startyear TEXT, cv_endday BIGINT, cv_endmonth BIGINT, cv_endyear BIGINT, num_photos BIGINT, 
    num_videos BIGINT, pwl FLOAT, make100 TEXT, category TEXT, subcategory TEXT, location TEXT, rd_creator_created TEXT, 
    num_backed TEXT, rd_comments BIGINT, rd_updates BIGINT, rd_faqs BIGINT, description TEXT, risk TEXT, cv_num_rewards BIGINT,
    """

    # Add columns for pledges.
    max_pledge_num = 126
    for i in range(0, max_pledge_num + 1):
        table_creation_sql += f"""rd_id_{i} TEXT, rd_title_{i} TEXT, rd_price_{i} TEXT, rd_desc_{i} TEXT, rd_list_{i} TEXT, 
        rd_delivery_date_{i} TEXT, rd_shipping_location_{i} TEXT, rd_backers_{i} TEXT, rd_limit_{i} TEXT, rd_gone_{i} TEXT,"""
    # Replace last "," to prevent sql error and also close command.
    table_creation_sql = table_creation_sql[:-1] + "\n)"
    cur.execute(table_creation_sql)

    cur.execute("""CREATE TABLE IF NOT EXISTS hidden_projects(
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
    con.commit()
    return con


def get_str(string, extra):
    """Returns a string without any digits.

    Inputs:
    string [str] - Any string.
    extra [set] - Extra set of characters to exclude."""
    return "".join([char for char in string if not (char.isdigit() or char in extra)]).strip()


def get_digits(string, conv="float"):
    """Returns only digits from string as a single int/float. Default
    is float. Returns None if no digits are found.

    Inputs:
    string[str] - Any string.
    conv[str] - Enter "float" if you need float. Otherwise will provide integer."""
    if conv == "float":
        res = re.findall(r'[0-9.]+', string)
        converter = float
    else:
        res = re.findall(r'\d+', string)
        converter = int

    if res != []:
        return converter("".join(res))
    else:
        return None


def get_pledge_data(bs4_tag, index=0, conversion_rate=1):
    """Returns a dict of data from a kickstarter pledge li bs4 tag.
    Dict will contain:
    rd_id: Pledge unique id.
    rd_title: Pledge title
    rd_price: Pledge price
    rd_desc: Pledge description
    rd_list: A list of rewards from pledge description. Empty string if no list given.
    rd_delivery_date: Pledge Estimated Delivery. Format YYYY-MM-DD.
    rd_shipping_location: Pledge Shipping location. Empty string if no shipping location.
    rd_backers: Total number of backers.
    rd_limit: Limit in number of backers of pledge. Empty string if there are no
    limits. For unavailable pledges, limit = backers because they don't show limits.
    rd_gone: Status of pledge. If it is no longer available has a value of 1 and otherwise 0.

    Inputs:
    bs4_tag [bs4.element.Tag] - A tag of a kickstarter Pledge.
    Index [int] - Optional. The index of the current pledge. Has a default value of 0.
    conversion_rate [int] - Conversion rate to use for converting pledge price. 1 by default."""
    pledge_data = {}
    i = str(index)

    pledge_data['rd_id_' + i] = bs4_tag['id']
    pledge_data['rd_title_' + i] = bs4_tag.select_one(
        '[class="support-700 semibold kds-heading type-18 m0 mr1 text-wrap-balance break-word"]').getText().strip()

    pledge_data['rd_price_' + i] = get_digits(bs4_tag.select_one('[class="support-700 type-18 m0 shrink0"]').getText(),
                                              "int") * float(conversion_rate)

    # Description may not exist for some pledges e.g. https://www.kickstarter.com/projects/davidgfores/tiny-creatures-alphabet-el-abc-de-las-criaturas-abominables/rewards
    desc_elem = bs4_tag.select_one('[class="type-14 lh20px mb0 support-700 text-prewrap"]')
    if desc_elem != None:
        pledge_data['rd_desc_' + i] = desc_elem.getText()
    else:
        pledge_data['rd_desc_' + i] = ""

    # Get container with included items for pledge and then extract text from
    # every item.
    rd_list = []
    item_list_elem = bs4_tag.select_one('[class="flex flex-column justify-between gap7"]')
    # No included items. e.g. https://www.kickstarter.com/projects/lucid-dreamers/empires-of-sorcery/rewards
    if item_list_elem != None:
        item_elems = item_list_elem.select(
            '[class="block ml-0 z3 border border2px border-white radius100p shadow-reward-avatar"]')
        for item_elem in item_elems:
            item = item_elem.getText()
            if "Quantity: 1" in item:
                item = item.replace("Quantity: 1", "")
            else:
                # In case of quantity > 1, move quantity to the front.
                rem_str = "Quantity: "
                rem_ind = item.find(rem_str)
                item = item[rem_ind + len(rem_str):] + " " + item[:rem_ind]

            rd_list.append(item)

    pledge_data['rd_list_' + i] = json.dumps(rd_list)

    delivery_date_elem = bs4_tag.select_one('time[datetime]')
    if delivery_date_elem != None:
        pledge_data['rd_delivery_date_' + i] = bs4_tag.select_one('time[datetime]')['datetime']
    else:
        pledge_data['rd_delivery_date_' + i] = MISSING

    shipping_location_elem = bs4_tag.select_one('div[class="flex1"] > div[class="type-14 lh20px mb0 support-700"]')
    if shipping_location_elem != None:
        pledge_data['rd_shipping_location_' + i] = shipping_location_elem.getText()
    else:
        pledge_data['rd_shipping_location_' + i] = MISSING

    rd_backers_elem = bs4_tag.select_one("span[aria-label]")
    if rd_backers_elem != None:
        rd_backers = int(rd_backers_elem.getText())
    else:
        rd_backers = MISSING
    pledge_data["rd_backers_" + i] = rd_backers

    # Check if h3 tag with text "Limited quantity" exists. If so, get sibling
    # tag which contains the reward limit.
    rd_limit_sib_elem = bs4_tag.select_one('h3:-soup-contains("Limited quantity")')
    if rd_limit_sib_elem != None:
        rd_limit_digits = get_digits(rd_limit_sib_elem.find_next_sibling().getText().split()[-1], "int")
        # Must have "None left" as value since get_digits failed. E.g. of reward page: https://www.kickstarter.com/projects/artorder/2018-snowman-greeting-card-collection/rewards
        if rd_limit_digits == None:
            rd_limit = rd_backers
        else:
            rd_limit = rd_limit_digits
    else:
        rd_limit = MISSING
    pledge_data["rd_limit_" + i] = rd_limit

    pledge_data["rd_gone_" + i] = int(rd_limit == rd_backers)

    return pledge_data


def get_category_data(cat_str):
    """Returns a tuple of (category, subcategory) from a given cat_str which
    can be either a category or subcategory.

    Inputs:
    cat_str = A string which is either a category or subcategory."""
    categories = {
        'Art': {'Ceramics', 'Conceptual Art', 'Digital Art', 'Illustration', 'Installations', 'Mixed Media', 'Painting',
                'Performance Art', 'Public Art', 'Sculpture', 'Social Practice', 'Textiles', 'Video Art'},
        'Comics': {'Anthologies', 'Comic Books', 'Events', 'Graphic Novels', 'Webcomics'},
        'Crafts': {'Candles', 'Crochet', 'DIY', 'Embroidery', 'Glass', 'Knitting', 'Pottery', 'Printing', 'Quilts',
                   'Stationery', 'Taxidermy', 'Weaving', 'Woodworking'},
        'Dance': {'Performances', 'Residencies', 'Spaces', 'Workshops'},
        'Design': {'Architecture', 'Civic Design', 'Graphic Design', 'Interactive Design', 'Product Design', 'Toys',
                   'Typography'},
        'Fashion': {'Accessories', 'Apparel', 'Childrenswear', 'Couture', 'Footwear', 'Jewelry', 'Pet Fashion',
                    'Ready-to-wear'},
        'Film & Video': {'Action', 'Animation', 'Comedy', 'Documentary', 'Drama', 'Experimental', 'Family', 'Fantasy',
                         'Festivals', 'Horror', 'Movie Theaters', 'Music Videos', 'Narrative Film', 'Romance',
                         'Science Fiction', 'Shorts', 'Television', 'Thrillers', 'Webseries'},
        'Food': {'Bacon', 'Community Gardens', 'Cookbooks', 'Drinks', 'Events', "Farmer's Markets", 'Farms',
                 'Food Trucks', 'Restaurants', 'Small Batch', 'Spaces', 'Vegan'},
        'Games': {'Gaming Hardware', 'Live Games', 'Mobile Games', 'Playing Cards', 'Puzzles', 'Tabletop Games',
                  'Video Games'},
        'Journalism': {'Audio', 'Photo', 'Print', 'Video', 'Web'},
        'Music': {'Blues', 'Chiptune', 'Classical Music', 'Comedy', 'Country & Folk', 'Electronic Music', 'Faith',
                  'Hip-Hop', 'Indie Rock', 'Jazz', 'Kids', 'Latin', 'Metal', 'Pop', 'Punk', 'R&B', 'Rock',
                  'World Music'},
        'Photography': {'Animals', 'Fine Art', 'Nature', 'People', 'Photobooks', 'Places'},
        'Publishing': {'Academic', 'Anthologies', 'Art Books', 'Calendars', "Children's Books", 'Comedy', 'Fiction',
                       'Letterpress', 'Literary Journals', 'Literary Spaces', 'Nonfiction', 'Periodicals', 'Poetry',
                       'Radio & Podcasts', 'Translations', 'Young Adult', 'Zines'},
        'Technology': {'3D Printing', 'Apps', 'Camera Equipment', 'DIY Electronics', 'Fabrication Tools', 'Flight',
                       'Gadgets', 'Hardware', 'Makerspaces', 'Robots', 'Software', 'Sound', 'Space Exploration',
                       'Wearables', 'Web'},
        'Theater': {'Comedy', 'Experimental', 'Festivals', 'Immersive', 'Musical', 'Plays', 'Spaces'}}

    category, subcategory = cat_str, MISSING

    # No way to know subcategory from category.
    if cat_str not in categories.keys():
        # Might be given a subcategory so try finding it's category.
        for category_name, subcategories in categories.items():
            if cat_str in subcategories:
                category = category_name
                subcategory = cat_str
                break
    return (category, subcategory)


def get_or_create_driver():
    global global_driver
    if global_driver is not None:
        # Quit the previous driver if it exists
        try:
            global_driver.quit()
        except Exception as e:
            print(f"Error quitting previous driver: {e}")

    # global_driver = Driver(undetectable=True, incognito=True, undetected=True)
    global_driver = Driver(undetectable=True, incognito=True, undetected=True, headless=True)
    return global_driver


class PageSourceAccessError(Exception):
    """Exception raised when the page source cannot be accessed after retries."""
    """Handling this error: 
    03/02/2024 08:01:43 AM - WARNING - Retrying (Retry(total=0, connect=None, read=None, redirect=None, status=None)) 
    after connection broken by 'NewConnectionError('<urllib3.connection.HTTPConnection object at 0x000002701D3C4850>: Failed to establish a new connection: 
    [WinError 10061] No connection could be made because the target machine actively refused it')': /session/dd2007a64a3cbe3284fe88bb5522d491/window/handles"""

    def __init__(self, message="Failed to access page source"):
        self.message = message
        super().__init__(self.message)


# function to get page source with retries
def safe_get_page_source():
    """Tries to get the page source, with retries on WebDriverException."""
    global global_driver
    try:
        return global_driver.page_source
    except (WebDriverException, MaxRetryError) as e:
        print(f"\nError accessing page source: {e}.")
        global_driver.quit()
        raise PageSourceAccessError("Failed to access page source after attempts.")


def handle_captcha(db_lock, link):
    """
    Handle captcha if present by beeping and sleeping for some time (seconds).
    Then, create a new WebDriver instance and navigate to the link again.
    Return the new WebDriver instance.
    """
    global global_driver
    db_lock.acquire()
    try:
        attempts = 0
        max_attempts = 5  # Example limit
        while attempts < max_attempts:
            global_driver.refresh()
            soup = BeautifulSoup(global_driver.page_source, "lxml")
            if soup.select_one('div[id="px-captcha"]') is not None or soup.select_one('h2[id="challenge-running"]') is not None:
                logging.info("CAPTCHA encountered. Attempting to bypass...")
                # winsound.Beep(440, 1000)  # Uncomment for an audible alert

                # Properly quit the previous driver instance
                if global_driver:
                    global_driver.quit()
                click_random(icon_num)  # Ensure this function is defined to interact with CAPTCHA

                global_driver = get_or_create_driver()
                global_driver.get(link)
            else:
                logging.info("Successfully bypassed CAPTCHA or none encountered.")
                break
            attempts += 1
    except Exception as e:
        logging.error(f"Error handling CAPTCHA: {e}")
    finally:
        db_lock.release()
    return global_driver


def get_live_soup(db_lock, link, page=None):
    """Returns a bs4 soup object of the given link. Returns None if it is a deleted kickstarter account.

    link [str] - A link to a website.
    scroll [bool] - True if you want selenium to keep scrolling down till loading no longer happens.
    False by default.
    given_driver [selenium webdriver] - A webdriver. None by default.
    page [str] - Additional behavior depending on page type."""
    global global_driver

    success = False  # Flag to indicate whether extraction was successful
    attempts, max_retries = 0, 10
    while not success and attempts < max_retries:
        global_driver = get_or_create_driver()
        try:
            page_source = safe_get_page_source()  # sometimes get error in retirveing the webpage so this handles it
            # checks for capcha and handles if the process before it has a capcha
            soup = BeautifulSoup(page_source, "lxml")
            if soup.select_one('div[id="px-captcha"]') != None or soup.select_one('h2[id="challenge-running"]') is not None:
                global_driver.refresh()
                time.sleep(5)

            global_driver.get(link)

            # checks for capcha and handles if the process before it has a capcha
            soup = BeautifulSoup(global_driver.page_source, "lxml")
            if soup.select_one('div[id="px-captcha"]') is not None or soup.select_one('h2[id="challenge-running"]') is not None:
                global_driver = handle_captcha(db_lock, link)

            # Hidden project. For e.g. https://www.kickstarter.com/projects/732431717/photo-time-machine
            hidden_elem = soup.select_one('div[id="hidden_project"]')
            if hidden_elem is not None:
                # Open "privacy_count.txt" in a mode that supports reading and writing,
                # and creates the file if it doesn't exist.
                with open("privacy_count.txt", "w+") as file:  # Open the file in read/write mode
                    content = file.read().strip()  # Read and strip the file's content to handle possible whitespace
                    count = int(content) if content else 0  # Convert to int, defaulting to 0 if the file is empty
                    count += 1  # Increment the count
                    file.seek(0)  # Move back to the start of the file before writing
                    file.write(str(count))  # Write the updated count as a string
                    file.truncate()  # Truncate any remaining data in the file (if the new number is shorter)
                return "HIDDEN_CAMPAIGN"

            # Click creator page for page to load additional data if it is a campaign page.
            # There are two possible alternate selectors. One for successful campaigns and the
            # other for other campaigns. Try finding both and click whichever that exists.
            if page == "campaign":
                # Try reloading page at most 2 times if required elems aren't found.
                tries = 2
                while tries != 0:
                    elems = []
                    # Successful campaigns.
                    elems.extend(
                        global_driver.find_elements(By.CSS_SELECTOR, 'a[data-modal-title="About the creator"]'))
                    # Other campaigns.
                    elems.extend(global_driver.find_elements(By.CSS_SELECTOR,
                                                             'div[class="do-not-visually-track text-left type-16 bold clip text-ellipsis"]'))
                    try:
                        elems[0].click()
                        time.sleep(random.uniform(3, 7))
                    except Exception:
                        global_driver.refresh()
                        tries -= 1
                        continue
                    else:
                        break

            soup = BeautifulSoup(global_driver.page_source, "lxml")
            success = True
            return soup
        except PageSourceAccessError:
            # Handle the custome Error
            logging.info(f"\nException -PageSourceAccessError\n {traceback.format_exc()} \nRetrying...")
            print(f"\nPageSourceAccessError inside get_live_soup (attempt {attempts}) - {link}")
            # Reopen reader so unscraped rows_to_process will get added in next iteration.
        except Exception as e:
            print(f"Error inside get_live_soup (attempt {attempts}) - {link} \n[~]{e}")
            attempts += 1
        finally:
            global_driver.quit()
    if attempts == max_retries:  # this is when the is really no connection at all
        return None

def extract_soup_json(soup):
    """This function extracts JSON-like data embedded within a <script> tag in the HTML content represented by a BeautifulSoup object. Specifically, it looks for a script containing a window.current_project variable, which holds the JSON data.
    Returns:
        A dictionary containing the parsed JSON data if successful.
        None if the script tag is not found or if there is an error decoding the JSON
    """
    # Locate the script tag containing JSON-like data
    script_tag = soup.find('script', text=re.compile(r'window\.current_project'))
    # Extract the JSON from window.current_project in the script content
    if script_tag:
        script_content = script_tag.string
        # Use regex to find the starting position of the JSON-like data for window.current_project
        start_match = re.search(r'window\.current_project\s*=\s*"', script_content)

        if start_match:
            start_index = start_match.end()  # Adjust to include the starting curly brace
            open_braces = 0
            json_str = ""
            # Iterate over the script content starting from the matched position
            for i in range(start_index, len(script_content)):
                char = script_content[i]
                json_str += char
                if char == '{':
                    open_braces += 1
                elif char == '}':
                    open_braces -= 1
                    if open_braces == 0:
                        break
            # Replacements to convert HTML entities and escaped characters
            json_str = json_str.replace('&quot;', '"').replace('&amp;', '&').replace('\\\\', '\\').replace('\\"', "'")
            # Parse the JSON data
            try:
                current_project_data = json.loads(json_str)
                return current_project_data
            except json.JSONDecodeError:
                print("Error decoding JSON")
                return None
    return None

def extract_campaign_data(db_lock, path):
    """Extracts data from a kickstarter campaign page and returns
    it in a dictionary.

    Inputs:
    path [str] - Path to html file.
    conversion_rate[int] - Conversion rate to use for pledges. 1 by default."""
    global global_driver

    if not path or not path.lower().startswith("https"):
        return None
    data = {"rd_project_link": path}
    # Main try catch to get the soup
    campaign_soup = None
    try:
        campaign_soup = get_live_soup(db_lock, path, page="campaign")
        # Campaign is hidden.
        if campaign_soup == "HIDDEN_CAMPAIGN" or campaign_soup is None:
            print("\n\n***Hidden campaign detected***\n")
            return None
    except Exception as e:
        print(f"\nError fetching data from extract_campaign_data, \n[+]{path} retrying...\n{e}")

    campaign_json = extract_soup_json(campaign_soup)

    data = {}

    # Date and time accessed.
    date = datetime.now().strftime('%Y%m%d')
    data["date_accessed"] = date
    # Creator, Title and Blurb
    meta_elem = campaign_soup.select_one('meta[name="description"]')
    lines = meta_elem["content"].splitlines()
    rd_creator_name, title = lines[0].split(" is raising funds for ")
    title = title.strip().replace(" on Kickstarter!", "")
    blurb = lines[-1].strip()

    data["title"] = title
    data["rd_creator_name"] = rd_creator_name
    data["blurb"] = blurb
    # data-initial attribute has a lot of the required data elements
    # so check if it exists.
    project_data_elem = campaign_soup.select_one('div[data-initial]')
    project_data = None
    if project_data_elem != None:
        project_data = json.loads(project_data_elem['data-initial']).get('project', None)

        # Creator verified identity.
    if project_data:
        verified_identity = project_data['verifiedIdentity']
    else:
        verified_identity_elem = campaign_soup.select_one('span[class="identity_name"]')
        verified_identity = verified_identity_elem.getText().strip() if verified_identity_elem != None else MISSING

        # Creators who verified their account don't have their name posted. e.g. https://www.kickstarter.com/projects/perry/video-chat-at-35000-feet
        if verified_identity == "(name not available)":
            verified_identity = ""
    data['verified_identity'] = verified_identity

   

    # Collaborators. Empty list if no collaborators and
    # empty string if it was not possible to extract.
    collaborators = []
    if project_data:
        collab_list = project_data['collaborators']['edges']
        for collab in collab_list:
            collaborators.append((collab['node']['name'], collab['node']['url'], collab['title']))
    else:
        # Get past collaborators if available.
        past_collab_elem = campaign_soup.select_one('p[class="col col-12"]')
        if past_collab_elem != None:
            for a_elem in past_collab_elem.select('a'):
                # Past collaborators don't have titles.
                collaborators.append((a_elem.getText(), "https://www.kickstarter.com/" + a_elem['href'], ""))

        # Selector in case of single collaborator.
        single_collab_elem = campaign_soup.select_one('[class="flag col col-4 mb3"] > div[class="flag-body"]')
        if single_collab_elem != None:
            a_elem = single_collab_elem.select_one('a')
            collaborators.append((a_elem.getText(), "https://www.kickstarter.com/" + a_elem['href'], single_collab_elem.select_one('div').getText()))

    data["collaborators"] = json.dumps(collaborators)

    data["project_id"] = campaign_json["profile"]["project_id"]
    data["creator_id"] = campaign_json["creator"]["id"]
    data["rd_project_link"] = path
    data["rd_creator_name"] = campaign_json["creator"]["name"]
    # Status of campaign.
    data["status"] = campaign_json["state"]

    # Backers.
    data["cv_num_backers"] = campaign_json["backers_count"]
    # Default values. prj.db
    data["original_curr_symbol"] = campaign_json["current_currency"]
    data["converted_curr_symbol"] = campaign_json["currency"]
    data["conversion_rate"] = campaign_json["fx_rate"]
    data["goal"] = campaign_json["goal"]
    data["converted_goal"] = campaign_json["goal"] / campaign_json["fx_rate"]
    data["pledged"] = campaign_json["pledged"]
    data["converted_pledged"] = campaign_json["converted_pledged_amount"]

    # Convert UNIX timestamps to datetime objects
    start_date = datetime.fromtimestamp(campaign_json["launched_at"])
    end_date = datetime.fromtimestamp(campaign_json["deadline"])
    # Calculate the duration in days
    duration = (end_date - start_date).days
    # Extract and store the day, month, and year components
    # Campaign start time.
    data["time_interval"] = duration
    data["cv_startday"] = start_date.day
    data["cv_startmonth"] = start_date.month
    data["cv_startyear"] = start_date.year
    # Campaign end time.
    data["cv_endday"] = end_date.day
    data["cv_endmonth"] = end_date.month
    data["cv_endyear"] = end_date.year
    data["cv_duration"] = duration
    
    
    # Number of images and photos.
    photos, videos = 0, 0
    # Get number of photos and videos within all content. Do not try to get
    # all photos for all content because there are campaign unrelated photos within
    # this elem.
    content_elem = campaign_soup.select_one('div[id="content-wrap"]')
    description_elem = campaign_soup.select_one('div[class="story-content"]')
    if content_elem != None:
        # Front video.
        videos += len(content_elem.select('video[preload="none"]'))
        # Embedded videos.
        videos += len(content_elem.select('div[class="embedly-card-hug"]'))
        # Front image.
        photos += len(content_elem.select('img[class="js-feature-image"]'))

    if description_elem != None:
        # Images in description.
        photos += len(description_elem.select('img'))

    data["num_photos"] = photos
    data["num_videos"] = videos

    # Make 100 (make100), Projects we love (pwl), Category, Location. make100/pwl is 1 if project is
    # part of it and otherwise 0. prj.db
    data["pwl"] = MISSING
    # Find the icon element using the CSS class selector
    icon_element = campaign_soup.find('svg', class_='svg-icon__icon--small-k icon-14 fill-white')
    # Update the 'pwl' value based on the presence of the icon
    if icon_element is not None:
        data["pwl"] = 1
    else:
        data["pwl"] = 0
        
    data["make100"] = MISSING
    try:
        data["subcategory"] = campaign_json["category"]["parent_name"]
    except KeyError:
        # If there's any KeyError, set the category to None
        data["subcategory"] = None
    data["category"] = campaign_json["category"]["name"]
    data["location"] = campaign_json["location"]["name"]
    # # Number of comments. TODO: remove delete when no need
    # data["rd_comments"] = campaign_json["comments_count"]
    # # Number of updates.
    # data["rd_updates"] = campaign_json["updates_count"]
    
    # Number of projects created.
    rd_creator_created = MISSING
    if project_data and project_data['creator']:
        if 'createdProjects' in project_data['creator']:
            rd_creator_created = project_data['creator']['createdProjects']['totalCount']
        else:
            rd_creator_created = project_data['creator']['launchedProjects']['totalCount']

    if not project_data:
        # This elem contains information about both created and backed projects by creator.
        created_backed_elem = campaign_soup.select_one('[class="created-projects py2 f5 mb3"]')
        if created_backed_elem != None:
            created_text, backed_text = created_backed_elem.getText().replace('\n', '').split('·')
            digits = get_digits(created_text, "int")
            if digits != None:
                rd_creator_created = digits
            else:
                # If not digits, then this project must be "First Created"
                rd_creator_created = 1

    data["rd_creator_created"] = rd_creator_created

    # Number of projects backed.
    num_backed = MISSING
    if project_data and project_data['creator']:
        if 'backedProjects' in project_data['creator']:
            if project_data['creator']['backedProjects'] != None:
                num_backed = project_data['creator']['backedProjects']['totalCount']
        else:
            num_backed = project_data['creator']['backingsCount']

    if not project_data and created_backed_elem != None:
        digits = get_digits(backed_text, "int")
        if digits != None:
            num_backed = digits

    data["num_backed"] = num_backed

    # Number of comments.
    comments_elem = campaign_soup.select_one("a[id='comments-emoji']")
    if comments_elem != None:
        data["rd_comments"] = comments_elem["data-comments-count"]
    else:
        data["rd_comments"] = MISSING

    # Number of updates.
    updates_elem = campaign_soup.select_one("a[id='updates-emoji']")
    if updates_elem != None:
        data["rd_updates"] = updates_elem["emoji-data"]
    else:
        data["rd_updates"] = MISSING

    # Number of faq.
    faq_elem = campaign_soup.select_one("a[id='faq-emoji']")
    if faq_elem != None:
        data["rd_faqs"] = faq_elem["emoji-data"]
    else:
        data["rd_faqs"] = MISSING

    # Description.
    if description_elem != None:
        description = description_elem.getText().strip()
    else:
        description = MISSING
    data["description"] = description

    # Risks.
    risk_elem = campaign_soup.select_one('p[class="js-risks-text text-preline"]')
    if risk_elem != None:
        risk = risk_elem.getText().strip()
    else:
        risk = MISSING
    data["risk"] = risk

    # Pledges. rd_gone is 0 for available pledges and 1 for complete pledges.
    all_pledge_elems = []
    all_pledge_elems.extend([pledge_elem for pledge_elem in campaign_soup.select('article[data-test-id]')])

    data["cv_num_rewards"] = len(all_pledge_elems)

    for i, pledge_elem in enumerate(all_pledge_elems):
        data |= get_pledge_data(pledge_elem, i, 1) # can modify this later to data["fx_rate"] if need be to get from html json

    return data


def scrape_write(db_lock, row):
    """Takes a row of data, scrapes additional data from url and adds full data to database."""
    # attempts to crape additon data from url to ensure its not None
    if DATA_PATH.lower().endswith('.json'):
        row_url = row['urls']['web']['project'].strip()
    elif DATA_PATH.lower().endswith('.csv'):
        row_url = json.loads(row['urls'])['web']['project'].strip()

    logging.info(f"Attempt for scraping {row_url}...")
    project_data = 404
    try:
        project_data = extract_campaign_data(db_lock, row_url)
        if project_data is None:
            logging.error(f"Failed to scrape {row_url} in scrape_write.")
        else:
            logging.info(f"SUCCESS on scraping {row_url} in scrape_write.")
    except Exception:
        logging.error(
            f"Exception occurred from extract_campaign in scrapy_write {row_url}\n{traceback.format_exc()}")

    # This happens after extraction
    with db_lock:
        con = get_projects_db(DATABASE)
        cur = con.cursor()
        if project_data is not None:
            columns = ', '.join(project_data.keys())
            placeholders = ', '.join('?' * len(project_data))
            insert_command = "INSERT OR IGNORE INTO projects ({}) VALUES ({})".format(columns, placeholders)
            cur.execute(insert_command, tuple(project_data.values()))
            if cur.rowcount == 1:
                print(f"Added {project_data['rd_project_link']} to PROJECT table.")
            elif cur.rowcount == 0:
                try:
                    project_data['rd_project_link'] = row_url
                    insert_command = "INSERT OR IGNORE INTO ignored_projects ({}) VALUES ({})".format(columns, placeholders)
                    cur.execute(insert_command, tuple(project_data.values()))
                    print(
                        f"Ignored {row_url} --> same as {project_data['rd_project_link']} in PROJECT table.")
                except sqlite3.Error as e:
                    con.commit()
                    con.close()
                    print("Exception error IGNORED_PROJECTS for ", row_url, e)
        elif project_data != 404:
            insert_command = "INSERT OR IGNORE INTO hidden_projects (url) VALUES (?)"
            cur.execute(insert_command, (row_url,))
            print(f"Into {row_url} in HIDDEN_PROJECT table.")

        con.commit()
        con.close()
    logging.info(f"DONE scraping {row_url}\n\n")


if __name__ == "__main__":
    if not TESTING:
        main()
    else:
        test_extract_campaign_data()
