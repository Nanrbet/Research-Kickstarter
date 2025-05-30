import multiprocessing
from datetime import datetime
import re
import logging
import time
import json
import winsound
import sqlite3
import os
import csv
import random
import traceback

import undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pyautogui
from bs4 import BeautifulSoup
import pandas as pd


# Settings.

# Path to project data. Make sure to use raw strings or escape "\".
DATA_PATH = r"scraping_projects.csv"
# Output path.
OUTPUT_PATH = r""
DATABASE = os.path.join(OUTPUT_PATH, "new_projects.db")
# Chromedriver path
DRIVER_PATH="C:/Users/Admin/Downloads/jaber-2024_02_04-001/jaber/chromedriver-win64/chromedriver-win64/chromedriver.exe"
# Chrome browser path
# BROWSER_PATH="C:/Users/Admin/Downloads/jaber-2024_02_04-001/jaber/chrome-win64/chrome-win64/chrome.exe"

# chrome_options.add_argument("--disable-gpu")
# chrome_options.add_argument("user-agent=Your_User_Agent_String")

# ua = UserAgent()

# Set logging.
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p')
# Set what value to enter in case of missing data. Default is ""
MISSING = ""
# Set to True if Testing and False otherwise.
TESTING = 1
# Number of processes per try.
chunk_size = 4
# Proton vpn windows taskbar location.
icon_num = 1

pyautogui.FAILSAFE = False

# Script.

# Lock to prevent multiple processes from trying to access database.
db_lock = multiprocessing.Lock()

def main():
    pool = multiprocessing.Pool()
    click_random(icon_num)

    # Get projects to scrape.
    f_obj = open(DATA_PATH, encoding="utf8", newline='')
    reader = csv.DictReader(f_obj)

    total = 0
    Done = False
    while not Done:
        # Get at maximum chunk_size number rows as a list per iteration.
        rows = get_rows(reader, DATABASE, chunk_size) 

        try:
            pool.map(scrape_write, rows)
        except Exception:
            logging.info(f"\nException -\n {traceback.format_exc()} \nRetrying...")

            # Reopen reader so unscraped rows will get added in next iteration.
            f_obj.close()
            f_obj = open(DATA_PATH, encoding="utf8", newline='')
            reader = csv.DictReader(f_obj)

            click_random(icon_num)
            time.sleep(30)

        # Scraping complete since there aren't enough rows left to reach chunk_size.
        if len(rows) < chunk_size:
            Done = True
        if len(rows) == 0:
            continue
        
        # Stop scraping for a period of time to not be blocked as a bot.
        total += chunk_size
        if total % (chunk_size * 3) == 0:
            logging.info("Changing server...\n")
            click_random(icon_num)

    f_obj.close()
    pool.close()
    pool.join()

    # logging.info("Writing data to file...")

def test_extract_campaign_data():
    # Testing code.
    file_paths = [
                #"https://www.kickstarter.com/projects/petersand/manylabs-sensors-for-students", 
                #   "https://www.kickstarter.com/projects/hellodawnco/pokeballoons-evolution-edition",
                #   "https://www.kickstarter.com/projects/lucid-dreamers/empires-of-sorcery",
                #   "https://www.kickstarter.com/projects/larianstudios/divinity-original-sin-the-board-game",
                #   "https://www.kickstarter.com/projects/120302834/deep-rock-galactic-space-rig-and-biome-expansions",
                #   "https://www.kickstarter.com/projects/ogglio/2023-olive-oil-harvest/",
                #   "https://www.kickstarter.com/projects/artorder/2018-snowman-greeting-card-collection/",
                #   "https://www.kickstarter.com/projects/732431717/photo-time-machine",
                  "https://www.kickstarter.com/projects/perry/video-chat-at-35000-feet",
                #   "https://www.kickstarter.com/projects/perry/grace-jones-does-not-give-a-f-t-shirt-limited-edition-0",
                #   "https://www.kickstarter.com/projects/lucid-dreamers/empires-of-sorcery",
                #   "https://www.kickstarter.com/projects/Samplefreq/crowdsource-a-musical-adventure"

                  ]
    if file_paths is not None:
        pool = multiprocessing.Pool(processes=1) # TODO: remove processes
        # pool = multiprocessing.Pool()
        data = pool.map(extract_campaign_data, file_paths)
        
        pool.close()
        pool.join()
        # for path in file_paths:
        #     data = extract_campaign_data(path)

        df = pd.DataFrame(data)
        df.to_csv('test.csv', index = False)
    else: 
        print("file_paths is empty")

def get_rows(reader, database, n_rows):
    """Returns n rows from csv reader while making sure they weren't already scraped by checking in database."""
    rows = []
    # Get already scraped urls.
    con = create_new_projects_db(database)
    cur = con.cursor()
    scraped_urls = set(url[0] for url in cur.execute("SELECT rd_project_link FROM projects;")) 
    scraped_urls |= set(url[0] for url in cur.execute("SELECT url FROM hidden_projects;")) 
    con.close()
    
    # Get n rows which haven't been scraped if there are enough remaining rows.
    while len(rows) != n_rows:
        try:
            row = next(reader)
        except StopIteration:
            break

        if row['url'] in scraped_urls:
            continue
        else:
            rows.append(row)
    
    return rows

def click_random(icon_num, wait=True):
    """
    Clicks random button in proton vpn. Proton VPN needs
    to be open on the profiles page. 
    
    icon_num [int] - Index of proton vpn icon on the windows taskbar.
    wait [bool] - If True, function will sleep for 10s to make sure Proton Vpn
    connects and otherwise it will not sleep. True by default.
    """
    pyautogui.hotkey('win', str(icon_num))
    pyautogui.click(1055, 444, clicks=2, interval=0.5)
    time.sleep(2)
    pyautogui.hotkey('alt', 'tab')
    if wait:
        time.sleep(5)

def create_new_projects_db(database):
    """
    Creates database if it doesn't exist and returns a connection.
    
    path[str] - Location to save/load database
    """
    con = sqlite3.connect(database)
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
    pledge_data['rd_title_' + i] = bs4_tag.select_one('[class="support-700 semibold kds-heading type-18 m0 mr1 text-wrap-balance break-word"]').getText().strip()
    
    pledge_data['rd_price_' + i] = get_digits(bs4_tag.select_one('[class="support-700 type-18 m0 shrink0"]').getText(), "int") * float(conversion_rate)

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
        item_elems = item_list_elem.select('[class="block ml-0 z3 border border2px border-white radius100p shadow-reward-avatar"]')
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
    categories = {'Art': {'Ceramics', 'Conceptual Art', 'Digital Art', 'Illustration', 'Installations', 'Mixed Media', 'Painting', 'Performance Art', 'Public Art', 'Sculpture', 'Social Practice', 'Textiles', 'Video Art'}, 
                'Comics': {'Anthologies', 'Comic Books', 'Events', 'Graphic Novels', 'Webcomics'}, 
                'Crafts': {'Candles', 'Crochet', 'DIY', 'Embroidery', 'Glass', 'Knitting', 'Pottery', 'Printing', 'Quilts', 'Stationery', 'Taxidermy', 'Weaving', 'Woodworking'}, 
                'Dance': {'Performances', 'Residencies', 'Spaces', 'Workshops'}, 
                'Design': {'Architecture', 'Civic Design', 'Graphic Design', 'Interactive Design', 'Product Design', 'Toys', 'Typography'}, 
                'Fashion': {'Accessories', 'Apparel', 'Childrenswear', 'Couture', 'Footwear', 'Jewelry', 'Pet Fashion', 'Ready-to-wear'}, 
                'Film & Video': {'Action', 'Animation', 'Comedy', 'Documentary', 'Drama', 'Experimental', 'Family', 'Fantasy', 'Festivals', 'Horror', 'Movie Theaters', 'Music Videos', 'Narrative Film', 'Romance', 'Science Fiction', 'Shorts', 'Television', 'Thrillers', 'Webseries'}, 
                'Food': {'Bacon', 'Community Gardens', 'Cookbooks', 'Drinks', 'Events', "Farmer's Markets", 'Farms', 'Food Trucks', 'Restaurants', 'Small Batch', 'Spaces', 'Vegan'}, 
                'Games': {'Gaming Hardware', 'Live Games', 'Mobile Games', 'Playing Cards', 'Puzzles', 'Tabletop Games', 'Video Games'}, 
                'Journalism': {'Audio', 'Photo', 'Print', 'Video', 'Web'}, 
                'Music': {'Blues', 'Chiptune', 'Classical Music', 'Comedy', 'Country & Folk', 'Electronic Music', 'Faith', 'Hip-Hop', 'Indie Rock', 'Jazz', 'Kids', 'Latin', 'Metal', 'Pop', 'Punk', 'R&B', 'Rock', 'World Music'}, 
                'Photography': {'Animals', 'Fine Art', 'Nature', 'People', 'Photobooks', 'Places'}, 
                'Publishing': {'Academic', 'Anthologies', 'Art Books', 'Calendars', "Children's Books", 'Comedy', 'Fiction', 'Letterpress', 'Literary Journals', 'Literary Spaces', 'Nonfiction', 'Periodicals', 'Poetry', 'Radio & Podcasts', 'Translations', 'Young Adult', 'Zines'}, 
                'Technology': {'3D Printing', 'Apps', 'Camera Equipment', 'DIY Electronics', 'Fabrication Tools', 'Flight', 'Gadgets', 'Hardware', 'Makerspaces', 'Robots', 'Software', 'Sound', 'Space Exploration', 'Wearables', 'Web'}, 
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

def handle_captcha(driver, link):
    """
    Handle captcha if present by beeping and sleeping for some time (seconds).
    Then, create a new WebDriver instance and navigate to the link again.
    Return the new WebDriver instance.
    """
    max_attempts = 5
    attempts = 0

    while attempts < max_attempts:
        # Check for CAPTCHA element
        soup = BeautifulSoup(driver.page_source, "lxml")
        capcha_elem = soup.select_one('div[id="px-captcha"]')
        if capcha_elem:
            # Beep to indicate CAPTCHA and sleep for 30 seconds
            winsound.Beep(440, 1000)        
            time.sleep(3)   # TODO: replace to 30

            # Quit the current WebDriver instance
            driver.quit()
            click_random(1) # TODO: replace with global variable of VPN position
            # Create a new WebDriver instance
            chrome_options = uc.ChromeOptions()
            driver = uc.Chrome(options=chrome_options, driver_executable_path=DRIVER_PATH, parse_with_lxml=True)
            
            # Navigate to the link again
            driver.get(link)
            attempts += 1
        else:
            break
    
    return driver


def get_live_soup(link, given_driver=None, page=None):
    """Returns a bs4 soup object of the given link. Returns None if it is a deleted kickstarter account.
    
    link [str] - A link to a website.
    scroll [bool] - True if you want selenium to keep scrolling down till loading no longer happens.
    False by default.
    given_driver [selenium webdriver] - A webdriver. None by default.
    page [str] - Additional behavior depending on page type."""
    driver = given_driver
    
    driver.get(link)

    # Click creator page for page to load additional data if it is a campaign page.
    # There are two possible alternate selectors. One for successful campaigns and the
    # other for other campaigns. Try finding both and click whichever that exists.
    if page == "campaign":
            # Try reloading page at most 2 times if required elems aren't found.
            tries = 2
            while tries != 0:
                elems = []
                # Successful campaigns.
                elems.extend(driver.find_elements(By.CSS_SELECTOR, 'a[data-modal-title="About the creator"]'))
                # Other campaigns.
                elems.extend(driver.find_elements(By.CSS_SELECTOR, 'div[class="do-not-visually-track text-left type-16 bold clip text-ellipsis"]'))
                try:
                    elems[0].click()
                    time.sleep(random.uniform(3, 7))
                except Exception:
                    driver.refresh()
                    tries -= 1
                    # checks for capcha
                    soup = BeautifulSoup(driver.page_source, "lxml")
                    if soup.select_one('div[id="px-captcha"]') != None:
                        driver = handle_captcha(driver, link)
                    continue
                else:
                    break

    soup = BeautifulSoup(driver.page_source, "lxml")

    # Hidden project. For e.g. https://www.kickstarter.com/projects/732431717/photo-time-machine
    hidden_elem = soup.select_one('div[id="hidden_project"]')
    if hidden_elem != None:
        driver.quit()
        return
    
        
    # # If it is a deleted account or there is a 404 error, return.
    # deleted_elem = soup.select_one('div[class="center"]')
    # non_existent_elem = soup.select_one('a[href="/?ref=404-ksr10"]')
    # if deleted_elem != None or non_existent_elem != None:
    #     driver.quit()
    #     return

    # Wait for rewards to load.
    if page == "rewards":
        max_timeout = 15
        # Try two times. Some campaigns don't have any rewards 
        # and the page will keep loading forever e.g. https://www.kickstarter.com/projects/spencerclintonparker/dvd-cases-for-bissell-family-documentary/rewards
        tries = 2
        while tries != 0:
            try:
                element_present = EC.presence_of_element_located((By.CSS_SELECTOR, 'article[data-test-id]'))
                WebDriverWait(driver, max_timeout).until(element_present)
            except TimeoutException:
                print(f"Timed out waiting for {link} to load. Refreshing...")
                driver.refresh()
                tries -= 1
                # checks for capcha
                soup = BeautifulSoup(driver.page_source, "lxml")
                if soup.select_one('div[id="px-captcha"]') != None:
                    driver = handle_captcha(driver, link)
            else:
                break

    soup = BeautifulSoup(driver.page_source, "lxml")

    if driver:
        driver.quit()

    return soup

def extract_campaign_data(path, conversion_rate=1):
    """Extracts data from a kickstarter campaign page and returns
    it in a dictionary. 
    
    Inputs:
    path [str] - Path to html file.
    conversion_rate[int] - Conversion rate to use for pledges. 1 by default."""
    data = {"rd_project_link": path}
    driver = None
    try:
        chrome_options = uc.ChromeOptions()
        driver = uc.Chrome(options=chrome_options, driver_executable_path=DRIVER_PATH, parse_with_lxml=True)
        campaign_soup = get_live_soup(path, given_driver=driver, page="campaign")

        # Campaign is hidden.
        if campaign_soup == None:
            return
        
        chrome_options = uc.ChromeOptions()
        driver = uc.Chrome(options=chrome_options, driver_executable_path=DRIVER_PATH, parse_with_lxml=True)
        reward_soup = get_live_soup(path + "/rewards", given_driver=driver, page="rewards")
        
    except WebDriverException as e:
        print(f"Error creating WebDriver from extract_campaign_data: {e}")
    
    # finally: Already handled by get_live_soup
    #     if driver:
    #         driver.quit()

    # Prepare str for getting date and time. 
    path = datetime.now().strftime('_%Y%m%d-%H%M%S.html')

    data = {}

    # Date and time accessed.
    date_time_str = path.split("_")[-1]
    date_time_str = date_time_str[:-5] 
    date, time = date_time_str.split("-")

    data["date_accessed"] = date

    # rd_project_link. If missing, do not continue.
    try:
        rd_project_link_elem = campaign_soup.select_one('meta[property="og:url"]')
        data["rd_project_link"] = rd_project_link_elem["content"]
    except:
        return data

    # Project Id and Creator Id.
    creator_id, project_id = data["rd_project_link"].split("/")[-2:]
    data["project_id"] = project_id
    data["creator_id"] = creator_id

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

    # Status of campaign.
    data["status"] = MISSING
    data["cv_duration"] = MISSING

    # Backers.
    data["cv_num_backers"] = MISSING

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

    # Default values. prj.db
    data["original_curr_symbol"] = MISSING
    data["converted_curr_symbol"] = MISSING   
    data["conversion_rate"] = MISSING
    data["goal"] = MISSING
    data["converted_goal"] = MISSING
    data["pledged"] = MISSING
    data["converted_pledged"] = MISSING

    # Campaign start time.
    data["cv_startday"] = MISSING
    data["cv_startmonth"] = MISSING
    data["cv_startyear"] = MISSING

    # Campaign end time.
    data["cv_endday"] = MISSING
    data["cv_endmonth"] = MISSING
    data["cv_endyear"] = MISSING

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
    data["make100"] = MISSING
    data["category"] = MISSING
    data["subcategory"] = MISSING
    data["location"] = MISSING

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
    all_pledge_elems.extend([pledge_elem for pledge_elem in reward_soup.select('article[data-test-id]')])

    data["cv_num_rewards"] = len(all_pledge_elems)

    for i, pledge_elem in enumerate(all_pledge_elems):
        data |= get_pledge_data(pledge_elem, i, conversion_rate)

    return data

def scrape_write(row):
    """Takes a row of data, scrapes additional data from url and adds full data to database."""
    logging.info(f"Started scraping {row['url']}...")
    project_data = extract_campaign_data(row["url"], row["conversion_rate"])

    if project_data != None:
        # Merge data.
        start_date = datetime.strptime(row["launched_date"], "%Y-%m-%d")
        end_date = datetime.strptime(row["deadline_date"], "%Y-%m-%d")
        duration = (end_date - start_date).days

        project_data["time_interval"] = duration
        project_data["cv_startday"] = start_date.day
        project_data["cv_startmonth"] = start_date.month
        project_data["cv_startyear"] = start_date.year
        project_data["cv_endday"] = end_date.day
        project_data["cv_endmonth"] = end_date.month
        project_data["cv_endyear"] = end_date.year
        project_data["cv_duration"] = duration

        project_data["status"] = row["state"]
        project_data["original_curr_symbol"] = row["original_currency"]
        project_data["converted_curr_symbol"] = row["converted_currency"]   
        project_data["conversion_rate"] = float(row["conversion_rate"])
        project_data["goal"] = float(row["goal"]) / project_data["conversion_rate"]
        project_data["converted_goal"] = float(row["goal"])
        project_data["pledged"] = float(row["pledged"]) / project_data["conversion_rate"]
        project_data["converted_pledged"] = float(row["pledged"])

        project_data["pwl"] = row["pwl"]
        project_data["make100"] = MISSING
        project_data["category"] = row["category"]
        project_data["subcategory"] = row["subcategory"]
        project_data["location"] = row["location"]

    with db_lock:
        con = create_new_projects_db(DATABASE)
        cur = con.cursor()

        if project_data != None:
            columns = ', '.join(project_data.keys())
            placeholders = ', '.join('?' * len(project_data))
            insert_command = "INSERT OR IGNORE INTO projects ({}) VALUES ({})".format(columns, placeholders)
            cur.execute(insert_command, tuple(project_data.values()))
            logging.info(f"Added {row['url']} to table...")
        else:
            columns = ', '.join(row.keys())
            placeholders = ', '.join('?' * len(row))
            insert_command = "INSERT OR IGNORE INTO hidden_projects ({}) VALUES ({})".format(columns, placeholders)           
            cur.execute(insert_command, tuple(row.values()))

        con.commit()
        con.close()

if __name__ == "__main__":
    if not TESTING:
        main()
    else:
        test_extract_campaign_data()
