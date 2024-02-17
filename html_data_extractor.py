import os
import zipfile
import multiprocessing
from datetime import datetime
import re
from collections import defaultdict
import logging
import time
import shutil
import json

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

# Settings.

# Path to data. Make sure to use raw strings or escape "\".
DATA_PATH = r"F:\Kickstarter Zips\Unzipped"
# If data is already unzipped, set UNZIP to False and True otherwise.
UNZIP = False
# Toggle to turn on/off deleting unzipped files.
DELETE = True
# Toggle to turn off/on live scraping. 
OFFLINE = True
# Toggle to turn on/off testing.
TESTING = False
# Set what value to enter in case of missing data. Default is ""
MISSING = ""
# Set logging.
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p')

# Script.

def main():
    campaign_data = []
    update_data = {}
    zip_files = []
    pool = multiprocessing.Pool()

    if UNZIP:
        # Find all zip files in DATA_PATH.
        for file in os.listdir(DATA_PATH):
            if file.endswith(".zip"):
                zip_files.append(os.path.join(DATA_PATH, file))

        # Folder which will contain unzipped data. Script will create it if
        # it doesn't exist.
        to_path = os.path.join(DATA_PATH, "Unzipped")

        # Unzip one zip at a time, extract data from files and then delete
        # the unzipped data.
        zip_num = len(zip_files)
        for i, zip_file in enumerate(zip_files, 1):
            logging.info(f"Zip: {i} / {zip_num}")
            os.makedirs(to_path, exist_ok=True)

            folder_path = nested_unzipper(zip_file, to_path)
            campaign_files, update_files = classifier(folder_path)

            # Process update files.
            logging.info("Processing update files...")
            roots = defaultdict(list)
            for file_path in  update_files:
                roots[os.path.dirname(file_path)].append(file_path)

            update_data |= dict(pool.map(extract_update_files_data, roots.values()))

            # Process campaign files.
            logging.info("Processing campaign files...")
            campaign_res = list(tqdm(pool.imap(extract_campaign_data, campaign_files, chunksize=10), total=len(campaign_files)))
            campaign_data.extend(campaign_res)
            
            # Delete unzipped data.
            if DELETE:
                logging.info("Deleting unzipped files...")
                shutil.rmtree(to_path)
            logging.info("Finished processing.\n")

    else:
        campaign_files, update_files = classifier(DATA_PATH)
        # campaign_files = campaign_files[:10000]
        # Process update files.
        logging.info("Processing update files...")
        roots = defaultdict(list)
        for file_path in  update_files:
            roots[os.path.dirname(file_path)].append(file_path)

        update_data = pool.map(extract_update_files_data, roots.values())
        update_data = dict(update_data)

        # Process campaign files.
        logging.info("Processing campaign files...")
        campaign_data = list(tqdm(pool.imap(extract_campaign_data, campaign_files, chunksize=20), total=len(campaign_files)))

    pool.close()
    pool.join()

    # Merge campaign and update data.
    logging.info("Merging data...")
    all_data = []
    missing_data = []
    imp_columns = ['verified_identity','status', 'backers', 'collaborators', 'original_curr_symbol', 'converted_curr_symbol', 'conversion_rate', 'goal', 
                    'converted_goal', 'pledged', 'converted_pledged', 'startday', 'startmonth', 'startyear', 'endday', 
                    'endmonth', 'endyear', 'pwl', 'make100', 'category', 'location', 'num_projects', 'num_backed', 'num_comments', 'num_updates', 
                    'num_faq', 'description', 'risk']

    verified_identities = {}
    for campaign_datum in tqdm(campaign_data):
        url = campaign_datum.get("url", None)
        
        if url != None:
            campaign_datum["startday"], campaign_datum["startmonth"], campaign_datum["startyear"] = update_data.get(url, (MISSING, MISSING, MISSING))

            if campaign_datum['verified_identity'] == MISSING:
                campaign_datum['verified_identity'] = verified_identities.get(url, MISSING)
            elif url not in verified_identities.keys():
                verified_identities[url] = campaign_datum['verified_identity']

        all_data.append(campaign_datum)

        # Keep track of files which are missing data in important columns.
        missing = [col for col in imp_columns if campaign_datum.get(col, MISSING) == MISSING]
        if len(missing) > 0:
            missing_datum = {'missing': missing}
            missing_datum |= campaign_datum
            missing_data.append(missing_datum)

    logging.info("Writing data to file...")

    output_folder = "Output"
    os.makedirs(output_folder, exist_ok=True)
    
    # Generate time string for output files for current zips.
    time_str = datetime.now().strftime('%Y%m%d-%H%M%S')

    with open(os.path.join(output_folder, f"zips_{time_str}.txt"), "w") as f_obj:
        f_obj.writelines([zip_file + "\n" for zip_file in zip_files])

    # Create dataframe and export output as csv.
    df = pd.DataFrame(all_data)
    df.to_csv(os.path.join(output_folder, f'results_{time_str}.csv'), index=False)

    missing_df = pd.DataFrame(missing_data)
    missing_df.to_csv(os.path.join(output_folder, f'missing_{time_str}.csv'), index=False)

def test_extract_campaign_data():
    # Testing code.
    file_paths = [
                (r"C:\Users\jaber\OneDrive\Desktop\Research_JaberChowdhury\Data\art\Other\Unzipped\a1\1-1000-supporters-an-art-gallery-and-design-boutiq\1-1000-supporters-an-art-gallery-and-design-boutiq_20190312-010622.html",), # Nothing special
                (r"F:\Kickstarter Zips\Unzipped\sos-save-our-ship-0\sos-save-our-ship-0_20181205-004742.html",), # Video count issue
                (r"F:/Kickstarter Zips/Unzipped/statue-of-the-martyr-of-science-giordano-bruno/statue-of-the-martyr-of-science-giordano-bruno_20181101-183924.html",), # Youtube videos
                # ("https://www.kickstarter.com/projects/metmo/metmo-pocket-driver?ref=section-homepage-view-more-discovery-p1", True), # Has collaborators.
                (r"F:/Kickstarter Zips/Unzipped/10-years-of-work-in-a-deluxe-artbook-paintings-and/10-years-of-work-in-a-deluxe-artbook-paintings-and_20181106-213950.html",), # Missing data
                (r"F:/Kickstarter Zips/Unzipped/fixed-animal-collage/fixed-animal-collage_20181124-085618.html",), # Empty creator in data-initial
                ]
    data = [extract_campaign_data(*file_path) for file_path in file_paths]
    df = pd.DataFrame(data)
    df.to_csv('test.csv', index = False)

def nested_unzipper(file_path, to_path):
    """Unzips nested zip in file_path to given to_path. Deletes nested
    zips after unzipping. Returns path to unzipped data.

    Inputs - 
    file_path [str]: Path to nested zip.
    to_path [str]: Path to store unzipped files."""
    # Create folder in destination for unzipped data.
    base = os.path.basename(file_path)
    to_path = os.path.join(to_path, base[:-4])
    os.makedirs(to_path, exist_ok=True)

    logging.info(f"Unzipping \"{base}\"...")
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(to_path)

    # Unzip any files inside unzipped zip.
    logging.info(f"Unzipping nested zips inside \"{base}\"...")
    to_path_zips = []
    for (root, dirs, files) in os.walk(to_path):
        for file in files:
            if file.endswith(".zip"):
                to_path_zips.append(os.path.join(root, file))
    
    # Unzip nested zips and delete the zips.
    for zip_file in tqdm(to_path_zips):
        file_dir = os.path.dirname(zip_file)
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(file_dir)
        except zipfile.BadZipFile:
            continue
        os.remove(zip_file)
    
    return to_path

def classifier(path):
    """Classifies html files in path and returns a tuple of the paths of the classified files according
    to their class."""
    # Files to ignore.
    ignore_set = {"community", "faqs", "comments"}

    # # Get paths of all html files in the data folder.
    campaign_files = []
    update_files = []
    for (root, dirs, files) in os.walk(path):
        for file in files:
            if file.endswith(".html"):
                file_type = file.split("_")[1]

                if file_type == "updates":
                    update_files.append(os.path.join(root, file))
                elif file_type not in ignore_set:
                    campaign_files.append(os.path.join(root, file))
    
    return campaign_files, update_files

def get_str(string, extra):
    """Returns a string without any digits.
    
    Inputs:
    string [str] - Any string.
    extra [set] - Extra set of characters to exclude."""
    return "".join([char for char in string if not (char.isdigit() or char in extra)]).strip()

def get_digits(string, conv="float"):
    """Returns only digits from string as a single int/float. Default
    is float.
    
    Inputs: 
    string[str] - Any string.
    conv[str] - Enter "float" if you need float. Otherwise will provide integer."""
    if conv == "float":
        res = re.findall(r'[0-9.]+', string)
        return float("".join(res))
    else:
        res = re.findall(r'\d+', string)
        return int("".join(res))

def get_pledge_data(bs4_tag, index=0):
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
    Index [int] - Optional. The index of the current pledge. Has a default value of 0."""
    pledge_data = {}
    i = str(index)

    pledge_data['rd_id_' + i] = bs4_tag['data-reward-id']
    pledge_data['rd_title_' + i] = bs4_tag.select_one('h3[class="pledge__title"]').getText().strip()
    pledge_data['rd_price_' + i] = get_digits(bs4_tag.select_one('span[class="pledge__currency-conversion"] > span').getText()) 
    pledge_data['rd_desc_' + i] = bs4_tag.select_one('div[class="pledge__reward-description pledge__reward-description--expanded"]').getText().replace('\n', '')[:-4]
    
    # Rewards list. If it does not exist, return empty string.
    rd_list = [elem.getText().replace('\n', '') for elem in bs4_tag.select('li[class="list-disc"]')]
    if len(rd_list) == 0:
        pledge_data['rd_list_' + i] = MISSING
    else:
        pledge_data['rd_list_' + i] = rd_list
    
    pledge_data['rd_delivery_date_' + i] = bs4_tag.select_one('span[class="pledge__detail-info"] > time')['datetime']

    # Below elem can contain estimated date of delivery and the shipping location (optional).
    pledge_detail_elems = bs4_tag.select('span[class="pledge__detail-info"]')
    # It has the shipping location.
    if len(pledge_detail_elems) > 1:
        pledge_data['rd_shipping_location_' + i] = pledge_detail_elems[1].getText()
    # No shipping location.
    else:
        pledge_data['rd_shipping_location_' + i] = MISSING

    try:
        rd_backers = get_digits(bs4_tag.select_one('span[class="pledge__backer-count"]').getText())
    # Reward has a limit so it has a different class value.
    except AttributeError:
        rd_backers = get_digits(bs4_tag.select_one('span[class="block pledge__backer-count"]').getText())
    finally:
        pledge_data["rd_backers_" + i] = rd_backers

    rd_limit_elem = bs4_tag.select_one('span[class="pledge__limit"]')
    try:
        rd_limit = get_digits(rd_limit_elem.getText().split()[-1])
    except:
        rd_limit = MISSING
    pledge_data["rd_limit_" + i] = rd_limit

    # Below tag is there only for pledges which have reached their limit.
    # These pledges don't show the limit so their limit = num of backers 
    rd_gone_elem = bs4_tag.select_one('span[class="pledge__limit pledge__limit--all-gone mr2"]')
    if rd_gone_elem != None:
        pledge_data["rd_limit_" + i] = rd_backers
        pledge_data["rd_gone_" + i] = 1
    else:
        pledge_data["rd_gone_" + i] = 0

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

def extract_update_files_data(files):
    """"Takes a list of update files of the same root and returns a tuple of url and startdate."""
    url = MISSING
    date = (MISSING, MISSING, MISSING)
    for file in files:
        with open(file, encoding='utf8', errors="backslashreplace") as infile:
            soup = BeautifulSoup(infile, "lxml")
        
        try:
            # Url
            url = soup.select_one('meta[property="og:url"]')["content"]
        except:
           continue

        # Start date
        date_elem = soup.select_one('time[class="invisible-if-js js-adjust-time"]')

        # First file has the start date so no point in checking the other files
        if date_elem != None:
            dt = datetime.strptime(date_elem.getText(), "%B %d, %Y")
            date = (dt.day, dt.month, dt.year)
            break
    # None of the saved files had the start date so take it from the live page
    # if not offline.
    else:
        if not OFFLINE:
            update_url = url + "/posts"
            driver = webdriver.Chrome()
            driver.get(update_url)
            # Wait at most 10s for required tag to load and otherwise raise a TimeoutException.
            date_selector = 'div[class="type-11 type-14-sm text-uppercase"]'
            try:
                element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, date_selector))
                )
            finally:
                dt = datetime.strptime(driver.find_element(By.CSS_SELECTOR, date_selector).text, "%B %d, %Y")
                date = (dt.day, dt.month, dt.year)
                driver.quit()

    return (url, date)

def get_live_soup(link):
    """Returns a bs4 soup object of the given link.
    
    link [str] - A link to a website."""
    driver = webdriver.Chrome()
    driver.get(link)
    time.sleep(1)
    soup = BeautifulSoup(driver.page_source, "lxml")
    driver.quit()

    return soup

def extract_campaign_data(path, is_link=False):
    """Extracts data from a kickstarter campaign page and returns
    it in a dictionary. 
    
    Inputs:
    path [str] - Path to html file.
    is_link [boolean] - True if path is a link and False otherwise. False by default."""
    if not is_link:
        with open(path, encoding='utf8', errors="backslashreplace") as infile:
            soup = BeautifulSoup(infile, "lxml")
    else:
        if OFFLINE:
            data = {"url": path}
            return data
        
        soup = get_live_soup(path)
        # Prepare str for getting date and time. 
        path = datetime.now().strftime('_%Y%m%d-%H%M%S.html')

    data = {}

    # Date and time accessed.
    date_time_str = path.split("_")[-1]
    date_time_str = date_time_str[:-5] 
    date, time = date_time_str.split("-")

    data["date_accessed"] = date
    data["time_accessed"] = time

    # Url. If missing, do not continue.
    try:
        url_elem = soup.select_one('meta[property="og:url"]')
        data["url"] = url_elem["content"]
    except:
        return data

    # Project Id and Creator Id.
    creator_id, project_id = data["url"].split("/")[-2:]
    data["project_id"] = project_id
    data["creator_id"] = creator_id

    # Creator, Title and Blurb
    meta_elem = soup.select_one('meta[name="description"]')
    lines = meta_elem["content"].splitlines()
    creator, title = lines[0].split(" is raising funds for ")
    title = title.strip().replace(" on Kickstarter!", "")
    blurb = lines[-1].strip()

    data["title"] = title
    data["creator"] = creator
    data["blurb"] = blurb 

    # data-initial attribute has a lot of the required data elements
    # so check if it exists.
    project_data_elem = soup.select_one('div[data-initial]')
    project_data = None
    if project_data_elem != None:
        project_data = json.loads(project_data_elem['data-initial']).get('project', None)  

    # Creator verified identity.
    verified_identity = MISSING
    if project_data:
        verified_identity = project_data['verifiedIdentity']
    data['verified_identity'] = verified_identity  

    # Status of campaign.
    status = MISSING

    # Status strings.
    successful = "Successful"
    failed = "Failed"
    canceled = "Canceled"
    suspended = "Suspended"
    live = "Live"

    state_elem = soup.select_one('section[class="js-project-content js-project-description-content project-content"]')
    if state_elem != None:
        status = state_elem['data-project-state']
    elif project_data:
        status = project_data['state']

    status = status.title()
    data["status"] = status

    # Backers.
    backers = MISSING

    if project_data:
        if 'backersCount' in project_data:
            backers = project_data['backersCount']
        else:
            backers = project_data['backers']['totalCount']   
    elif status == successful:
        backers_elem = soup.select_one('div[class="mb0"] > h3[class="mb0"]')
        if backers_elem != None:
            backers = backers_elem.getText().strip()
    else:
        backers_elem = soup.select_one('div[class="block type-16 type-24-md medium soft-black"]')
        if backers_elem != None:
            backers = backers_elem.getText().strip()       

    data["backers"] = backers

    # Collaborators. Empty list if no collaborators and
    # empty string if it was not possible to extract.
    collaborators = []
    if project_data:
        collab_list = project_data['collaborators']['edges']
        for collab in collab_list:
            collaborators.append((collab['node']['name'], collab['node']['url'], collab['title']))
    else:
        collaborators = ""
    data["collaborators"] = collaborators

    # Default values.
    original_curr_symbol = converted_curr_symbol = MISSING
    conversion_rate = 0
    goal = converted_goal = MISSING
    pledged = converted_pledged = MISSING

    # Some tags for campaigns at different statuses are distinct for
    # currency symbols, goals and pledges.
    if status == live:
        # Get conversion rate of currency if necessary for goal and pledged amounts.
        curr_elems = soup.select('div[class="input__currency-conversion"]')
        # Check if currency conversions are present.
        if len(curr_elems) > 0:
            curr_elem = curr_elems[0]
            back_elem = soup.select('input[name="backing[amount]"]')[0]

            converted_curr_amount = get_digits(curr_elem.contents[1].getText())
            original_curr_amount = get_digits(back_elem["value"])
            conversion_rate = converted_curr_amount / original_curr_amount

            # Get symbols for both currencies.
            converted_curr_symbol = get_str(curr_elem.contents[1].getText(), {'.', ','})
            original_curr_symbol = soup.select_one('span[class="new-form__currency-box__text"]').getText().strip()

        # No need for conversion.
        else:
            original_curr_symbol = converted_curr_symbol = re.findall("window.current_currency = '(\w+)'", str(soup))[0].strip()
            conversion_rate = 1

        # Fix symbols to one form if they have known alternate forms.
        fixed_symbols = {"USD": "$", "US$": "$", "Â£": "£", "â‚¬": "€"}
        if original_curr_symbol in fixed_symbols.keys():
            original_curr_symbol = fixed_symbols[original_curr_symbol]
        if converted_curr_symbol in fixed_symbols.keys():
            converted_curr_symbol = fixed_symbols[converted_curr_symbol]

        # Project goal.
        if project_data:
            goal = float(project_data['goal']['amount'])
            converted_goal = goal * conversion_rate
        else:
            goal_elem = soup.select_one('span[class="block dark-grey-500 type-12 type-14-md lh3-lg"] > span')
            if goal_elem != None:
                goal = get_digits(goal_elem.contents[1].getText(), "int") 
                converted_goal = goal * conversion_rate

        # Pledged amount.
        if project_data:
            pledged = float(project_data['pledged']['amount'])
            converted_pledged = pledged * conversion_rate
        else:
            pledged_elem = soup.select_one('span[class="ksr-green-700"]')
            if pledged_elem != None:
                pledged = get_digits(pledged_elem.getText())
                converted_pledged = pledged * conversion_rate     

    elif status == successful:
        # No way to get conversion rate in a completed project.
        successful_goal_elem = soup.select_one('div[class="type-12 medium navy-500"] > span[class="money"]')
        completed_pledge_elem = soup.select_one('h3[class="mb0"] > span[class="money"]')
        if successful_goal_elem != None:
            original_curr_symbol = converted_curr_symbol = get_str(successful_goal_elem.getText(), {'.', ','})
            goal = converted_goal = get_digits(successful_goal_elem.getText(), "int")

        if completed_pledge_elem != None:
            pledged = converted_pledged = get_digits(completed_pledge_elem.getText())

    else:
        # No way to get conversion rate in a completed project.
        completed_goal_elem = soup.select_one('span[class="inline-block-sm hide"] > span[class="money"]')
        completed_pledge_elem = soup.select_one('span[class="soft-black"]')

        if completed_goal_elem != None:
            original_curr_symbol = converted_curr_symbol = get_str(completed_goal_elem.getText(), {'.', ','})
            goal = converted_goal = get_digits(completed_goal_elem.getText(), "int")
        elif project_data:
            goal = converted_goal = float(project_data['goal']['amount'])
            original_curr_symbol = converted_curr_symbol = project_data['goal']['symbol']

        if completed_pledge_elem != None:
            pledged = converted_pledged = get_digits(completed_pledge_elem.getText())
        elif project_data:
            pledged = converted_pledged = float(project_data['pledged']['amount'])

    data["original_curr_symbol"] = original_curr_symbol
    data["converted_curr_symbol"] = converted_curr_symbol   
    data["conversion_rate"] = conversion_rate
    data["goal"] = goal
    data["converted_goal"] = converted_goal
    data["pledged"] = pledged
    data["converted_pledged"] = converted_pledged

    # Campaign start time. Will be extracted from updates files
    # so just leave space for it to be added later.
    data["startday"] = MISSING
    data["startmonth"] = MISSING
    data["startyear"] = MISSING

    # Campaign end time.
    endday, endmonth, endyear = MISSING, MISSING, MISSING

    if project_data:
        dt = datetime.fromtimestamp(project_data['deadlineAt'])
        endday, endmonth, endyear = dt.day, dt.month, dt.year

    elif status != live:
        end_time_elem = soup.select('time[data-format="ll"]')
        if len(end_time_elem) >= 2:
            time_str = end_time_elem[1].attrs['datetime'][:10]
            dt = datetime.strptime(time_str, "%Y-%m-%d")
            endday, endmonth, endyear = dt.day, dt.month, dt.year
    else:
        end_time_elem = soup.select_one('p[class="mb3 mb0-lg type-12"]')
        if end_time_elem != None:
            time_str = end_time_elem.getText()[80:]
            dt = datetime.strptime(time_str, "%B %d %Y %I:%M %p %Z %z.")
            endday, endmonth, endyear = dt.day, dt.month, dt.year

    data["endday"] = endday
    data["endmonth"] = endmonth
    data["endyear"] = endyear

    # Number of images and photos.
    photos, videos = 0, 0
    # Get number of photos and videos in highlight.
    highlight_elem = soup.select_one('div[class="grid-row grid-row mb5-lg mb0-md order-0-md order-2-lg"]')
    if highlight_elem != None:
        photos += len(highlight_elem.select("img"))
        # Check either possible tag for video in highlight.
        videos += len(highlight_elem.select('svg[class="svg-icon__icon--play icon-20 fill-white"]')) or len(highlight_elem.select("video"))
    # Get number of photos and videos in description.
    description_container_elem = soup.select_one('div[class="col col-8 description-container"]')
    if description_container_elem != None:
        photos += len(description_container_elem.select("img"))

        videos += len(description_container_elem.select("video"))
        videos += len(description_container_elem.select('div[class="template oembed"]'))
        
    data["num_photos"] = photos
    data["num_videos"] = videos

    # Make 100 (make100), Projects we love (pwl), Category, Location. make100/pwl is 1 if project is 
    # part of it and otherwise 0.
    pwl = MISSING    
    make100 = MISSING
    category = MISSING
    subcategory = MISSING
    location = MISSING

    if status != successful:
            spc_cat_loc_elems = soup.select('span[class="ml1"]')
            spc_cat_loc_data = [pwl_cat_loc_elem.getText() for pwl_cat_loc_elem in spc_cat_loc_elems]

            if len(spc_cat_loc_data) >= 2:
                # Project is part of Projects we Love or Make 100.
                if "Project We Love" in spc_cat_loc_data:
                    pwl = 1
                else:
                    pwl = 0

                if "Make 100" in spc_cat_loc_data:
                    make100 = 1
                else:
                    make100 = 0

                cat_str = spc_cat_loc_data[-2]
                category, subcategory = get_category_data(cat_str)

                location = spc_cat_loc_data[-1]

            elif project_data:
                # No subcategory.
                if project_data['category']['parentCategory'] == None:
                    category = project_data['category']['name']
                else:
                    subcategory = project_data['category']['name']
                    category = project_data['category']['parentCategory']['name']
                
                # Converts True -> 1 and False -> 0
                pwl = int(project_data['isProjectWeLove'])

                location = project_data['location']['displayableName']
    else:   
        # Try alternate tags for successful campaigns
        pwl_elem = soup.select_one('svg[class="svg-icon__icon--small-k nowrap fill-white icon-14"]')
        if pwl_elem != None:
            pwl = 1
        else:
            pwl = 0

        # Successful projects don't have Make 100.
        make100 = 0

        cat_loc_elems = soup.select('a[class="grey-dark mr3 nowrap type-12"]')
        if len(cat_loc_elems) != 0:
            location = cat_loc_elems[0].getText().strip()
            category, subcategory = get_category_data(cat_loc_elems[1].getText().strip())

    data["pwl"] = pwl
    data["make100"] = make100
    data["category"] = category
    data["subcategory"] = subcategory
    data["location"] = location

    # Number of projects created.
    num_projects = MISSING
    if project_data and project_data['creator']:
            if 'createdProjects' in project_data['creator']:
                num_projects = project_data['creator']['createdProjects']['totalCount']
            else:
                num_projects = project_data['creator']['launchedProjects']['totalCount']

    data["num_projects"] = num_projects

    # Number of projects backed.
    num_backed = MISSING
    if project_data and project_data['creator']:
        if 'backedProjects' in project_data['creator']:
            if project_data['creator']['backedProjects'] != None:
                num_backed = project_data['creator']['backedProjects']['totalCount']
        else:
            num_backed = project_data['creator']['backingsCount']

    data["num_backed"] = num_backed 

    # Number of comments.
    comments_elem = soup.select_one('data[itemprop="Project[comments_count]"]')
    data["num_comments"] = comments_elem.getText()
    
    # Number of updates.
    updates_elem = soup.select_one('a[data-content="updates"] > span[class="count"]')
    data["num_updates"] = updates_elem.getText()

    # Number of faq.
    faq_elem = soup.select_one('a[data-content="faqs"]')
    # Kickstarter does not show 0 if there is no faq.
    if len(faq_elem.contents) > 1:
        data["num_faq"] = faq_elem.contents[1].getText()
    else:
        data["num_faq"] = 0

    # Description.
    description_elem = soup.select_one('div[class="full-description js-full-description responsive-media formatted-lists"]')
    if description_elem != None:
        description = description_elem.getText().strip()
    else:
        description = MISSING
    data["description"] = description
    
    # Risks.
    risk_elem = soup.select_one('div[class="mb3 mb10-sm mb3 js-risks"]')
    if risk_elem != None:
        risk = risk_elem.getText().strip()
        # Remove first line "Risks and challenges" and last line "Learn about accountability on Kickstarter"
        # because they are the same for all projects.
        risk = "".join(risk.splitlines(keepends=True)[1:-1])
    else:
        risk = MISSING
    data["risk"] = risk

    # Pledges. rd_gone is 0 for available pledges and 1 for complete pledges. 
    all_pledge_elems = []
    all_pledge_elems.extend([pledge_elem for pledge_elem in soup.select('li[class="hover-group js-reward-available pledge--available pledge-selectable-sidebar"]')])
    all_pledge_elems.extend([pledge_elem for pledge_elem in soup.select('li[class="hover-group pledge--all-gone pledge-selectable-sidebar"]')])
    all_pledge_elems.extend([pledge_elem for pledge_elem in soup.select('li[class="hover-group pledge--inactive pledge-selectable-sidebar"]')])

    data["num_rewards"] = len(all_pledge_elems)

    for i, pledge_elem in enumerate(all_pledge_elems):
        data |= get_pledge_data(pledge_elem, i)

    return data

if __name__ == "__main__":
    if not TESTING:
        main()
    else:
        test_extract_campaign_data()