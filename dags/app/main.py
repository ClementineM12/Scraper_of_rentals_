from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
from time import sleep
import uuid
from random import random, sample, randint
from fake_useragent import UserAgent
import json
import os 

# Get the absolute path of the current script
script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)
# Construct the path to params.json relative to scraper.py
params_file = os.path.join(script_dir, "params.json")

# Open and load the JSON file with the necessary parameters
with open(params_file, "r") as file:
    data = json.load(file)

from app.scraper import *
remote_webdriver = 'remote_chromedriver'
user_agent = UserAgent().chrome

def configure_options(user_agent) -> Options:
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run Chrome in headless mode
    chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration
    chrome_options.add_argument("--no-sandbox")  # Add the sandbox argument
    chrome_options.add_argument("--lang=el")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument('--referer=https://www.google.com')
    chrome_options.add_argument(f'user-agent={user_agent}')
    return chrome_options
"""
By default, Docker runs a container with a /dev/shm shared memory space 64MB. This is typically too small for Chrome and will cause Chrome to crash when rendering large pages. 
To fix, run the container with docker run --shm-size=1gb to increase the size of /dev/shm. Since Chrome 65, this is no longer necessary. Instead, launch the browser with the --disable-dev-shm-usage flag.
According to that, another idea would be to try using --shm-size=1gb when running the container if you really want to use /dev/shm.
"""
headers = data['headers']
def random_agent() -> dict:
    """
    Returns a dictionary with a single key-value pair where the key is "User-Agent" and the value is a randomly generated user agent string.
    """
    return {"User-Agent": UserAgent().chrome}

def scrape_data_by_adv(hrefs) -> dict:
    """
    The scrape_data_by_adv function performs web scraping on a list of hrefs using a remote ChromeDriver. 
    It extracts rental data from each URL and stores it in a dictionary called inner_dict. The function returns the inner_dict containing the scraped data.
    """
    chrome_options = configure_options(user_agent)
    
    inner_dict = {}
    driver = None  # Initialize the driver variable
    try:
        driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options)
        
        for i, href in enumerate(hrefs):
            start = time.time()
            try:
                url = href[0]
                print(f"This is the --{url}--")

                driver.get(url)
                time.sleep(3)
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')

                detailed_rental = get_details_from_div(soup)
                creation_id = str(uuid.uuid4())
                inner_dict[creation_id] = {
                    'web_id': href[1], 
                    'characteristics': detailed_rental,
                    'creation_date': creation_date()
                }

                stop = time.time()
                print(f"Iteration {i+1}: {stop - start} seconds")
                sleep(randint(1, 5))

            except Exception as e:
                print(f"Error processing iteration {i+1}: {str(e)}")
                print(f"Error in href: {href[1]}")
                # You can choose to handle the error or raise it to the caller
                # raise

    except Exception as e:
        print(f"Error occurred outside the loop: {str(e)}")

    finally:
        if driver is not None:
            driver.quit()

    return inner_dict

def get_hrefs_of_page(soupObj) -> list:
    """
    The get_hrefs_of_page function takes a soupObj parameter and checks if it's an instance of a string. If it is, it raises a ValueError with an error message.
    Next, it retrieves a list of hrefs from the soupObj using the get_list_hrefs_per_page function. Then, it shuffles the hrefs using random.sample to create a new list called shuffled_data.
    Finally, it calls the scrape_data_by_adv function with the shuffled_data list as an argument and returns the result.
    """
    if isinstance(soupObj, str):
        raise ValueError("Error occurred while fetching soupObj. Stopping execution.")
        
    hrefs = get_list_hrefs_per_page(soupObj)
    shuffled_data = sample(hrefs, len(hrefs)) # Shuffle the list data.
    
    return scrape_data_by_adv(shuffled_data)

def scrape_all_data(url, params) -> dict:
    """
    Inside the function, an empty dictionary called outer_dict is initialized. Then, a request is made to the specified url using 
    the request_response function, passing the url, a random user agent generated by the random_agent function, and the params as headers and parameters, respectively. 
    The response is assigned to soupObj.
    The outer_dict is updated with the result of calling the get_hrefs_of_page function with soupObj as an argument.
    The function checks the num_pages obtained from the soupObj. If num_pages is greater than 1, it enters a loop to 
    iterate over the range from 2 to num_pages. Inside the loop, requests are made to subsequent pages by calling the request_response 
    function with the appropriate parameters. The resulting soupObj is then used to update the outer_dict using the get_hrefs_of_page function. 
    A random sleep interval is introduced between requests using sleep from the time module.
    Finally, the outer_dict is returned. If num_pages is not greater than 1, the function returns the empty outer_dict.
    """
    
    outer_dict = {}  # Outer dictionary
    
    soupObj = request_response(url = url, headers = headers, params = params)
    num_pages = count_num_pages(soupObj)
    print(f"The number of pages in the search were: {num_pages}")

    outer_dict.update(get_hrefs_of_page(soupObj)) 
    sleep(randint(1, 5))
    if num_pages > 1:
        if num_pages > 2:
            for num_page in range(2, num_pages):
                soupObj = request_response(url, headers = headers, params = params, page = num_page)
                outer_dict.update(get_hrefs_of_page(soupObj)) 
                sleep(randint(1, 5))
            return outer_dict
        else:
            soupObj = request_response(url, headers = headers, params = params, page = 2)
            outer_dict.update(get_hrefs_of_page(soupObj)) 
            return outer_dict
    else:        
        return outer_dict