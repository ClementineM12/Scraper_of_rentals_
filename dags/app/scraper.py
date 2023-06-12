from bs4 import BeautifulSoup
import requests
import re
import datetime
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

greek_month_names = data["greek_month_names"]
media_ = data["media"]

def creation_date() -> str:
    """
    It uses the datetime module to get the current date and time (datetime.datetime.now()) 
    and then formats it using the strftime method to represent it as a string in the format 
    "YYYY-MM-DD HH:MM:SS".
    """
    dt = datetime.datetime.now()
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def count_num_pages(soupObj) -> int :
    """
    It counts the number of <li> elements within the pagination component and 
    returns the count as an integer. 
    If there is only one <li> element, it returns 1 as the total number of pages.
    """
    pagination = soupObj.find('div', {'class': 'pagination-component'})

    li_elements = pagination.find_all('li')
    if len(li_elements) > 1:
        try:
            num = int(li_elements[-1].text.strip())
            return num
        except ValueError:
            print("Not a number")
    else:
        return 1

def find_unique_id(div, regex = r'/(?P<number>\d+)/') -> int:
    """
    Τhe function is designed to extract a number from a string using a regular 
    expression and return it as an integer if possible. If the conversion fails, 
    it returns the extracted number as a string.
    """
    match = re.search(regex, div)

    if match:
        extracted_number = match.group('number')
        try:
            u_id = int(extracted_number)
            return u_id
        except ValueError:
            return extracted_number
        
def request_response(url, headers, params = None, page = 1) -> BeautifulSoup:
    """
    It sends a GET request to the specified url with optional 
    headers and params. If the response status code is 200 (indicating a successful request), 
    the HTML content of the response is obtained from response.content. This content is 
    then parsed using BeautifulSoup with the 'html.parser' parser and returned as a 
    BeautifulSoup object. If the response status code is not 200, an error message is printed to the console.
    """
    if page != 1:
        params['page'] = page
        
    response = requests.get(url, headers = headers, params = params)

    if response.status_code == 200:
        html_content = response.content
        return BeautifulSoup(html_content, 'html.parser')
    else:
        print(f"Error {response.status_code} : {response.reason}")
        
def get_list_hrefs_per_page(soupObj) -> list:
    """
    It takes a BeautifulSoup object and retrieves a list of href values 
    from anchor tags ('a') within specific HTML elements. It searches for 
    a div with the class 'cell large-6 tiny-12 results-grid-container' 
    and finds all divs with the class 'common-property-ad-body grid-y align-justify' 
    within it. For each of these divs, it retrieves the href value from the 
    corresponding anchor tag using div.find('a')['href'] and creates a list of these href values.
    """
    body = soupObj.find('div', {'class': 'cell large-6 tiny-12 results-grid-container'})
    
    divs = body.find_all('div', attrs={'class': 'common-property-ad-body grid-y align-justify'})

    return [(url.find('a')['href'], find_unique_id(str(url))) for url in divs]

def change_date_format(date_string) -> str:
    """
    It takes a date string and converts it to a different date format.
    It replaces any Greek month names in the date string with 
    their English equivalents using the dictionary greek_month_names. 
    Then, parses the modified date string into a datetime object, 
    assuming the original format as '%d %B %Y' (e.g., '7 June 2023').
    """
    date_format = '%d-%m-%y'

    for greek_month, english_month in greek_month_names.items():
        date_string = date_string.replace(greek_month, english_month)

    date_obj = datetime.datetime.strptime(date_string, '%d %B %Y')
    formatted_date = date_obj.strftime(date_format)

    return formatted_date

def num_images(soup) -> int:
    """
    Takes a BeautifulSoup object and returns the number of images.
    It searches for the section with the class "image-gallery" and
    retrieves the count of images from the corresponding HTML elements.
    """
    images_section = soup.find('section', class_='image-gallery')
    
    if images_section is not None:
        image_count_span = images_section.select_one('div.photo-actions button[data-testid="image-count-icon"] span')
        if image_count_span is not None:
            return int(image_count_span.text)
    
    return None

def split_title(text, regex = r'(?=\d)') -> tuple:
    """
    Takes a text string and splits it into two parts based on the 
    first occurrence of a digit. The split is performed using the pattern r'(?=\d)'. 
    If the split results in two parts, they are stripped of leading and trailing 
    whitespace. The first part is assigned to first_part, and the second part 
    is assigned to second_part. If the split results in only one part, first_part 
    is assigned the entire text string, and second_part is set to an empty string.
    """
    split_parts = re.split(regex, text, 1)

    if len(split_parts) > 1:
        first_part = split_parts[0].strip()
        second_part = int(split_parts[1].split()[0].strip())
    else:
        first_part = text.strip()
        second_part = ""
    
    return first_part, second_part

def return_price(text, regex = r'\d+(?:\.\d+)?') -> int:
    """
    Takes a text string and extracts the first number from it using regular expressions. 
    It finds all numbers in the text. If at least one number is found, the first number in 
    the list is assigned to first_number. If first_number contains a decimal point ('.'), 
    it is replaced with an empty string and converted to an integer.
    """
    numbers = re.findall(regex, text)

    if numbers:
        first_number = numbers[0]
        if '.' in first_number:
            first_number = int(first_number.replace('.', ''))
            return first_number
        return int(first_number)
    
def return_details(details) -> dict:
    """
    The function extracts details based on the specified pattern and structure of the 
    'li' elements, creating a dictionary where the extracted class names are used as 
    keys and the associated values are used as values.
    """
    empty_d = {}
    
    list_li = details.find_all('li')
    for i, li in enumerate(list_li):
        pattern = r'xe xe-(\w+)'
        regex = re.compile(pattern)
        span = list_li[i].find('span', class_=regex)

        class_name = span['class'][1] if span else None

        text = list_li[i].find('span', class_=class_name).find_next_sibling('span').text

        key = class_name.split('-')[-1] if class_name else None
        value = text.split(':')[-1].strip() if text else None

        if key:
            empty_d[key] = value
            
    return empty_d

def return_statistics(soup) -> dict:
    """
    It is designed to parse HTML content using BeautifulSoup and retrieve 
    specific information from a particular section of the webpage. It can 
    be used to extract statistics such as creation date, last update, 
    number of visitors, favorite count, and id.
    """
    result = {}
    statistics = soup.find('section', {'class': 'grid-x statistics align-left'})

    for stat in statistics.find_all('p'):
        if '|' not in stat.text:
            matched_values = stat.text.split(':')

            if len(matched_values) == 2:
                greek_word = matched_values[0].strip()
                value = matched_values[1].strip()
                for greek, english in media_.items():
                    if greek_word == greek:
                        result[english] = value
                        break
                        
    if 'refId' in result:
        result['refId'] = result['refId'].split(':')[-1].strip()
    if 'created_at' in result:
        result['created_at'] = change_date_format(result['created_at'])
    if 'last_update' in result:
        result['last_update'] = change_date_format(result['last_update'])
        
    return result

def get_details_from_div(soup) -> dict:
    """
    The function utilizes Selenium WebDriver to load and navigate webpages 
    dynamically, allowing for extraction of data from pages with dynamic content. 
    It combines the usage of various helper functions to extract specific details 
    from different sections of the webpage.
    """

    images = num_images(soup)
    listing, m2 = split_title(soup.find('div', {'class': 'title'}).text.strip())
    location = soup.find('div', class_='address').text.strip()
    price = return_price(soup.find('div', {"class":'price'}).text)
    
    the_details = soup.find('div', {'class':'characteristics'})
    details = return_details(the_details)
    
    media = return_statistics(soup)
    
    return {
        'images' : images,
        'listing': listing,
        'm2': m2,
        'location': location,
        'price': price,
        'details': details,
        'media' : media
    }