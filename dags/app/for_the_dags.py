from pymongo import MongoClient
from app.main import *
import warnings
import json
import os 
from airflow.models import Variable

warnings.filterwarnings("ignore", category=UserWarning, module="slack_sdk")

# Get the absolute path of the current script
script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)
# Construct the path to params.json relative to scraper.py
params_file = os.path.join(script_dir, "params.json")

# Open and load the JSON file with the necessary parameters
with open(params_file, "r") as file:
    data = json.load(file)

DB = "test"
COLLECTION_HREFS = "hrefs"
MAX_HREFS_PER_DAG = 50

def get_hrefs(soupObj) -> list:
    if isinstance(soupObj, str):
        raise ValueError("Error occurred while fetching soupObj. Stopping execution.")
        
    hrefs = get_list_hrefs_per_page(soupObj)
    shuffled_data = sample(hrefs, len(hrefs)) # Shuffle the list data.
    
    return shuffled_data

def scrape_hrefs(url, params) -> list:
    """
    It performs web scraping to retrieve hrefs (hyperlinks) from a given URL and parameters. It initializes an empty list 
    to store the hrefs and sends a request to the URL. It determines the number of pages in the search results and prints it. 
    Then, it retrieves the hrefs from the first page, adds them to the list, and introduces a random delay. If there are additional pages, 
    it retrieves the hrefs from each page, adds them to the list, and introduces a delay. 
    Finally, it returns the list of retrieved hrefs.
    """
    outer_list = []
    soupObj = request_response(url = url, headers = headers, params = params)
    num_pages = count_num_pages(soupObj)
    print(f"The number of pages in the search were: {num_pages}")
    
    outer_list.extend(get_hrefs(soupObj)) 
    sleep(randint(8, 12))
    if num_pages > 1:
        if num_pages > 2:
            for num_page in range(2, num_pages):
                print("Task page: {num_page}")
                soupObj = request_response(url, headers = headers, params = params, page = num_page)
                outer_list.extend(get_hrefs(soupObj)) 
                sleep(randint(8, 12))
            return outer_list
        else:
            print("Task page: 2")
            soupObj = request_response(url, headers = headers, params = params, page = 2)
            outer_list.extend(get_hrefs(soupObj)) 
            return outer_list
    else:        
        return outer_list
    
def add_hrefs_to_mongo(database_name=DB, collection_name=COLLECTION_HREFS, **context) -> bool:
    """
    adds the retrieved hrefs to a MongoDB collection. It retrieves the hrefs from the previous task's XCom using context['ti'].xcom_pull(), 
    and if there are hrefs available, it establishes a connection to the MongoDB server. It iterates over each href, 
    converts it into a dictionary format, and inserts it into the specified collection. The function also prints the 
    number of hrefs and saves the length of hrefs in the task's context using context['ti'].xcom_push()]. 
    Finally, it returns True to indicate successful execution.
    """
    hrefs = context['ti'].xcom_pull(task_ids='scrape_hrefs', key='return_value')
    print(hrefs)
    if hrefs:
        client = MongoClient('mongo:27017', username='root', password='password')

        # Establish MongoDB connection
        db = client[database_name]
        collection = db[collection_name]
        
        # Convert each tuple to a dictionary and insert into the collection
        for href in hrefs:
            doc = {'url': href[0], 'id': href[1]}
            collection.insert_one(doc)

        len_hrefs = len(hrefs)
        print(f"The number of hrefs was: {len_hrefs}")
        # Save the length of data in the task's context
        context['ti'].xcom_push(key='hrefs_length', value=len_hrefs)

        # Save the length of data in the results_slack.json file
        results = {'hrefs_length': len_hrefs}
        with open('app/results_slack.json', 'w') as file:
            json.dump(results, file)

        client.close()

        return True

def send_slack_number_of_hrefs() -> str:
    """
    Sending slack notifications over the number of urls gathered ans saved in the database.
    """

    with open('app/results_slack.json') as file:
        hrefs = json.load(file)
    num_of_hrefs = hrefs.get('hrefs_length', 0)

    if int(num_of_hrefs) > 0:
        message = f"The number of urls found and saved in the database was: {num_of_hrefs}"
    else:
        message = "No data were saved.."

    return str(message)

def calculate_num_dags(**context):
    """
    It calculates the number of DAGs needed based on the total number of hrefs obtained from the previous task's XCom. 
    It also calculates the delay between each DAG run.
    The function retrieves the total number of hrefs using context['ti'].xcom_pull(). It then calculates the number of 
    DAGs by dividing the total number of hrefs by the maximum hrefs per DAG (MAX_HREFS_PER_DAG) and adding 1.
    If there is only one DAG, the delay between DAG runs is set to 2 hours. Otherwise, the delay is calculated 
    by dividing the maximum duration in hours (max_duration_hours) by the number of DAGs.
    The function prints the total hrefs, the number of DAGs to be created, and the delay between DAG runs. 
    It also saves the calculated number of DAGs and delay in the task's context using context['ti'].xcom_push()].
    """
    total_hrefs = context['ti'].xcom_pull(task_ids='add_hrefs_to_mongo', key='hrefs_length')
    max_duration_hours = 12
    print(f"Total hrefs: {total_hrefs}")
    # Calculate the number of dags needed
    num_dags = (total_hrefs // MAX_HREFS_PER_DAG) + 1

    # Calculate the delay between each dag
    if num_dags == 1:
        delay_hours = 2  # Set delay to 0 when there's only one dag
    else:
        delay_hours = max_duration_hours / (num_dags)

    print(f"Number of dags going to create: {num_dags}")
    print(f"Distance in hours between dags runs: {delay_hours}")

    context['ti'].xcom_push(key='num_dags', value=num_dags)
    context['ti'].xcom_push(key='delay_hours', value=delay_hours)

    

def retrieve_and_remove_hrefs(database_name, collection_name, **context) -> bool:
    """
    It retrieves a specified number of hrefs from a MongoDB collection and removes them from the collection.
    Next, it retrieves the specified number of rows (num_rows) from the collection using the find() method with the limit() parameter. 
    The retrieved hrefs are stored in the retrieved_hrefs list.
    After retrieving the hrefs, the function deletes the corresponding rows from the collection using the delete_many() 
    method and the $in operator to match the _id values of the retrieved hrefs.
    Finally, the retrieved hrefs are pushed to the XCom using context['ti'].xcom_push()] with the key 'retrieved_hrefs'.
    """
    num_rows = MAX_HREFS_PER_DAG

    client = MongoClient('mongo:27017', username='root', password='password')
    db = client[database_name]
    collection = db[collection_name]

    # Retrieve the specified number of rows from the collection
    retrieved_hrefs = list(collection.find().limit(num_rows))

    print(f'Retrieved: {retrieved_hrefs}')
    # Convert ObjectId instances to strings
    hrefs_serializable = []
    for href in retrieved_hrefs:
        href_serializable = {**href, '_id': str(href['_id'])}
        hrefs_serializable.append(href_serializable)

    # Push the retrieved hrefs to XCom
    context['ti'].xcom_push(key='retrieved_hrefs', value=hrefs_serializable)
    
    # Remove the retrieved rows from the collection
    collection.delete_many({'_id': {'$in': [href['_id'] for href in retrieved_hrefs]}})
    client.close()

    return True


def process_retrieved_hrefs(database_name=DB, collection_name=COLLECTION_HREFS, **context) -> dict:
    """
    It processes a list of retrieved hrefs by scraping data from their corresponding URLs. It initializes a 
    dictionary to store the scraped data and establishes a connection to MongoDB using provided credentials. 
    The function attempts to create a remote WebDriver connection to the Selenium Grid. For each href, 
    it navigates to the URL, extracts detailed rental information, and stores it in the dictionary. 
    Any encountered errors are logged and the error count is stored. 
    The function returns the dictionary of processed data.
    """
    retrieved_hrefs = context['ti'].xcom_pull(task_ids='retrieve_and_remove_hrefs', key='retrieved_hrefs')
    print(f'Hrefs: {retrieved_hrefs}')
    len_hrefs = len(retrieved_hrefs)
    print(f'Number of urls: {len_hrefs}')
    
    client = MongoClient('mongo:27017', username='root', password='password')

    # Establish MongoDB connection
    db = client[database_name]
    collection = db[collection_name]
    
    chrome_options = configure_options(user_agent)
    
    inner_dict = {}
    driver = None  # Initialize the driver variable
    error_count = 0  # Variable to keep count of errors
    
    try:
        driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options)
        
        for i in range(len_hrefs):
            start = time.time()
            try:
                url = retrieved_hrefs[i]['url']
                print(f"This is the --{url}--")

                driver.get(url)
                time.sleep(3)
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')

                detailed_rental = get_details_from_div(soup)
                creation_id = str(uuid.uuid4())
                inner_dict[creation_id] = {
                    'web_id': retrieved_hrefs[i]['id'], 
                    'characteristics': detailed_rental,
                    'creation_date': creation_date()
                }

                stop = time.time()
                print(f"Iteration {i+1}: {stop - start} seconds")
                sleep(randint(4, 10))

            except Exception as e:
                print(f"Error processing iteration {i+1}: {str(e)}")
                print(f"Error in href: {url}")
                print(f"Error in href: {retrieved_hrefs[i]['url']}")
                error_count += 1

                # Add error href to the database
                doc = {'id': retrieved_hrefs[i]['id'], 'url': retrieved_hrefs[i]['url']}
                collection.insert_one(doc)
                
        # Close the MongoDB client connection
        client.close()

    except Exception as e:
        print(f"Error occurred outside the loop: {str(e)}")

    finally:
        if driver is not None:
            driver.quit()

    # Save the error count in the results_slack.json file
    results = {'error_count': error_count}
    with open('app/results_slack.json', 'w') as results_file:
        json.dump(results, results_file)

    return inner_dict

def send_slack_note_for_errors() -> str:
    """
    Sending slack notifications over the number of urls in which error occured.
    """

    with open('app/results_slack.json') as file:
        hrefs = json.load(file)
    error_count = hrefs.get('error_count', 0)

    if error_count > 0:
        message = f"Errors occurred during processing. Error count: {error_count}"
    else:
        message = "No errors occurred during processing."

    return message

def transfer_to_database_data(database_name, collection_name, **context) -> bool:
    """
    It handles the data transfer process to a MongoDB database. Here's a summary of what the function does:
    It retrieves the data from the previous task execution by using the xcom_pull method with the task ID 'scrape_data'.
    Then it establishes a connection to the MongoDB database and inserts the data into the collection, passing the values from the data dictionary.
    It saves the length of the data in the task's context and finally
    returns True to indicate the successful completion of the data transfer process.
    """
    data = context['ti'].xcom_pull(task_ids='process_retrieved_hrefs', key='return_value')
    if data:
        client = MongoClient('mongo:27017', username='root', password='password')

        # Establish MongoDB connection
        db = client[database_name]
        collection = db[collection_name]
        
        # Insert documents into the collection
        collection.insert_many(data.values())

        len_data = len(data)
        print(f"The number of items was: {len_data}")

        # Save the error count in the results_slack.json file
        results = {'data_length': len_data}
        with open('app/results_slack.json', 'w') as results_file:
            json.dump(results, results_file)

        client.close()

        return True
    
def slack_end() -> str:
    """
    It is responsible for generating a summary message based on the outcome of the data transfer process.
    """
    with open('app/results_slack.json') as file:
        hrefs = json.load(file)
    data_len = hrefs.get('data_length', 0)

    if data_len > 0:
        message = f"Data transfer complete. Total records transferred: {data_len}"
    else:
        message = "No data were transferred.."
    
    return message