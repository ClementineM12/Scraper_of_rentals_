from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import timedelta, datetime
import os
import json

file_path = os.path.join(os.path.dirname(__file__), "app")
# Import the scraper module
from app.main import *
from app.for_the_dags import *

DB = "test"
COLLECTION_HREFS = "hrefs"
COLLECTION_ADVS = "rentals"

"""
        1. Checking if the params_file exists: The code is using the os.path.exists() function to check if the params_file (presumably a file path) exists.

        2. Checking if the params_file is not empty: The code then checks if the params_file has a file size greater than 0 using os.path.getsize(). 
           This ensures that the file is not empty before proceeding.

        3. Loading schedule from JSON file: If the params_file exists and is not empty, the code opens the schedule.json file and loads its content using json.load(). 
           The loaded data is stored in the schedule variable.

        4. Creating a DAG: The create_dag() function is defined, which takes dag_id, schedule, and default_args as arguments. 
           It creates an instance of DAG with the specified parameters, including schedule interval, concurrency, and default arguments.

        5. Defining tasks and setting dependencies: Inside the create_dag() function, specific tasks are defined using different operators like DummyOperator and PythonOperator. 
           These tasks represent different steps in the workflow. Dependencies between the tasks are established using the >> operator, indicating the order of execution.

        6. Creating DAG instances dynamically: Finally, a loop iterates over the schedule data, extracting dag_id, schedule, and default_args for each entry. 
           It then dynamically creates DAG instances by calling create_dag() and assigns them to global variables with the dag_id as the variable name.

        The overall purpose of the code seems to be dynamically creating DAGs in Apache Airflow based on the schedule data loaded from the schedule.json file.
"""

schedule_file = "app/schedule.json"
if os.path.exists(schedule_file):
      if os.path.getsize(schedule_file) > 0:
            with open('app/schedule.json', 'r') as schedule_file:
                 schedule = json.load(schedule_file)

def create_dag(dag_id, schedule, default_args):
      
    dag = DAG(
            dag_id, schedule_interval=schedule,
            concurrency=20,
            max_active_runs=1,
            catchup=False,
            default_args=default_args,
            dagrun_timeout=timedelta(minutes=45)
            )
    
    # Task definitions
    def delete_record_from_json_file(dag_id, json_file_path):
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)

        # Find the record with the given dag_id
        for record in data:
            if record['dag_name'] == dag_id:
                data.remove(record)
                break

        with open(json_file_path, 'w') as json_file:
            json.dump(data, json_file)

    start = DummyOperator(
            task_id='start',
            dag=dag)

    retrieve_and_remove_hrefs_task = PythonOperator(
                task_id='retrieve_and_remove_hrefs',
                python_callable=retrieve_and_remove_hrefs,
                op_kwargs={'database_name': DB, 'collection_name': COLLECTION_HREFS},
                provide_context=True,
                dag=dag
            )

    process_retrieved_hrefs_task = PythonOperator(
                task_id='process_retrieved_hrefs',
                python_callable=process_retrieved_hrefs,
                op_kwargs={'database_name': DB, 'collection_name': COLLECTION_HREFS},
                dag=dag
            )
    
    send_slack_note_for_errors_task = SlackWebhookOperator(
            task_id="send_slack_note_for_errors",
            http_conn_id="slack_conn",
            message= send_slack_note_for_errors(),
            channel="#monitoring",
            dag=dag
        )

    transfer_to_db_task = PythonOperator(
                task_id='transfer_to_database_data',
                python_callable=transfer_to_database_data,
                op_kwargs={'database_name': DB, 'collection_name': COLLECTION_ADVS},
                dag=dag
            )
    
    send_slack_end_task = SlackWebhookOperator(
            task_id="send_slack_length_of_data_transfered",
            http_conn_id="slack_conn",
            message= slack_end(),
            channel="#monitoring",
            dag=dag
        )
    
    delete_record_task = PythonOperator(
        task_id='delete_record_from_json',
        python_callable=delete_record_from_json_file,
        op_kwargs={'dag_id': dag_id, 'json_file_path': 'app/schedule.json'},
        dag=dag
    )

    finish = DummyOperator(
            task_id='finish',
            dag=dag)
    
    start >> retrieve_and_remove_hrefs_task >> process_retrieved_hrefs_task >> transfer_to_db_task >> delete_record_task >> finish
    process_retrieved_hrefs_task >> send_slack_note_for_errors_task
    transfer_to_db_task >> send_slack_end_task 

                                                           
    return dag

for x in schedule:
        
        dag_id = '{}'.format(x['dag_name'])
        schedule = '{}'.format(x['cron'])
        default_args = {
                        'owner': 'airflow',
                        'depends_on_past': False,
                        'start_date': datetime.datetime(2023, 6, 8),
                        'retries': 3,
                        'retry_delay': timedelta(minutes=1),
                        }

        globals()[dag_id] = create_dag(dag_id, schedule, default_args)