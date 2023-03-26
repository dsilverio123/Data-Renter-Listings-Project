from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_for_events123',
    default_args=default_args,
    schedule_interval=None
)

def api_to_text():
    import requests
    from datetime import datetime

    url = "https://realtor.p.rapidapi.com/properties/v3/list"

    payload = {
        "limit": 200,
        "offset": 0,
        "baths": {"min": 3},
        "list_price": {
            "max": 900,
            "min": 200
        },
        "beds": {
            "max": 3,
            "min": 1
        },
        "cats": True,
        "dogs": True,
        "state_code": "GA",
        "status": ["for_rent"],
        "type": ["condos", "condo_townhome_rowhome_coop", "condo_townhome", "townhomes", "duplex_triplex", "single_family", "multi_family", "apartment", "condop", "coop"],
        "sort": {
            "direction": "desc",
            "field": "list_date"
        }
    }
    headers = {
        "content-type": "application/json",
        "X-RapidAPI-Key": " ",
        "X-RapidAPI-Host": "realtor.p.rapidapi.com"
    }

    response = requests.request("POST", url, json=payload, headers=headers)

    print(response.text)

    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d_%H-%M-%S')

    filename = f'api_call_{date_str}.txt'

    with open(filename, "w") as f:
        f.write(response.text)

api_to_text_task = PythonOperator(
    task_id='api_to_text',
    python_callable=api_to_text,
    dag=dag
)

def text_to_csv():

    import json
    import csv
    import requests
    from datetime import datetime
    import os
    import glob

    # Define the filename criteria
    filename_criteria = 'api_call_2023*.txt'

    # Get a list of all files in the current directory that match the criteria
    matching_files = glob.glob(filename_criteria)

    # Find the most recently created file
    most_recent_file = max(matching_files, key=os.path.getctime)

    # Open the file and read its contents
    with open(most_recent_file, 'r') as f:
        response_text = f.read()
        
    # Parse the JSON response
    response_json = json.loads(response_text)

    # Extract the data you want to save to CSV
    data = response_json['data']['home_search']['results']

    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d_%H-%M-%S')

    filename = f'rental_proporties_ga_{date_str}.csv'


    # Define the CSV file name

    # Open the CSV file for writing
    with open(filename, 'w', newline='') as f:
        # Define the CSV header row
        header = ['property_id', 'listing_id', 'status', 'city', 'state_code', 'postal_code', 'beds', 'baths', 'sqft', 'list_price']
        writer = csv.DictWriter(f, fieldnames=header)

        # Write the header row to the CSV file
        writer.writeheader()

        # Write each row of data to the CSV file
        for result in data:
            row = {
                'property_id': result['property_id'],
                'listing_id': result['listing_id'],
                'status': result['status'],
                'city': result['location']['address']['city'],
                'state_code': result['location']['address']['state_code'],
                'postal_code': result['location']['address']['postal_code'],
                'beds': result['description']['beds'],
                'baths': result['description']['baths'],
                'sqft': result['description']['sqft'],
                'list_price': result['list_price']
            }
            writer.writerow(row)
    pass

text_to_csv_task = PythonOperator(
    task_id='text_to_csv',
    python_callable=text_to_csv,
    dag=dag
)

def csv_to_postgresql():
    import os
    import glob
    import csv
    import psycopg2

    # database connection parameters
    DB_HOST = " "
    DB_NAME = " "
    DB_USER = " "
    DB_PASS = " "

    # file name pattern to match for the CSV file
    FILE_NAME_PATTERN = "rental_proporties_ga_*.csv"

    # extract columns to insert into the database
    COLUMNS = ["property_id", "listing_id", "status", "city", "state_code", "postal_code", "beds", "baths", "sqft", "list_price"]

    # connect to the database
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)

    # find the most recent CSV file that matches the file name pattern
    latest_file = max(glob.glob(FILE_NAME_PATTERN), key=os.path.getctime)

    # read the CSV file and insert rows into the database
    with open(latest_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # check if the row already exists in the database (assuming property_id and listing_id are unique)
            cur = conn.cursor()
            cur.execute("SELECT * FROM renters_table2 WHERE property_id = %s AND listing_id = %s", (row["property_id"], row["listing_id"]))
            existing_row = cur.fetchone()
            if existing_row:
                print(f"Skipping duplicate row: {row}")
            else:
                # insert the row into the database
                values = [row[col] for col in COLUMNS]
                cur.execute("INSERT INTO renters_table2 (property_id, listing_id, status, city, state_code, postal_code, beds, baths, sqft, list_price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", values)
                conn.commit()
                print(f"Inserted row: {row}")

    # close the database connection
    conn.close()

    pass

csv_to_postgresql_task = PythonOperator(
    task_id='csv_to_postgresql',
    python_callable=csv_to_postgresql,
    dag=dag
)

api_to_text_task >> text_to_csv_task >> csv_to_postgresql_task
