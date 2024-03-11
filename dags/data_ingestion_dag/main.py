# Import necessary libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlite3
import os
from airflow.operators.email_operator import EmailOperator
import pyspark
from pyspark.sql import SparkSession 





# Get dag directory path
dag_path = os.getcwd()
spark = SparkSession.builder.appName('Practise').getOrCreate() #create a spark session

# Establish connection with the SQLite DB
conn = sqlite3.connect("/usr/local/airflow/db/declaredTickets.db")
cursor = conn.cursor()

def check_and_create_table():
    # Check if 'tickets' table exists and if not, create it
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            ticketID INT,
            ticket_creation_time TEXT,
            ticket_status TEXT,
            day INT,
            predicted_time TEXT
        )
    """)
    conn.commit()


def check_ticket_data():
    # Check if tickets data exists in batchTickets.csv
    try:
        tickets = pd.read_csv(f"{dag_path}/raw_data/batchTickets.csv", low_memory=False)
        if tickets.empty:
            print("No new tickets to declare.")
            return None
        else:
            return tickets.to_dict('records')
    except FileNotFoundError:
        print("No new tickets to declare.")
        return None

def filter_tickets(**context):
    # Read the declared tickets from the declaredTickets DB
    declared_tickets = pd.read_sql_query("SELECT * from tickets", conn)

    # Get new tickets data
    new_tickets_dict = context['task_instance'].xcom_pull(task_ids='check_ticket_data')
    if new_tickets_dict is None:
        return None
    new_tickets = pd.DataFrame(new_tickets_dict)

    # Filter out declared tickets from the new tickets data
    new_tickets = new_tickets[~new_tickets['ticketID'].isin(declared_tickets['ticketID'])]

    if new_tickets.empty:
        print("No new tickets to declare.")
        return None
    else:
        return new_tickets.to_dict('records')

def predict_resolution_time(**context):
    # Get new tickets data
    new_tickets_dict = context['task_instance'].xcom_pull(task_ids='filter_tickets')
    if new_tickets_dict is None:
        return None
    new_tickets = pd.DataFrame(new_tickets_dict)

    # Convert 'ticket_creation_time' to datetime and extract the day
    new_tickets['ticket_creation_time'] = pd.to_datetime(new_tickets['ticket_creation_time'])
    new_tickets['day'] = new_tickets['ticket_creation_time'].dt.day

    # Convert 'ticket_creation_time' to string
    new_tickets['ticket_creation_time'] = new_tickets['ticket_creation_time'].astype(str)

    # Predict resolution time based on the day
    new_tickets['predicted_time'] = new_tickets['day'].apply(lambda x: '3 hours' if x <= 10 else '10 hours')

    return new_tickets.to_dict('records')

def store_results(**context):
    # Get predicted tickets data
    predicted_tickets_dict = context['task_instance'].xcom_pull(task_ids='predict_resolution_time')
    if predicted_tickets_dict is None:
        return None
    predicted_tickets = pd.DataFrame(predicted_tickets_dict)

    # Store the results in SQLite DB
    predicted_tickets.to_sql('tickets', conn, if_exists='append', index=False)



# Initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'Tickets_ML_Pipeline',
    default_args=default_args,
    description='Aggregates booking records for data analysis',
    schedule_interval=timedelta(days=1),
    catchup=False
)

task_0 = PythonOperator(
    task_id='check_ticket_data',
    python_callable=check_ticket_data,
    dag=ingestion_dag,
)

task_1 = PythonOperator(
    task_id='filter_tickets',
    python_callable=filter_tickets,
    provide_context=True,
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='predict_resolution_time',
    python_callable=predict_resolution_time,
    provide_context=True,
    dag=ingestion_dag,
)

task_3 = PythonOperator(
    task_id='store_results',
    python_callable=store_results,
    provide_context=True,
    dag=ingestion_dag,
)

email = EmailOperator(
    task_id='send_email',
    to='aganithshanbhag@gmail.com',
    subject='Airflow Alert',
    html_content='Your task has been executed successfully.',
    dag=ingestion_dag,
)


check_and_create_table()

task_0 >> task_1 >> task_2 >> task_3 >> email