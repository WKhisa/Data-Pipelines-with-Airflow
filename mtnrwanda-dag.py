from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@daily')

def extract_data():
    # extract data from CSV files
    customer_data_df = pd.read_csv('https://raw.githubusercontent.com/WKhisa/Data-Pipelines-with-Airflow/main/customer_data.csv')
    order_data_df = pd.read_csv('https://raw.githubusercontent.com/WKhisa/Data-Pipelines-with-Airflow/main/order_data.csv')
    payment_data_df = pd.read_csv('https://raw.githubusercontent.com/WKhisa/Data-Pipelines-with-Airflow/main/payment_data.csv')
    return customer_data_df,order_data_df,payment_data_df

def transform_data(customer_data_df,order_data_df,payment_data_df):
    # convert date fields to the correct format using pd.to_datetime
    customer_df['date_of_birth'] = pd.to_datetime(customer_df['date_of_birth'])
    order_df['order_date'] = pd.to_datetime(order_df['order_date'])
    payment_df['payment_date'] = pd.to_datetime(payment_df['payment_date'])

    # merge customer and order dataframes on the customer_id column
    merged_df = pd.merge(customer_df, order_df, on='customer_id')

    # merge payment dataframe with the merged dataframe on the order_id and customer_id columns
    merged_df = pd.merge(merged_df, payment_df, on=['order_id', 'customer_id'])

    # drop unnecessary columns like customer_id and order_id
    merged_df.drop(['customer_id', 'order_id'], axis=1, inplace=True)

    # group the data by customer
    agg_df = merged_df.groupby('customer_name').agg({'amount_paid': 'sum'})

    # create a new column to calculate the total value of orders made by each customer
    clean_df['total_order_value'] = merged_df.groupby('customer_name').agg({'order_value': 'sum'})

    # calculate the customer lifetime value using the formula CLV = (average order value) x (number of orders made per year) x (average customer lifespan) 
    clean_df['lifespan'] = (max(merged_df['payment_date']) - min(merged_df['payment_date'])).days / 365
    clean_df['purchase_frequency'] = merged_df.groupby('customer_name').size() / len(merged_df['payment_date'].unique())
    clean_df['avg_order_value'] = clean_df['total_order_value'] / clean_df['purchase_frequency']
    clean_df['clv'] = (clean_df['avg_order_value'] * clean_df['purchase_frequency']) * clean_df['lifespan']                    
    return clean_df
    
def load_data():
    # load the transformed data into Postgres database
    conn = psycopg2.connect(host=pg_host, database=pg_database, user=pg_user, password=pg_password)    
    clean_df.to_sql('airflow', con=conn, if_exists='replace',index=False)
    conn.autocommit = True    

default_args = {
    'owner': 'XYZ Telecoms',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'MTN_DAG',
    default_args=default_args,
    description='A DAG to extract,clean and save MTN data',
    schedule_interval='@daily',
) as dag:
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    extract_data >> transform_data >> load_data
   
