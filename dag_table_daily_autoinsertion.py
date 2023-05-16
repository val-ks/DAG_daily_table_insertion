# importing libraries
from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import requests
from io import StringIO
import numpy as np
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# connecting to clickhouse
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

# standard arguments for tasks
default_args = {
    'owner': 'val_ks',
    'depends_on_post': False, 
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 25),
}

# DAG interval: mins, hours, days, etc
schedule_interval = '0 15 * * *'

# main DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_val_ks_task6():
    
    @task()
    def extract_feed():
        query = """SELECT user_id, toDate(time) event_date, 
                gender, age, os, 
                countIf(action = 'like') likes,
                countIf(action = 'view') views
                FROM simulator_20230220.feed_actions
                WHERE toDate(time) = today()-1
                GROUP BY event_date, user_id, gender, age, os
                    format TSVWithNames"""
        feed = ch_get_df(query)
        return feed
    
    @task()
    def extract_message():
        query = """SELECT user_id, event_date, gender, age, os, 
                        messages_sent, messages_received, users_sent, users_received 
                    FROM (SELECT user_id, toDate(time) event_date, gender, age, os,
                             count() messages_sent,
                             uniq(reciever_id) users_sent
                            FROM simulator_20230220.message_actions
                            WHERE toDate(time) = today()-1
                            GROUP BY event_date, user_id, gender, age, os) A
                    FULL outer JOIN 
                        (SELECT reciever_id as user_id, toDate(time) event_date,
                              COUNT() messages_received, 
                              uniq(user_id) users_received
                            FROM simulator_20230220.message_actions
                            WHERE toDate(time) = today()-1
                            GROUP BY user_id, event_date) B
                    on A.user_id = B.user_id 
                    WHERE event_date = today()-1
                    format TSVWithNames"""
        message = ch_get_df(query)
        return message
    
    @task
    def join_tables(feed, message):
        joint = feed.merge(message, on=['user_id', 'gender', 'os', 'age', 'event_date'], how='outer')
        return joint

    @task
    def get_gender(joint):
        gender = joint[
            ['event_date', 'gender', 'likes', 'views', 'messages_sent', 'messages_received', 'users_sent',
             'users_received']] \
            .groupby(['event_date', 'gender'], as_index=False).sum().rename(columns={'gender': 'dimension_value'})
        gender['dimension'] = 'gender'
        with open('gender.csv', 'w') as f:
            f.write(gender.to_csv(index=False))
    
    @task
    def get_os(joint):
        os = joint[
            ['event_date', 'os', 'likes', 'views', 'messages_sent', 'messages_received', 'users_sent',
             'users_received']] \
            .groupby(['event_date', 'os'], as_index=False).sum().rename(columns={'os': 'dimension_value'})
        os['dimension'] = 'os'
        with open('os.csv', 'w') as f:
            f.write(os.to_csv(index=False))
            
    @task
    def get_age(joint):
        age = joint[
            ['event_date', 'age', 'likes', 'views', 'messages_sent', 'messages_received', 'users_sent',
             'users_received']] \
            .groupby(['event_date', 'age'], as_index=False).sum().rename(columns={'age': 'dimension_value'})
        age['dimension'] = 'age'
        with open('age.csv', 'w') as f:
            f.write(age.to_csv(index=False))

    @task
    def load(gender, os, age):
        gender = pd.read_csv('gender.csv')
        os = pd.read_csv('os.csv')
        age = pd.read_csv('age.csv')

        test = pd.concat([gender, os, age]).reset_index().drop(columns=['index'])\
                        [['event_date','dimension','dimension_value','views','likes','messages_received','messages_sent','users_received','users_sent']]
         
        connection_test = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': '656e2b0c9c',
            'user': 'student-rw',
            'database': 'test'
        }

        print(test)
        print('creating table...')
        q = """CREATE TABLE IF NOT EXISTS test.val_ks_task6
                      (event_date Date, 
                      dimension String, 
                      dimension_value String, 
                      views Float64, 
                      likes Float64, 
                      messages_received Float64,
                      messages_sent Float64,
                      users_received Float64,
                      users_sent Float64)
                     ENGINE = MergeTree()
                     ORDER BY event_date"""
        ph.execute(connection=connection_test, query=q)
        print('table created. Sending to database')
        ph.to_clickhouse(test, 'val_ks_task6', connection=connection_test, index=False)
        print('Database updated.')
        

    # completing tasks
    extract_feed = extract_feed()
    extract_message = extract_message()
    joint = join_tables(extract_feed, extract_message)
    gender = get_gender(joint)
    os = get_os(joint)
    age = get_age(joint)
    load(gender, os, age)
    
dag_val_ks_task6 = dag_val_ks_task6()
