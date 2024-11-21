import os
# import json
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
# import numpy as np


# Load credentials
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
creds = open(f'{root_dir}\\credentials.txt', 'r').readlines()
uname = creds[0].strip()
pkey = creds[1].strip()
db_name = 'chaos'

# Create the database URI
DATABASE_URI = f'postgresql+psycopg2://{uname}:{pkey}@localhost:5432/{db_name}'
engine = create_engine(DATABASE_URI)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()



# # CONNECT TO LOCAL SSH TUNNEL
# # ------------------------------------------------------------------------------------------------------------------ #

conn = psycopg2.connect(user=uname, password=pkey, database="chaos", host="localhost", port="5432")
conn.rollback()
c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


# QUERY DATABASE FOR LATEST BATCH NUMBER AND ASSIGNES THEM TO A VARIABLE FOR EACH one
# ------------------------------------------------------------------------------------------------------------------ #
def last_batch_check():

    with open(f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}\\Queries\\latest_batch.sql', 'r') as file:
        latest_batch_query = file.read()

    c.execute(latest_batch_query)

    latest_batch_query_results = c.fetchall()
    latest_batch_query_results.sort()


    latest_batch_query_columns_names = ['name', 'highest_batch_name']

    df_latest_batch_query_results = pd.DataFrame(latest_batch_query_results,columns=latest_batch_query_columns_names)



    list_of_regions = ['Japan', 'Peru', 'Philippines', 'UAE']

    region_batches = {}

    for region in list_of_regions:
        for index, row in df_latest_batch_query_results.iterrows():
            if row['name'].lower().startswith(region.lower()):
                region_batches[region] = row['highest_batch_name']



    japan_latest_batch = region_batches['Japan']
    peru_latest_batch = region_batches['Peru']
    philippines_latest_batch = region_batches['Philippines']
    uae_latest_batch = region_batches['UAE']

    return [['Japan',japan_latest_batch], ['Peru',peru_latest_batch], ['Philippines',philippines_latest_batch], ['UAE',uae_latest_batch]]
