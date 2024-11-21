import re
import os
import json
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np

from functions.latest_batch import last_batch_check

# Load credentials
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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

# Printing in the command line for Aestethics
# ------------------------------------------------------------------------------------------------------------------ #
print()
print("------------------------------------------------------------------------")


# PARSING THE .XLSX FILE THAT WILL BE PASSED THROUGh THE SCRIPT
# ------------------------------------------------------------------------------------------------------------------ #

cases_input_file_path = f'{os.path.dirname(os.path.abspath(__file__))}\\Cases_Id.xlsx'

df_cases_input = pd.read_excel(cases_input_file_path)

# Extract the values from the first column and convert them to a list
values = df_cases_input.iloc[:, 0].astype(str).tolist()  # Ensures they are strings

# Format each value as required
formatted_values = [f"'{value}'," for value in values[:-1]] + [f"'{values[-1]}'"]

cases_id = " ".join(formatted_values)

# PROMP THE USER FOR FIRST LOGICAL DECISION OF THE SCRIPT
# ------------------------------------------------------------------------------------------------------------------ #

for region in last_batch_check(): # returns a ordered list of list of latests batch per region
    with open(f'{os.path.dirname(os.path.abspath(__file__))}\\Queries\\contacts_query.sql', 'r') as file:
        contacts_query = file.read()

    country = region[0]
    current_region_batch = int(re.search(r'\d+$',region[1]).group())

    # Formating main query
    contacts_query = contacts_query.format(current_region_batch, current_region_batch, cases_id, country)

    # Now the execute command of the query
    c.execute(contacts_query)

    contacts_query_results = c.fetchall()
    contacts_query_results.sort()


    columns_names = ['entity_id', 'country', 'contact.id', 'contact.tags']

    df_contacts_query_results = pd.DataFrame(contacts_query_results, columns=columns_names)

    df_contacts_query_results = df_contacts_query_results.drop(['entity_id', 'country'], axis=1)



    #
    # ------------------------------------------------------------------------------------------------------------------ #
    excel_output_filepath = f'{os.path.dirname(os.path.abspath(__file__))}\\Chaos - {country} - Batch {current_region_batch} - Contacts.xlsx'
    df_contacts_query_results.to_excel(excel_output_filepath, index=False)

    print()
    print("-----------------------------------------------------")
    print(f'Output file succesfully created for {country}')
    print("-----------------------------------------------------")
