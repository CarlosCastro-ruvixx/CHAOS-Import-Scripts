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
# # ------------------------------------------------------------------------------------------------------------------ #
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
creds = open(f'{root_dir}\\credentials.txt', 'r').readlines()
uname = creds[0].strip()
pkey = creds[1].strip()
db_name = 'chaos'


# Create the database URI
# # ------------------------------------------------------------------------------------------------------------------ #
DATABASE_URI = f'postgresql+psycopg2://{uname}:{pkey}@localhost:5432/{db_name}'
engine = create_engine(DATABASE_URI)


# # ------------------------------------------------------------------------------------------------------------------ #
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


# User Prompt to check if all previous batches launched and runs the script accordingly
# ------------------------------------------------------------------------------------------------------------------ #

while True:
    first_question = input('Did all previous batches launch? (Y/N): ').upper()
    if first_question == 'Y':

        # print('the script runs')

        # LOOPING THROUGH THE RESULTS OF last_batch_check(), EXECUTING THE MAIN QUERY PER REGION
        # ------------------------------------------------------------------------------------------------------------------ #

        for region in last_batch_check(): # returns a ordered list of list of latests batch per region
            with open(f'{os.path.dirname(os.path.abspath(__file__))}\\Queries\\entities_query.sql', 'r') as file:
                entities_query = file.read()

            country = region[0]
            current_region_batch = int(re.search(r'\d+$',region[1]).group())
            new_region_batch = current_region_batch + 1


            # Formating main query
            entities_query = entities_query.format(new_region_batch, new_region_batch, new_region_batch, country, cases_id)


            # Now the execute command of the query
            c.execute(entities_query)

            entities_query_results = c.fetchall()
            entities_query_results.sort()


            columns_names = ['country','entity.id','case.id','case.investigation_status','entity.account_manager',
                            'entity.account_managers','entity.account_manager_phone','entity.account_manager_email',
                            'entity.tags','entity.years_machines_approved_for_engagement',
                            'case.years_machines_approved_for_engagement','case.tags','revenue_opportunity.recipient_type',
                            'revenue_opportunity.status','revenue_opportunity.invoice_item.0.product',
                            'revenue_opportunity.invoice_item.0.term','revenue_opportunity.invoice_item.0.quantity',
                            'revenue_opportunity.ro_account_manager','revenue_opportunity.effective_date']

            df_entities_query_results = pd.DataFrame(entities_query_results, columns=columns_names)

            df_entities_query_results = df_entities_query_results.drop(['country'], axis=1)



            # Merge the dataframes on 'case.id' to bring in the columns from df_cases_input
            df_merged = df_entities_query_results.merge(df_cases_input[['case.id', 'machines_approved_for_engagement', 'approved_machines_quantity',
                                                                        'all_time_machines']], on='case.id', how='left')

            # Populate the specific columns in df_entities_query_results using the merged columns
            df_merged['entity.years_machines_approved_for_engagement'] = df_merged['machines_approved_for_engagement']
            df_merged['case.years_machines_approved_for_engagement'] = df_merged['machines_approved_for_engagement']
            df_merged['revenue_opportunity.invoice_item.0.quantity'] = df_merged['approved_machines_quantity']

            # Drop the helper columns brought in by the merge
            df_merged = df_merged.drop(['machines_approved_for_engagement', 'approved_machines_quantity'], axis=1)

            df_merged = df_merged.rename(columns={'all_time_machines':'entity.total_machines'})

            df_merged['entity.actionable_machines'] = df_merged['revenue_opportunity.invoice_item.0.quantity']
            df_merged['entity.case_id'] = df_merged['case.id']


            #
            # ------------------------------------------------------------------------------------------------------------------ #
            excel_output_filepath = f'{os.path.dirname(os.path.abspath(__file__))}\\Chaos - {country} - Batch {new_region_batch} - Entities, Cases and RO.xlsx'
            df_merged.to_excel(excel_output_filepath, index=False)

            print()
            print("-----------------------------------------------------")
            print(f'Output file succesfully created for {country}')
            print("-----------------------------------------------------")
        break

    elif first_question == 'N':
        while True:

            region_map = {1:'Japan', 2:'Peru', 3:'Philippines', 4:'UAE'}


            second_question = input('Which region did not launched? (1: Japan, 2: Peru, 3: Philippines, 4: UAE): ')
            # additional steps must be inplemented for handling wrong user inputs

            selected_regions = [int(num.strip()) for num in second_question.split(',') if num.strip().isdigit()]

            not_updated_regions_names = [region_map[num] for num in selected_regions if num in region_map]

            # Get the full entry (region name + batch number) from last_batch_check() for regions not updated
            not_updated_regions = [region for region in last_batch_check() if region[0] in not_updated_regions_names]

            # Filter out the regions to update (i.e., remove the regions in not_updated_regions)
            regions_to_update = [i for i in last_batch_check() if i not in not_updated_regions]

            if all(num in [1, 2, 3, 4] for num in selected_regions):

                # -> for regions thar will update:
                for region in regions_to_update:
                    with open(f'{os.path.dirname(os.path.abspath(__file__))}\\Queries\\entities_query.sql', 'r') as file:
                        entities_query = file.read()

                    country = region[0]
                    current_region_batch = int(re.search(r'\d+$',region[1]).group())
                    new_region_batch = current_region_batch + 1


                    # Formating main query
                    entities_query = entities_query.format(new_region_batch, new_region_batch, new_region_batch, country, cases_id)


                    # Now the execute command of the query
                    c.execute(entities_query)

                    entities_query_results = c.fetchall()
                    entities_query_results.sort()


                    columns_names = ['country','entity.id','case.id','case.investigation_status','entity.account_manager',
                                    'entity.account_managers','entity.account_manager_phone','entity.account_manager_email',
                                    'entity.tags','entity.years_machines_approved_for_engagement',
                                    'case.years_machines_approved_for_engagement','case.tags','revenue_opportunity.recipient_type',
                                    'revenue_opportunity.status','revenue_opportunity.invoice_item.0.product',
                                    'revenue_opportunity.invoice_item.0.term','revenue_opportunity.invoice_item.0.quantity',
                                    'revenue_opportunity.ro_account_manager','revenue_opportunity.effective_date']

                    df_entities_query_results = pd.DataFrame(entities_query_results, columns=columns_names)

                    df_entities_query_results = df_entities_query_results.drop(['country'], axis=1)



                    # Merge the dataframes on 'case.id' to bring in the columns from df_cases_input
                    df_merged = df_entities_query_results.merge(df_cases_input[['case.id', 'machines_approved_for_engagement', 'approved_machines_quantity',
                                                                                'all_time_machines']],
                                                                on='case.id', how='left')

                    # Populate the specific columns in df_entities_query_results using the merged columns
                    df_merged['entity.years_machines_approved_for_engagement'] = df_merged['machines_approved_for_engagement']
                    df_merged['case.years_machines_approved_for_engagement'] = df_merged['machines_approved_for_engagement']
                    df_merged['revenue_opportunity.invoice_item.0.quantity'] = df_merged['approved_machines_quantity']

                    # Drop the helper columns brought in by the merge
                    df_merged = df_merged.drop(['machines_approved_for_engagement', 'approved_machines_quantity'], axis=1)

                    df_merged = df_merged.rename(columns={'all_time_machines':'entity.total_machines'})

                    df_merged['entity.actionable_machines'] = df_merged['revenue_opportunity.invoice_item.0.quantity']
                    df_merged['entity.case_id'] = df_merged['case.id']


                    #
                    # ------------------------------------------------------------------------------------------------------------------ #
                    excel_output_filepath = f'{os.path.dirname(os.path.abspath(__file__))}\\Chaos - {country} - Batch {new_region_batch} - Entities, Cases and RO.xlsx'
                    df_merged.to_excel(excel_output_filepath, index=False)

                    print()
                    print("-----------------------------------------------------")
                    print(f'Output file succesfully created for {country}')
                    print("-----------------------------------------------------")

                # ------------------------------------------------------------------------------------------------------------------ #

                # -> for regions thar will NOT update:
                for region in not_updated_regions: # returns a ordered list of list of latests batch per region
                    with open(f'{os.path.dirname(os.path.abspath(__file__))}\\Queries\\entities_query.sql', 'r') as file:
                        entities_query = file.read()

                    country = region[0]
                    current_region_batch = int(re.search(r'\d+$',region[1]).group())
                    #new_region_batch = current_region_batch + 1


                    # Formating main query
                    entities_query = entities_query.format(current_region_batch, current_region_batch, current_region_batch, country, cases_id)


                    # Now the execute command of the query
                    c.execute(entities_query)

                    entities_query_results = c.fetchall()
                    entities_query_results.sort()


                    columns_names = ['country','entity.id','case.id','case.investigation_status','entity.account_manager',
                                    'entity.account_managers','entity.account_manager_phone','entity.account_manager_email',
                                    'entity.tags','entity.years_machines_approved_for_engagement',
                                    'case.years_machines_approved_for_engagement','case.tags','revenue_opportunity.recipient_type',
                                    'revenue_opportunity.status','revenue_opportunity.invoice_item.0.product',
                                    'revenue_opportunity.invoice_item.0.term','revenue_opportunity.invoice_item.0.quantity',
                                    'revenue_opportunity.ro_account_manager','revenue_opportunity.effective_date']

                    df_entities_query_results = pd.DataFrame(entities_query_results, columns=columns_names)

                    df_entities_query_results = df_entities_query_results.drop(['country'], axis=1)



                    # Merge the dataframes on 'case.id' to bring in the columns from df_cases_input
                    df_merged = df_entities_query_results.merge(df_cases_input[['case.id', 'machines_approved_for_engagement', 'approved_machines_quantity',
                                                                                'all_time_machines']],
                                                                on='case.id', how='left')

                    # Populate the specific columns in df_entities_query_results using the merged columns
                    df_merged['entity.years_machines_approved_for_engagement'] = df_merged['machines_approved_for_engagement']
                    df_merged['case.years_machines_approved_for_engagement'] = df_merged['machines_approved_for_engagement']
                    df_merged['revenue_opportunity.invoice_item.0.quantity'] = df_merged['approved_machines_quantity']

                    # Drop the helper columns brought in by the merge
                    df_merged = df_merged.drop(['machines_approved_for_engagement', 'approved_machines_quantity'], axis=1)

                    df_merged = df_merged.rename(columns={'all_time_machines':'entity.total_machines'})

                    df_merged['entity.actionable_machines'] = df_merged['revenue_opportunity.invoice_item.0.quantity']
                    df_merged['entity.case_id'] = df_merged['case.id']


                    #
                    # ------------------------------------------------------------------------------------------------------------------ #
                    excel_output_filepath = f'{os.path.dirname(os.path.abspath(__file__))}\\Chaos - {country} - Batch {current_region_batch} - Entities, Cases and RO.xlsx'
                    df_merged.to_excel(excel_output_filepath, index=False)

                    print()
                    print("-----------------------------------------------------")
                    print(f'Output file succesfully created for {country}')
                    print("-----------------------------------------------------")


                break
            else:
                print('Not a valid input. Please enter valid numbers separated by commas (Example: 1,2).')
                #

        break

    else:
        print('Not a valid input. Please enter Y or N.')
