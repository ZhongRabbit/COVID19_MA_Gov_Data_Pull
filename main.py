# This file can be used in conjunction w/ a Docker container on Airflow to schedule daily data pulling from 
# https://www.mass.gov/info-details/covid-19-cases-quarantine-and-monitoring. Most data updates occur daily at 4pm.
# The known COVID19 cases by city/town are updated every Wednesday @ 4pm.
# This code will download, process & save the data locally as a failsafe. Additionally, it'll attempt to load the 
# result as BigQuery tables under "upload_bq_project.upload_bq_dataset".

import os
import io
import requests
import urllib
import certifi
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import datetime
from google.cloud import bigquery
from Levenshtein import distance
import math
from collections import defaultdict
from pyarrow.lib import ArrowTypeError

###########################
# Parameters
###########################

gov_url = 'https://www.mass.gov/info-details/covid-19-cases-quarantine-and-monitoring'

tolerable_levenshtein_ratio = 0.1 # Allow 1 word mismatch per 10 words when matching up target file/sheet names w/ actual file/sheet names, since mass.gov can have typo's or idiosyncrasies like that.
verbose = 2 # verbosity. Recommend set to min. 1. For debugging, set to 2.

col_chars_underscore_list = [' ', '/', '-']
col_chars_eliminate_list = ['(', ')', '*', '.', '=']

min_number_ratio_convert = 0.5 # ratio of minimum number format cells in a column for the entire column to be forcefully cast as numeric. This is to avoid data type issue during data upload to BQ.

download_folder = '/opt/covid19_public_data_map' # Change this depending on your Docker container setup
upload_bq_project = 'your_bigquery_project_id' # Change this to your BigQuery project ID.
upload_bq_dataset = 'covid19__staging_data'

total_upload_attempts = 5

job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE


###########################
# Helper Functions
###########################

def process_cols(df):
    for col in df.columns:
        renamed_col = col
        if re.search('^\d', col):
            renamed_col = f'_{renamed_col}'

        if '100000' in col and '1000000' not in col:
            renamed_col = col.replace('100000', '100k')

        for char in col_chars_underscore_list:
            renamed_col = renamed_col.replace(char, '_')

        for char in col_chars_eliminate_list:
            renamed_col = renamed_col.replace(char, '')

        if renamed_col != col:
            df.rename(columns={col: renamed_col}, inplace=True)

    return df


def levenshtein_dist_ok(str_1,
                        str_2,
                        tolerable_levenshtein_ratio = tolerable_levenshtein_ratio):

    processed_str_1 = str_1.lower().replace(' ', '')
    processed_str_2 = str_2.lower().replace(' ', '')

    return distance(processed_str_1, processed_str_2) <= math.ceil(len(str_1) * tolerable_levenshtein_ratio)


def count_float_ratio(col):
    counter = 0
    for cell in col:
        if isinstance(cell, float):
            counter += 1
    float_ratio = round(counter / len(col), 2)
    return float_ratio

def count_int_ratio(col):
    counter = 0
    for cell in col:
        if isinstance(cell, int):
            counter += 1
    int_ratio = round(counter / len(col), 2)
    return int_ratio


###########################
# Main.py
###########################

def main(tolerable_levenshtein_ratio = tolerable_levenshtein_ratio,
         verbose = verbose):
    global total_upload_attempts

    opener=urllib.request.build_opener()
    opener.addheaders=[('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36')]
    urllib.request.install_opener(opener)

    bq_client = bigquery.Client()

    res = requests.get(gov_url)
    soup = BeautifulSoup(res.text, 'html.parser')

    # try:
    daily_raw_files_suffix = soup.find('a', text=re.compile(f"^COVID-?19\s?Raw\s?Data.*", re.IGNORECASE))

    # today_B_str = datetime.date.today().strftime('%B')
    yesterday_B_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%B')

    today_d_str = int(datetime.date.today().strftime('%d'))
    yesterday_d_str = int((datetime.date.today() - datetime.timedelta(days=1)).strftime('%d'))

    tgt_B_str = re.search('([\w]*)\s?[\d]{1,2},?\s?\d{4}', daily_raw_files_suffix.text).group(1)
    tgt_d_str = re.search('[\w]*\s?([\d]{1,2}),?\s?\d{4}', daily_raw_files_suffix.text).group(1)

    if int(tgt_d_str) == today_d_str: # if days of the month match, ignore month match as it's almost impossible to have month mismatch in that case
        if verbose >= 1:
            print(f"O -- Found today's COVID-19 dashboard raw data: {daily_raw_files_suffix.text}")
        daily_raw_files_filename = f"daily_dashboard__{datetime.date.today().strftime('%Y_%m_%d')}"

    elif int(tgt_d_str) == yesterday_d_str: # Same, ignore month match
        if verbose >= 1:
            print(f"| -- Found yesterday's COVID-19 dashboard raw data: {daily_raw_files_suffix.text}")
        daily_raw_files_filename = f"daily_dashboard__{(datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y_%m_%d')}"

    else:
        if verbose >= 1:
            print(f"| -- Found older than yesterday's COVID-19 dashboard raw data: {daily_raw_files_suffix.text}")
        tgt_B_str_numeric_str = datetime.datetime.strftime(datetime.datetime.strptime(tgt_B_str, '%B'), '%m')
        tgt_d_str_padded = datetime.datetime.strftime(datetime.datetime.strptime(tgt_d_str, '%d'), '%d')
        daily_raw_files_filename = f"daily_dashboard__{(datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y')}_{tgt_B_str_numeric_str}_{tgt_d_str_padded}"

    # TODO: Compare to log

    daily_raw_file_download_link = f"https://www.mass.gov{daily_raw_files_suffix['href']}"

    daily_raw_file_name = f'{download_folder}/unzipped/{daily_raw_files_filename}.xlsx'

    urllib.request.urlretrieve(daily_raw_file_download_link, f'{download_folder}/unzipped/{daily_raw_files_filename}.xlsx')

    # From Census as of 2019-1: https://censusreporter.org/data/table/?table=B01001&geo_ids=04000US25,01000US&primary_geo_id=04000US25#valueType|estimate
    # TODO: This doesn't line up exactly w/ the calculated results of Mass.gov. Perhaps to refine the number later.

    daily_tab_to_bq_relation_dict = {
        'HospBedAvailable-Regional': 'Regional_Bed_Availability',
        'HospBed-Hospital COVID Census': 'Hospital_COVID_Census',
        'Hospitalization from Hospitals': 'Hospitalization_from_Hospitals',
        'LTC Facilities': 'LTC_Facilities',
        'TestingByDate (Test Date)': 'TestingByDate',
        'AgeLast2Weeks': 'AgeLast2Weeks',
        'CountyDeaths': 'CountyDeathsLast2Weeks',
        'County_Weekly': 'County_Weekly'
        }

    daily_tab_status_dict = defaultdict(int)

    # Build a dictionary that maps the actual file names to template filenames, w/ the two following rules:
    # 1. Ignore spaces as they can be replaced /w underscore and who knows what.
    # 2. Make levenshtein distance comparison case-insensitive.

    daily_excel_file = pd.ExcelFile(daily_raw_file_name)
    
    for index, act_tab in enumerate(daily_excel_file.sheet_names):
        for tgt_tab, bq_relation in daily_tab_to_bq_relation_dict.items():
            if levenshtein_dist_ok(act_tab, tgt_tab):
                if act_tab != tgt_tab and verbose >= 1:
                    print(f'| -- A slight mismatch between actual tab name of {act_tab} & target tab name of {tgt_tab}.')

                try:
                    df = daily_excel_file.parse(index)

                    # process the dataframe
                    df = process_cols(df)

                    for col in df.columns:
                        if count_float_ratio(df[col]) >= 0.5:
                            df[col] = pd.to_numeric(df[col], errors = 'coerce')
                        if count_int_ratio(df[col]) >= 0.5:
                            df[col] = pd.to_numeric(df[col], errors = 'coerce')

                except Exception as e:
                    daily_tab_status_dict[tgt_tab] = 0
                    print(f'X -- Unable to process or load the tab {act_tab} as {bq_relation} due to {e}.')


                bq_destination = f'{upload_bq_project}.{upload_bq_dataset}.{bq_relation}'

                for attempt in range(1, total_upload_attempts+1):
                    try:
                        # Load table synchronously to allow more robust error detection
                        bq_client.load_table_from_dataframe(df,
                                                            destination = bq_destination,
                                                            job_config = job_config).result()

                        daily_tab_status_dict[tgt_tab] = 1
                    
                    except ArrowTypeError as e:
                        daily_tab_status_dict[tgt_tab] = 0
                        if re.search('Expected a bytes object, got a \'int\' object', str(e)):
                            col_name = re.search('failed for column (.*) with type', str(e)).group(1)
                            print(f'| -- Casting column {col_name} as type string before retry upload!')
                            df[col_name] = df[col_name].astype(str)
                    
                    else:
                        if attempt == total_upload_attempts:
                            print(f'X -- Unable to process or load the tab {act_tab} as {bq_relation} due to {e}.')
                        else:
                            if verbose >= 2:
                                print(f"O -- Uploaded act_tab: {act_tab} | df.shape: {df.shape} | bq_destination: {bq_destination} | attempt(s): {attempt}.")
                        break

    if verbose >= 1:
        if sum(daily_tab_status_dict.values()) == len(daily_tab_to_bq_relation_dict):
            print(f'O -- Successfully loaded all required daily files!')
        else:
            print(f'X -- Failed uploading some daily files:')
            for table, val in daily_tab_status_dict.items():
                if val == 0:
                    print(table)

    
    
    # Process weekly file
    weekly_raw_files_suffix = soup.find('a', text=re.compile(f"^Weekly.*Raw\s?Data.*", re.IGNORECASE))

    avail_weekly_date = int(re.search('[\w]*\s?([\d]{1,2}),?\s?\d{4}', weekly_raw_files_suffix.text).group(1))

    # TODO: compare to log.

    weekly_raw_files_download_link = f"https://www.mass.gov{weekly_raw_files_suffix['href']}"

    weekly_raw_files_filename = f"weekly_dashboard__{datetime.date.today().strftime('%Y_%m')}_{avail_weekly_date}"


    # Looks like this is an Excel file for now. But can change?
    urllib.request.urlretrieve(weekly_raw_files_download_link, f'{download_folder}/unzipped/{weekly_raw_files_filename}.xlsx')

    weekly_excel_file = pd.ExcelFile(f'{download_folder}/unzipped/{weekly_raw_files_filename}.xlsx')

    weekly_tab_to_bq_relation_dict = {
        'LTCF': 'LTCF',
        'ALR': 'ALR'
        }

    weekly_tab_status_dict = defaultdict(int)
    
    # TODO: convert 100000 to 100K, or 1M
    for index, act_tab in enumerate(weekly_excel_file.sheet_names):
        for tgt_tab, bq_relation in weekly_tab_to_bq_relation_dict.items():
            if levenshtein_dist_ok(act_tab, tgt_tab):
                match_found = True

                if tgt_tab != act_tab and verbose >= 1:
                    print(f"| -- A slight mismatch between the intended sheet name of  {sheet_name}  &  actual sheet name of {actual_sheet_name}")

                try:
                    df = weekly_excel_file.parse(act_tab)

                    # Process the dataframe
                    df = process_cols(df)

                    for col in df.columns:
                        if count_float_ratio(df[col]) >= 0.5:
                            df[col] = pd.to_numeric(df[col], errors = 'coerce')
                        if count_int_ratio(df[col]) >= 0.5:
                            df[col] = pd.to_numeric(df[col], errors = 'coerce')

                except Exception as e:
                    weekly_tab_status_dict[tgt_tab] = 0
                    print(f'X -- Unable to process the tab {act_tab} as {bq_relation} due to {e}.')
                    continue

                bq_destination = f'{upload_bq_project}.{upload_bq_dataset}.{bq_relation}'

                for attempt in range(1, total_upload_attempts+1):
                    try:
                        # Load table synchronously to allow more robust error detection
                        bq_client.load_table_from_dataframe(df,
                                                            destination = bq_destination,
                                                            job_config = job_config).result()

                        weekly_tab_status_dict[tgt_tab] = 1

                    except ArrowTypeError as e:
                        weekly_tab_status_dict[tgt_tab] = 0
                        if re.search('Expected a bytes object, got a \'int\' object', str(e)):
                            col_name = re.search('failed for column (.*) with type', str(e)).group(1)
                            print(f'| -- Casting column {col_name} as type string before retry upload!')
                            df[col_name] = df[col_name].astype(str)
                    
                    else:
                        if attempt == total_upload_attempts:
                            print(f'X -- Unable to process or load the tab {act_tab} as {bq_relation} due to {e}.')
                        else:
                            if verbose >= 2:
                                print(f"O -- Uploaded act_tab: {act_tab} | df.shape: {df.shape} | bq_destination: {bq_destination} | attempt(s): {attempt}.")
                        break

    if verbose >= 1:
        if sum(weekly_tab_status_dict.values()) == len(weekly_tab_to_bq_relation_dict):
            print(f'O -- Successfully loaded all required weekly files!')
        else:
            print(f'X -- Failed uploading some weekly files:')
            for table, val in weekly_tab_status_dict.items():
                if val == 0:
                    print(table)

    if verbose >= 1:
        print(f'O -- Successfully loaded all required weekly files!')

if __name__ == '__main__':
    main()