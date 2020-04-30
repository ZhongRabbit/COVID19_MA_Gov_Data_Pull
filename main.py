# This file can be used in conjunction w/ a Docker container on Airflow to schedule daily data pulling from 
# https://www.mass.gov/info-details/covid-19-cases-quarantine-and-monitoring. Most data updates occur daily at 4pm.
# The known COVID19 cases by city/town are updated every Wednesday @ 4pm.
# This code will download, process & save the data locally as a failsafe.

gov_url = 'https://www.mass.gov/info-details/covid-19-cases-quarantine-and-monitoring'

# Will attempt to use mount_path by default (if deployed on a Docker container for bind mount). Otherwise, save data locally.
mount_path = '/opt/covid19_public_data_map/'

# Will attempt to use git path by default (if bind mounting to a local git repository to be later git pushed to a master branch for automation).
git_path = '/opt/data/seed_covid19__by_city_ma.csv'
age_git_path = '/opt/data/seed_covid19__by_age_ma.csv'

import io
import requests
import urllib
import certifi
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
from docx2csv import extract_tables
import warnings
from pdfminer.converter import TextConverter
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfpage import PDFPage

# Some helper functions
def decode_table(tables):

    table = tables[0]
    res = []
    for row in table:
        processed_row = [x.decode('utf-8').replace(' ', '') for x in row]
        res.append(processed_row)

    return res


def extract_text_from_pdf(pdf_path):

    resource_manager = PDFResourceManager()
    fake_file_handle = io.StringIO()
    converter = TextConverter(resource_manager, fake_file_handle)
    page_interpreter = PDFPageInterpreter(resource_manager, converter)
    
    with open(pdf_path, 'rb') as fh:
        for page in PDFPage.get_pages(fh, 
                                      caching=True,
                                      check_extractable=True):
            page_interpreter.process_page(page)
            
        text = fake_file_handle.getvalue()
    
    # close open handles
    converter.close()
    fake_file_handle.close()
    
    if text:
        return text


def extract_nums(string):

    res_list = []
    num_holder = ''
    for index, num in enumerate(string):
        if ',' in num_holder[-3:]:
            num_holder += num
            continue
        elif len(num_holder) >= 3 and num != ',':
            res_list.append(num_holder.replace(',',''))
            num_holder=num
        elif num == ',':
            num_holder += num
        else:
            num_holder+=num
    res_list.append(num_holder.replace(',',''))
    
    return res_list


def construct_age_df_from_text(text):

    age_section_str = re.search(r'Confirmed\s?Cases\s?by\s?Age.*Average\s?age\s?of', text, re.IGNORECASE).group()

    ages_raw = re.search(r'0-[^a-z]*\+', age_section_str).group().replace(' ', '')
    ages = re.findall('(\d\d?-\d{2}|\d+\+)', ages_raw)

    age_cases_str = re.search(r'\+[^(A-Z)]*', age_section_str, re.IGNORECASE).group()[1:].replace(' ','')

    age_cases = extract_nums(age_cases_str)
    if len(age_cases) != len(ages):
        warnings.warn(f'ages & age_cases do not match!\nages: {ages}\age_cases: {age_cases}')


    age_rates_raw = re.search(r'rate per 100,000.*[^a-z]', age_section_str, re.IGNORECASE).group()
    age_rates_raw = re.search(r'\+[^a-z]+', age_rates_raw, re.IGNORECASE).group()[1:].replace(' ', '')
    age_rates = extract_nums(age_rates_raw)

    if len(age_rates) != len(ages):
        warnings.warn(f'ages & age_rates do not match!\nages: {ages}\nage_rates: {age_rates}')

    age_df = pd.DataFrame({'Age_Group': ages,
                           'Cases': age_cases,
                           'Per_1M_pp': age_rates})

    age_df['Cases'] = age_df['Cases'].astype(int).astype('Int64')
    age_df['Per_1M_pp'] = age_df['Per_1M_pp'].astype(int).astype('Int64')
    age_df['Per_1M_pp'] = age_df['Per_1M_pp'] * 10
    
    return age_df


def main():
    opener=urllib.request.build_opener()
    opener.addheaders=[('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36')]
    urllib.request.install_opener(opener)

    res = requests.get(gov_url)
    soup = BeautifulSoup(res.text, 'html.parser')

    suffix_link = soup.find('a', text=re.compile(r'Doc', re.IGNORECASE))['href']
    download_link = f'https://www.mass.gov{suffix_link}'
    filename = re.search(r'([^-]*-[^-]*-[^-]*)-[^-]*/download', download_link).group(1)
    filename = filename.replace('-', '_')

    docx_mount_filepath = f'{mount_path}{filename}.docx'
    docx_filepath = f'downloaded/{filename}.docx'

    try:
        urllib.request.urlretrieve(download_link, docx_mount_filepath)
        print(f'Downloaded from {download_link} to {docx_mount_filepath}!')
    except FileNotFoundError:
        print('{docx_mount_filepath} does not exist! Save locally instead as {docx_filepath}!')
        urllib.request.urlretrieve(download_link, docx_filepath)
        
    # fetch all tables
    tables = extract_tables(docx_filepath)

    # pre-process the table embedded in the .docx file
    table = decode_table(tables)

    # Process the table from .docx
    df = pd.DataFrame(table[1:], columns=table[0])

    df.rename(columns=lambda x: re.sub(r'.*city.*', 'City_Town', x, flags=re.I),
              inplace=True)
    df.rename(columns=lambda x: re.sub(r'.*count.*', 'Count', x, flags=re.I),
              inplace=True)
    df.rename(columns=lambda x: re.sub(r'rate.*', 'Per_1M_pp', x, flags=re.I),
              inplace=True)

    df.loc[df['Count'] == '<5', 'Count'] = 1
    df['Count'] = df['Count'].astype(int)
    df.loc[df['Per_1M_pp'] == '*', 'Per_1M_pp']  = np.NaN
    df['Per_1M_pp'] = df['Per_1M_pp'].astype(float)
    # The original data employ the unit of "cases per 100k people", I found "cases per million" more useful. A bit of idiosyncrasy.
    df['Per_1M_pp'] = round(df['Per_1M_pp'] * 10, 0)
    # Int64 (in quotes, capital I) will ignore floating points when loading to BigQuery
    df['Per_1M_pp'] = df['Per_1M_pp'].astype("Int64")

    # Town name conventions vary. Standardize.
    correction_dict = {"EastBridgewater": "East Bridgewater",
        "EastBrookfield": "East Brookfield",
        "EastLongmeadow": "East Longmeadow",
        "FallRiver": "Fall River",
        "GreatBarrington": "Great Barrington",
        "MountWashington": "Mount Washington",
        "NewAshford": "New Ashford",
        "NewBedford": "New Bedford",
        "NewBraintree": "New Braintree",
        "NewMarlborough": "New Marlborough",
        "NewSalem": "New Salem",
        "NorthAdams": "North Adams",
        "NorthAndover": "North Andover",
        "NorthAttleborough": "North Attleborough",
        "NorthBrookfield": "North Brookfield",
        "NorthReading": "North Reading",
        "OakBluffs": "Oak Bluffs",
        "SouthHadley": "South Hadley",
        "WestBoylston": "West Boylston",
        "WestBridgewater": "West Bridgewater",
        "WestBrookfield": "West Brookfield",
        "WestNewbury": "West Newbury",
        "WestSpringfield": "West Springfield",
        "WestStockbridge": "West Stockbridge",
        "WestTisbury": "West Tisbury"}

    for k, v in correction_dict.items():
        df.loc[df['City_Town'] == k, 'City_Town'] = v

    # Save the table (known COVID19 cases per city/town) locally
    df.to_csv(f'processed/{filename}.csv',
              index=False)

    print(f'{filename}.csv saved locally!')


    csv_mount_path = f'{mount_path}{filename}.csv'
    
    # save it to the bind mount path
    try:
        df.to_csv(csv_mount_path,
                  index=False)
    except FileNotFoundError:
        print(f'{csv_mount_path} not found! Bypassing.')

    # save it to the git path
    try:
        df.to_csv(git_path,
                  index=False)
    except FileNotFoundError:
        print(f'{git_path} not found! Bypassing.')

    
    # ============================================================

    # Download the daily dashboard, in PDF format
    dashboard_download_link_suffix = soup.find('a', text=re.compile(r'COVID-19 Dashboard - .*', re.IGNORECASE))['href']
    dashboard_download_link = f'https://www.mass.gov{dashboard_download_link_suffix}'

    dashboard_filename = 'dashboard_' + re.search(r'[^-]*-[\d]{1,2}-[\d]{4}', dashboard_download_link_suffix).group()
    dashboard_filename = dashboard_filename.replace('-', '_')
    age_filename = dashboard_filename.replace('dashboard', 'age')
    dashboard_pdf_mount_filepath = f'{mount_path}{dashboard_filename}.pdf'
    dashboard_pdf_filepath = f'downloaded/{dashboard_filename}.pdf'

    try:
        urllib.request.urlretrieve(dashboard_download_link, dashboard_pdf_mount_filepath)
    except FileNotFoundError:
        print(f'{dashboard_pdf_mount_filepath} not found! Save the dashboard PDF locally instead!')
        urllib.request.urlretrieve(dashboard_download_link, dashboard_pdf_filepath)

    dashboard_text = extract_text_from_pdf(dashboard_pdf_filepath)

    age_df = construct_age_df_from_text(text=dashboard_text)

    # save it locally
    age_df.to_csv(f'processed/{age_filename}.csv',
                  index=False)

    
    age_csv_mount_path = f'{mount_path}{age_filename}.csv'

    # try save it to the bind mount path
    try:
        age_df.to_csv(age_csv_mount_path,
                      index=False)
    except FileNotFoundError:
        print(f'{age_csv_mount_path} not found! Bypassing.')

    # try save it to the git repository path
    try:
        age_df.to_csv(age_git_path,
                      index=False)
        print(f'{age_git_path} updated, to be git pushed to master!')

    except FileNotFoundError:
        print(f'{age_git_path} not found! Bypassing.')

if __name__ == '__main__':
    main()

