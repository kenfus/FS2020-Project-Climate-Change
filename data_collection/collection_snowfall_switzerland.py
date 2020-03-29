from os import path
import numpy as np
import pandas as pd
import requests

### Patameters
sheets_to_transform = ['Neuschnee', 'Sonnenscheindauer']
first_year = 1931
last_year = 2019
url = 'https://www.bfs.admin.ch/bfsstatic/dam/assets/12047383/master'
path_to_folder = ''
data_name = 'klimadaten_swiss_open_data.xlsx'


###

def get_and_transform_data(sheets=['Neuschnee']):
    # Get Data with help of cliget
    cookies = {
        'TS0142722c': '019832244bd7e0ffcc1d3f2a2afa4b92c591dd2a6503d0df6fdda9a79b4d6421e18620904dc83e55fafe28ac7864a2a112e329599d',
    }

    headers = {
        'Host': 'www.bfs.admin.ch',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:74.0) Gecko/20100101 Firefox/74.0',
    }

    path_to_data = path_to_folder + data_name
    # Check if path exist:

    if not path.exists(path_to_data):
        url_data = requests.get(url, headers=headers, cookies=cookies)

        output = open(path_to_folder + data_name, 'wb')
        output.write(url_data.content)
        output.close()

    xlsx = pd.ExcelFile(path_to_data)

    ### Transform data:

    list_dfs = []

    for sheet in sheets:
        path_to_data = path_to_folder + sheet + '_' + str(first_year) + '-' + str(last_year) + '.xlsx'

        # Check if file doesn't already exist:
        if not path.exists(path_to_data):
            # Read data from web-grabber
            data = pd.read_excel(xlsx, sheet)

            # Get city names (first row with Säntis in it) and set this as colum-names
            r, c = np.where(data == 'Säntis')
            r = np.min(r)
            data.columns = data.iloc[r]

            # Slice dataframe between to years
            first_row = np.where(data.iloc[:, 0] == first_year)[0][0]
            last_row = np.where(data.iloc[:, 0] == last_year)[0][0]
            data_sliced = data[(data.index >= first_row) & (data.index <= last_row)]

            # Set column with years as index
            data_sliced.columns = data_sliced.columns.fillna('Year')
            data_sliced.set_index('Year', drop=True, inplace=True)

            # Replace "..." by np.nan
            data_sliced = data_sliced.replace('...', np.nan)

            # Save Data
            # data_sliced.to_excel(path_to_data + sheet + '_' + str(first_year) + '-' + str(last_year) + '.xlsx'

        else:
            data_sliced = pd.read_excel(path_to_data)

        list_dfs.append(data_sliced)

    if len(sheets) == 1:
        return data_sliced
    else:
        return dict(zip(sheets, list_dfs))