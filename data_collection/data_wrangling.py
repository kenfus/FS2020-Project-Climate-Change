from os import path

import numpy as np
import pandas as pd


def _transform_swiss_data(xlsx, first_year, last_year, sheets, order_of_columns, path_to_folder):
    if sheets is None:
        sheets = ['Neuschnee']

    ### Transform data:

    list_dfs = []

    for sheet in sheets:
        path_to_data = path_to_folder + sheet + '_' + str(first_year) + '-' + str(last_year) + '.csv'

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
            data_sliced.set_index('Year', inplace=True, drop=False)

            # Replace "..." by np.nan
            data_sliced = data_sliced.replace('...', np.nan)

            # Add country and region
            data_sliced['Country'] = 'Switzerland'
            data_sliced['Region'] = 'Europe'

            # Melt by Year, Country, Region and add Area
            data_melted = data_sliced.melt(['Country', 'Region', 'Year'], var_name='Area', value_name=sheet)

            # Sort dataframe
            order_of_columns.append(sheet)
            data_melted = data_melted.reindex(columns=order_of_columns)

            # Save Data
            data_melted.to_csv(path_to_data, index=False)
        else:
            data_melted = pd.read_csv(path_to_data)

        list_dfs.append(data_melted)

    if len(sheets) == 1:
        return data_melted
    else:
        return dict(zip(sheets, list_dfs))
