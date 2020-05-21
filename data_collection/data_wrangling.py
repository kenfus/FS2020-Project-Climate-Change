import inspect
import json
import numpy as np
import pandas as pd
from os import path


def transform_swiss_data(list_of_sheets, sheets_to_collect, order_of_columns, first_year, last_year, ):
    ### Transform data:

    list_dfs = []

    for sheet, data in zip(sheets_to_collect, list_of_sheets):
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
        data_sliced['alpha_3'] = 'CHE'
        # Melt by Year, Country, Region and add Area
        data_melted = data_sliced.melt(['Country', 'Year', 'alpha_3'], var_name='Region', value_name=sheet)
        # Clean the names
        rename_dict = {'Region': {0: 'Basel-Binningen', 1: 'Bern-Zollikofen 1)', 2: 'Davos WSL 2)', 3: 'Genf-Cointrin', 4: 'Locarno-Monti',
                    5: 'Lugano', 6: 'Luzern', 7: 'Neuenburg', 8: 'Säntis', 9: 'Samedan', 10: 'Sitten', 11: 'St. Gallen',
                    12: 'Zürich-Fluntern'},
                    'Region_cleaned': {0: 'Basel-Binningen', 1: 'Bern-Zollikofen', 2: 'Davos', 3: 'Genf-Cointrin',
                    4: 'Locarno-Monti', 5: 'Lugano', 6: 'Luzern', 7: 'Neuenburg', 8: 'Säntis', 9: 'Samedan', 10: 'Sitten', 11: 'St. Gallen',
                    12: 'Zürich-Fluntern'},
                    'Latitude': {0: 47.53796, 1: 46.99832, 2: 46.802746, 3: 46.22783, 4: 46.170978999999996,
                    5: 46.003601, 6: 47.04554, 7: 48.84655, 8: 47.256459, 9: 46.438365000000005, 10: 46.2276, 11: 47.424479999999996,
                    12: 47.382096999999995},
                    'Longitude': {0: 7.571110000000001, 1: 7.45109, 2: 9.83597, 3: 6.10424, 4: 8.79112, 5:
                    8.953619999999999, 6: 8.308010000000001, 7: 8.588410000000001, 8: 9.317421000000001, 9: 9.870266, 10: 7.3599429999999995, 11: 9.376717, 12: 8.566803}}
        df = pd.DataFrame.from_dict(rename_dict)
        data_melted = data_melted.replace(dict(zip(df['Region'], df['Region_cleaned'])))
        # Add Latitude and Longitude
        long_lat_df = df[['Latitude', 'Longitude', 'Region_cleaned']]
        data_melted = data_melted.merge(long_lat_df, left_on='Region', right_on='Region_cleaned')
        # Sort dataframe
        colums_sheet = order_of_columns + [sheet]
        data_melted = data_melted.reindex(columns=colums_sheet)
        list_dfs.append(data_melted)

    if len(sheets_to_collect) == 1:
        return data_melted
    else:
        return dict(zip(sheets_to_collect, list_dfs))


def transform_global_co2(df_co2):
    """
    Transforms the global co2
    - renames undesired naming of countries
    - renames true country names to chars A-Z|a-z
    - slices selected countries from `df_co2` based on 'data_collection/countries.json'
    - adds -00 to 'year' values

    :param df_co2: pd.DataFrame global co2 data

    :returns: pd.DataFrame where columns are: 'year', 'country', 'co2'
    """
    dir_path = path.dirname(path.abspath(inspect.getfile(inspect.currentframe())))
    countries_json = json.load(open(dir_path + '/countries.json', 'r'))

    # rename of impropper names
    rename = {'Occupied Palestinian Territory': 'Palestina', 'Republic of South Sudan': 'Sudan',
              'Russian Federation': 'Russia'}
    df_co2 = df_co2.rename(columns=rename)

    # slice only selected countries
    countries = list(countries_json.keys())
    drop_countries = [country for country in df_co2.columns[1:] if country not in countries]
    df_co2 = df_co2.drop(columns = drop_countries)

    # rename for db storage
    db_names = [country['db_name'] for country in countries_json.values()]
    rename = dict(zip(countries, db_names))
    df_co2 = df_co2.rename(columns=rename)

    # add year-01-01 to column year
    df_co2 = df_co2.astype({'year': 'str'})
    df_co2.loc[:, 'year'] = df_co2.loc[:, 'year'] + '-01-01'
    df_co2 = df_co2.astype({'year': 'datetime64'})

    # bring co2 to db-compatible format
    df_co2 = df_co2.melt(id_vars=['year'], var_name='country', value_name='co2')

    return df_co2


def _transform_country_temp(df, country, columns):
    """
    Transforms the temperature dataframe of a specific country
    - date from 'year' and 'month' as yyyy-mm
    - adds 'country' as the second column with values `country`
    - slices selected columns `columns`

    :param df: pd.DataFrame to transform
    :param country: str name of the country
    :param columns: list of which columns to slice

    :returns: transformed pd.DataFrame with the selected columns
    """
    # creates yyyy-mm
    month_replace = {
        'month': {'1': '01', '2': '02', '3': '03', '4': '04', '5': '05', '6': '06', '7': '07', '8': '08', '9': '09', }}
    df = df.replace(month_replace)
    df.loc[:, 'year'] = df.loc[:, 'year'] + '-' + df.loc[:, 'month'] + '-01'
    df = df.rename(columns={'year': 'date'})

    # add country to df and reorder columns
    df['country'] = country
    cols = df.columns.tolist()
    cols = [cols[0]] + [cols[-1]] + cols[1:-1]
    df = df.loc[:, cols]

    return df.loc[:, columns]


def transform_global_temp(df_dict):
    """
    Transforms the global temperature of each country in df_dict

    :param df_dict: dictionary with key(country) and value(pd.DataFrame)

    :returns: pd.DataFrame with columns: 'date', 'country', 'monthly_anomaly', 'monthly_unc.'
    """
    dir_path = path.dirname(path.abspath(inspect.getfile(inspect.currentframe())))
    countries_json = json.load(open(dir_path + '/countries.json', 'r'))
    
    columns = ['date', 'country', 'monthly_anomaly']  # add(, 'monthly_unc.') if desired
    df_temp = pd.DataFrame(columns=columns)    
    for country, df in zip(df_dict.keys(), df_dict.values()):
        df = _transform_country_temp(df, country, columns)
        df_temp = df_temp.append(df)

    df_temp = df_temp.astype({'date': 'datetime64'})
    
    # rename for db storage
    countries = list(countries_json.keys())
    db_names = [country['db_name'] for country in countries_json.values()]
    df_temp['country'] = df_temp['country'].replace(countries, db_names)
    return df_temp
