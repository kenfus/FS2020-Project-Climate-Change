# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 17:58:54 2020

@author: Lukas
"""

import json
import re
import requests
from os import path

import pandas as pd
from data_wrangling import _transform_swiss_data


def collect_global_temp():
    """
    Collects the global temperature of each country from 'http://berkeleyearth.lbl.gov/country-list/'
    
    Returns: - dict where the key is each country and the value is the according stored pandas dataframe
    """
    countries = json.load(open('./countries.json'))
    dfs = {}
    for country in countries.keys():
        df = _collect_region_temp(countries[country])
        if isinstance(df, pd.DataFrame):
            dfs[country] = df
    return dfs


def _collect_region_temp(country):
    """
    Collects the global temperature of the country `country` from 'http://berkeleyearth.lbl.gov/country-list/'
    
    It contains an extracted regional summary of land-surface temperature results produced
    by the Berkeley Earth averaging method for the region until 2013
    
    Input: the country value from ./countries.json
    
    Returns: - pandas dataframe, if the download was successful
             - None otherwise
    """
    url1, url2 = 'http://berkeleyearth.lbl.gov/auto/Regional/TAVG/Text/', '-TAVG-Trend.txt'
    df = None
    try:
        response = requests.get(url1 + country + url2)
        if response.status_code == 200:
            content = response.content.decode('latin1')  # utf-8 does not work due to some special characters
            content = re.split(
                r'% Year, Month,  Anomaly, Unc.,   Anomaly, Unc.,   Anomaly, Unc.,   Anomaly, Unc.,   Anomaly, Unc.\n \n  ',
                content)[1]
            rows = re.split(r'\n  ', content)
            data = [re.split(r'\s+', row) for row in rows]
            data[-1] = data[-1][:-1]  # last row contains an empty 13. value
            columns = ['year', 'month', 'monthly_anomaly', 'monthly_unc.',
                       'annual_anomaly', 'annual_unc.', 'five_year_anomaly', 'five_year_unc.',
                       'ten_year_anomaly', 'ten_year_unc.', 'twenty_year_anomaly', 'twenty_year_unc.']
            df = pd.DataFrame(data, columns=columns)
        else:
            print('status_code: ', response.status_code, '\noccured in: ', country)
    except Exception as e:
        print(e, '\noccured in : _collect_region_temp ', country)
    return df


def collect_global_co2():
    """
    Collects the global co_2 of each country from http://emissions2019.globalcarbonatlas.org
    
    It contains regional co_2 data in MtCO_2 from 1960 until 2018
    
    Returns: - pandas dataframe, if the file already exits or the download was successful
             - None otherwise
    """
    co2_path = './global_co2.csv'
    df_co2 = None
    if not path.exists(co2_path):
        url = 'http://emissions2019.globalcarbonatlas.org/exportDataset'
        data = {'app_selected': 'map', 'export_type': 'csv', 'year_start': '1960', 'year_end': '2018',
                'emissions_divider_selected': '{"0":{"emissionType_id":"3","divider_id":"1"}}',
                'countries_selected': '''[38,39,40,41,42,43,44,205,45,46,47,1,2,48,49,50,51,52,53,3,
               54,55,56,57,165,150,58,59,60,62,4,63,64,65,167,5,66,67,68,69,70,71,72,73,74,75,76,
               77,6,78,278,79,7,81,8,82,83,84,85,86,87,88,89,9,90,91,94,10,96,11,95,97,98,99,12,
               100,101,13,102,103,104,105,106,107,108,109,110,111,14,15,112,113,115,114,16,116,
               17,117,18,118,119,120,121,280,122,123,124,19,125,275,126,127,276,20,21,128,129,
               130,131,132,133,134,135,92,136,137,138,139,140,93,169,141,142,143,144,145,146,147,
               148,149,22,151,23,152,153,154,155,80,24,157,158,159,156,160,161,162,163,164,25,26,
               166,170,27,28,171,172,173,174,175,176,177,178,179,180,181,29,30,182,183,184,168,277,
               31,185,186,187,188,189,190,191,32,33,192,193,194,206,195,196,197,198,199,200,201,
               202,203,279,204,34,35,207,36,208,209,210,211,61,212,213,214,215,216]'''}
        try:
            response = requests.post(url, data=data)
            if response.status_code == 200:
                content = response.content.decode('latin1')
                content_cleaned = 'year' + content[35:-2776]  # [33:-2776] removes not data relevant characters
                f = open(co2_path, 'w')
                f.write(content_cleaned)
                f.close
            else:
                print('status_code in collect_global_co2: ', response.status_code)
        except Exception as e:
            print(e, '\noccured in collect_global_co2')
    if path.exists(co2_path):
        df_co2 = pd.read_csv(co2_path, delimiter=';')
    return df_co2


def get_swiss_data(sheets=None):
    """
    This function goes to "https://www.bfs.admin.ch/bfsstatic/dam/assets/12047383/master" and
    downloads the latest Version of swiss-climate data. This Data gets updated every year and contains the
    average yearly values of following climate-attributes: Temperature, Snowfall, Sunhours and Rain.
    In the function you can give either one or more of those attributes as list. If it's empty,
    it gives back the snowfall. This function also transforms the data into a melted form with
    Country, Region, Area and Year as an unique identifier.
    """
    ### Patameters
    first_year = 1931
    last_year = 2019
    url = 'https://www.bfs.admin.ch/bfsstatic/dam/assets/12047383/master'
    path_to_folder = ''
    data_name = 'klimadaten_swiss_open_data.xlsx'
    order_of_columns = ['Year', 'Country', 'Region', 'Area']

    ### Get Data with help of cliget
    # Set Sesion and get Cookie
    session = requests.Session()
    session.get(url)
    cookies = session.cookies.get_dict()

    # Generated Headers by cliegt
    headers = {
        'Host': 'www.bfs.admin.ch',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:74.0) Gecko/20100101 Firefox/74.0',
    }

    # Check if path exist and load it from there
    path_to_data = path_to_folder + data_name

    if not path.exists(path_to_data):
        url_data = requests.get(url, headers=headers, cookies=cookies)
        output = open(path_to_folder + data_name, 'wb')
        output.write(url_data.content)
        output.close()

    xlsx = pd.ExcelFile(path_to_data)

    return _transform_swiss_data(xlsx, first_year, last_year, sheets, order_of_columns, path_to_folder)


if __name__ == '__main__':
    get_swiss_data()
