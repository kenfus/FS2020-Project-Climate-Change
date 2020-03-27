# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 17:58:54 2020

@author: Lukas
"""

import json, requests, re
import pandas as pd

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
    
    It contains contains an extracted regional summary of land-surface temperature results produced
    by the Berkeley Earth averaging method for the region until 2013
    
    Input: the country value from ./countries.json
    
    Returns: - pandas dataframe, if the download was successful
             - None otherwise
    """
    url1, url2 = 'http://berkeleyearth.lbl.gov/auto/Regional/TAVG/Text/', '-TAVG-Trend.txt'
    df = None
    try:
        response = requests.get(url1+country+url2)
        if response.status_code == 200:
            content = response.content.decode('latin1') #utf-8 does not work due to some special characters
            content = re.split(r'% Year, Month,  Anomaly, Unc.,   Anomaly, Unc.,   Anomaly, Unc.,   Anomaly, Unc.,   Anomaly, Unc.\n \n  ', content)[1]
            rows = re.split(r'\n  ', content)
            data = [re.split(r'\s+', row) for row in rows]
            data[-1] = data[-1][:-1] #last row contains an empty 13. value
            columns = ['year','month','monthly_anomaly','monthly_unc.',
                       'annual_anomaly','annual_unc.','five_year_anomaly','five_year_unc.',
                       'ten_year_anomaly','ten_year_unc.','twenty_year_anomaly','twenty_year_unc.']
            df = pd.DataFrame(data, columns = columns)
        else:
            print('status_code: ', response.status_code, '\noccured in: ', country)
    except Exception as e:
        print(e, '\noccured in: ', country)
    return df

def collect_global_co2():
    pass

if __name__ == '__main__':
    pass
    