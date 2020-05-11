import data_collection.collection as dc

#df_snow = dc.get_swiss_data()
#df_temp = dc.collect_global_temp()
df_co2 = dc.collect_global_co2()

#df_snow.to_csv('snow.csv', index = False)
#df_temp.to_csv('temp.csv')
df_co2.to_csv('co2.csv')
