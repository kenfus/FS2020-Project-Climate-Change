import data_collection.collection as dc

#df_snow = dc.get_swiss_data()
df_swiss = dc.get_swiss_data(sheets_to_collect = ['Sonnenscheindauer', 'Jahresniederschlag', 'Jahrestemperatur', 'Neuschnee'])
df_swiss_sunshine = df_swiss['Sonnenscheindauer']
df_swiss_precipitation = df_swiss['Jahresniederschlag']
df_swiss_temp = df_swiss['Jahrestemperatur']
df_swiss_snow = df_swiss['Neuschnee']
#df_temp = dc.collect_global_temp()
#df_co2 = dc.collect_global_co2()

#df_snow.to_csv('snow.csv', index = False)
#df_temp.to_csv('temp.csv')
#df_co2.to_csv('co2.csv')
df_swiss_sunshine.to_csv('swiss_sunshine.csv')
df_swiss_precipitation.to_csv('swiss_precipitation.csv')
df_swiss_temp.to_csv('swiss_temp.csv')
df_swiss_snow.to_csv('swiss_snow.csv')
